"""Reporting layer: small tables for the dashboard, ``gold/reporting/*.parquet`` (ADR 0034).

Every table covers the whole history but a run only has the window months locally, so each
table is keyed by month and a run replaces the rows of the months it built and keeps the rest
(the previous file comes from the repo). All tables together stay in the low MB range.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from cvm_lake import gold, silver
from cvm_lake.anomalies.detector import anomalies_path
from cvm_lake.anomalies.restatements import restatements_path
from cvm_lake.db import copy_to_parquet, lit, one
from cvm_lake.periods import shift_month

RECENT_DAYS = 90  # anomalies_recent keeps this many days back from the newest report
COMPLETE_SHARE = 0.9  # a date is complete with 90% of the classes of the previous dates
# ... and when the classes that reported held 99% of the equity of the classes expected that day
# (ADR 0036). Normal days measure 99.4% or more, the March 2020 crash 99.9%; late dates still
# filling in measured 98.8%, 98.1% and 94.8% (2026-10-26 to 28).
COMPLETE_AUM_SHARE = 0.99
EXPECTED_DAYS = 14  # a class is expected on a date when it reported in the 14 days before it
MIN_RETURN_DAYS = 15  # a fund needs this many daily returns in a month for the monthly table

SCHEMAS: dict[str, pa.Schema] = {
    "aum_flows_daily": pa.schema(
        [
            ("dt", pa.date32()),
            ("peer_group", pa.string()),
            ("is_fic", pa.bool_()),
            ("n_classes", pa.int64()),
            ("aum", pa.float64()),
            ("subscriptions", pa.float64()),
            ("redemptions", pa.float64()),
            ("net_flow", pa.float64()),
            ("coverage", pa.float64()),  # equity weighted share of the expected classes
            ("complete", pa.bool_()),
        ]
    ),
    "returns_monthly": pa.schema(
        [
            ("ano_mes", pa.string()),
            ("peer_group", pa.string()),
            ("n_funds", pa.int64()),
            ("median_ret_brl", pa.float64()),
            ("p25_ret_brl", pa.float64()),
            ("p75_ret_brl", pa.float64()),
            ("median_ret_usd", pa.float64()),
        ]
    ),
    "anomalies_daily": pa.schema(
        [
            ("dt", pa.date32()),
            ("anomaly_type", pa.string()),
            ("severity", pa.string()),
            ("n", pa.int64()),
        ]
    ),
    "anomalies_recent": pa.schema(
        [
            ("dt", pa.date32()),
            ("fund_id", pa.string()),
            ("class_id", pa.string()),
            ("name", pa.string()),
            ("peer_group", pa.string()),
            ("anomaly_type", pa.string()),
            ("severity", pa.string()),
            ("score", pa.float64()),
            ("first_seen_run", pa.string()),
        ]
    ),
    "restatements_monthly": pa.schema(
        [
            ("ano_mes", pa.string()),
            ("detected_run", pa.string()),
            ("changed_reports", pa.int64()),
            ("flagged_before", pa.int64()),
        ]
    ),
    "ptax_daily": pa.schema([("dt", pa.date32()), ("sell", pa.float64())]),
}


def reporting_path(name: str) -> str:
    return f"gold/reporting/{name}.parquet"


REPORTING_PATHS = [reporting_path(n) for n in SCHEMAS]


def _files(root: Path, rels: list[str]) -> str | None:
    paths = [root / r for r in rels if (root / r).exists()]
    return "[" + ", ".join(lit(p) for p in paths) + "]" if paths else None


def _months_sql(months: list[str]) -> str:
    return "(" + ", ".join(f"'{m}'" for m in months) + ")"


def _merge(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    name: str,
    fresh: str | None,
    month_expr: str,
    months: list[str],
    order: str,
    post: str | None = None,
) -> int:
    """Rows of ``months`` from ``fresh``, the other months from the existing file."""
    schema = SCHEMAS[name]
    columns = ", ".join(schema.names)
    parts = []
    existing = root / reporting_path(name)
    if existing.exists():
        # A file written before a column was added reads that column as NULL.
        have = set(pq.read_schema(existing).names)
        old = ", ".join(
            f.name if f.name in have else f"NULL::{_sql_type(f.type)} AS {f.name}" for f in schema
        )
        parts.append(
            f"SELECT {old} FROM read_parquet({lit(existing)}) "
            f"WHERE {month_expr} NOT IN {_months_sql(months)}"
        )
    if fresh is not None:
        parts.append(f"SELECT {columns} FROM ({fresh})")
    if not parts:
        return 0
    query = f"SELECT * FROM ({' UNION ALL '.join(parts)})"
    if post:
        query = post.format(source=query)
    return copy_to_parquet(con, f"{query} ORDER BY {order}", root, reporting_path(name), schema)


def _sql_type(t: pa.DataType) -> str:
    return {pa.float64(): "DOUBLE", pa.bool_(): "BOOLEAN", pa.int64(): "BIGINT"}.get(t, "VARCHAR")


def _coverage_sql(root: Path, months: list[str]) -> str:
    """Per date of ``months``: equity of the expected classes that reported / all expected.

    A class is expected on date d when it reported in the EXPECTED_DAYS before d; its weight is
    the equity of that last report. The previous month is read so the first days have history.
    """
    with_previous = [shift_month(min(months), -1), *months]
    files = _files(root, [silver.fund_daily_path(m) for m in with_previous])
    if files is None:
        return "SELECT NULL::DATE AS dt, NULL::DOUBLE AS coverage WHERE false"
    return f"""
        WITH f AS (
            SELECT cnpj, coalesce(subclass_id, '') AS sc, dt, equity::DOUBLE AS eq
            FROM read_parquet({files}, hive_partitioning = false) WHERE equity > 0
        ),
        l AS (
            SELECT *, lag(dt) OVER w AS prev_dt, lag(eq) OVER w AS prev_eq,
                   lead(dt) OVER w AS next_dt
            FROM f WINDOW w AS (PARTITION BY cnpj, sc ORDER BY dt)
        ),
        cal AS (
            SELECT dt, lead(dt) OVER (ORDER BY dt) AS next_cal FROM (SELECT DISTINCT dt FROM f)
        ),
        dates AS (SELECT dt FROM cal WHERE strftime(dt, '%Y%m') IN {_months_sql(months)}),
        present AS (
            SELECT dt, sum(prev_eq) AS eq FROM l
            WHERE prev_dt >= dt - INTERVAL {EXPECTED_DAYS} DAY GROUP BY dt
        ),
        -- only reports followed by a gap can make a class missing, so join just those
        gaps AS (
            SELECT l.dt, l.eq, l.next_dt FROM l JOIN cal USING (dt)
            WHERE l.next_dt IS NULL OR l.next_dt > cal.next_cal
        ),
        missing AS (
            SELECT d.dt, sum(g.eq) AS eq FROM gaps g JOIN dates d
              ON d.dt > g.dt AND d.dt <= g.dt + INTERVAL {EXPECTED_DAYS} DAY
             AND (g.next_dt IS NULL OR d.dt < g.next_dt)
            GROUP BY d.dt
        )
        SELECT d.dt, p.eq / nullif(p.eq + coalesce(m.eq, 0), 0) AS coverage
        FROM dates d LEFT JOIN present p USING (dt) LEFT JOIN missing m USING (dt)
    """


@dataclass(frozen=True)
class ReportingStats:
    rows: dict[str, int]


def build_reporting(
    con: duckdb.DuckDBPyConnection, root: Path, months: list[str]
) -> ReportingStats | None:
    """Update ``gold/reporting`` for ``months`` (the months whose gold was just built)."""
    if not months:
        return None
    rows: dict[str, int] = {}
    ym = "strftime(dt, '%Y%m')"

    flows = _files(root, [gold.daily_flows_path(m) for m in months])
    rows["aum_flows_daily"] = _merge(
        con,
        root,
        "aum_flows_daily",
        flows
        and f"""SELECT dt, peer_group, is_fic, n_classes, aum::DOUBLE AS aum,
                       subscriptions::DOUBLE AS subscriptions,
                       redemptions::DOUBLE AS redemptions, net_flow::DOUBLE AS net_flow,
                       c.coverage, NULL::BOOLEAN AS complete
                FROM read_parquet({flows})
                LEFT JOIN ({_coverage_sql(root, months)}) c USING (dt)""",
        ym,
        months,
        "dt, peer_group, is_fic NULLS LAST",
        # Completeness is a property of the whole series (ADR 0021, 0036): a date is complete
        # when it has 90% of the median class count of the 10 dates before it and, where the
        # coverage is known, 99% of the expected equity.
        post=f"""
            WITH s AS ({{source}}),
                 d AS (SELECT dt, sum(n_classes) AS n, any_value(coverage) AS coverage
                       FROM s GROUP BY dt),
                 c AS (SELECT dt, n >= {COMPLETE_SHARE} * coalesce(median(n) OVER (
                           ORDER BY dt ROWS BETWEEN 10 PRECEDING AND 1 PRECEDING), 0)
                           AND coalesce(coverage >= {COMPLETE_AUM_SHARE}, true)
                           AS complete FROM d)
            SELECT s.* EXCLUDE (complete), c.complete FROM s JOIN c USING (dt)
        """,
    )

    returns = _files(root, [gold.returns_path(m) for m in months])
    rows["returns_monthly"] = _merge(
        con,
        root,
        "returns_monthly",
        returns
        and f"""
            WITH f AS (
                SELECT strftime(dt, '%Y%m') AS ano_mes, peer_group, cnpj, subclass_id,
                       exp(sum(ln(1 + ret_brl))) - 1 AS ret_brl,
                       exp(sum(ln(1 + ret_usd))) - 1 AS ret_usd,
                       count(ret_brl) AS days
                FROM read_parquet({returns})
                WHERE ret_brl > -1 AND (ret_usd IS NULL OR ret_usd > -1)
                GROUP BY ALL
            )
            SELECT ano_mes, peer_group, count(*) AS n_funds,
                   median(ret_brl) AS median_ret_brl,
                   quantile_cont(ret_brl, 0.25) AS p25_ret_brl,
                   quantile_cont(ret_brl, 0.75) AS p75_ret_brl,
                   median(ret_usd) AS median_ret_usd
            FROM f WHERE days >= {MIN_RETURN_DAYS}
            GROUP BY ALL""",
        "ano_mes",
        months,
        "ano_mes, peer_group",
    )

    anomalies = _files(root, [anomalies_path(m) for m in months])
    rows["anomalies_daily"] = _merge(
        con,
        root,
        "anomalies_daily",
        anomalies
        and f"""SELECT dt, anomaly_type, severity, count(*) AS n
                FROM read_parquet({anomalies}) GROUP BY ALL""",
        ym,
        months,
        "dt, anomaly_type, severity",
    )

    registry = root / silver.FUND_REGISTRY_PATH
    names = (
        f"SELECT cnpj, any_value(name) AS name FROM {lit(registry)} "
        "WHERE source = 'registro_classe' GROUP BY cnpj"
        if registry.exists()
        else "SELECT NULL::VARCHAR cnpj, NULL::VARCHAR name WHERE false"
    )
    rows["anomalies_recent"] = _merge(
        con,
        root,
        "anomalies_recent",
        anomalies
        and f"""SELECT a.dt, a.fund_id, a.class_id, n.name, a.peer_group, a.anomaly_type,
                       a.severity, a.score, a.first_seen_run
                FROM read_parquet({anomalies}) a
                LEFT JOIN ({names}) n ON n.cnpj = a.fund_id
                WHERE a.severity IN ('high', 'medium')""",
        ym,
        months,
        "dt DESC, severity, score DESC, fund_id, class_id",
        post=f"""
            WITH s AS ({{source}})
            SELECT * FROM s
            WHERE dt >= (SELECT max(dt) FROM s) - INTERVAL {RECENT_DAYS} DAY
        """,
    )

    restated = _files(root, [restatements_path(m) for m in months])
    rows["restatements_monthly"] = _merge(
        con,
        root,
        "restatements_monthly",
        restated
        and f"""SELECT strftime(dt, '%Y%m') AS ano_mes, detected_run,
                       count(DISTINCT (fund_id, class_id, dt)) AS changed_reports,
                       count(DISTINCT (fund_id, class_id, dt)) FILTER (WHERE flagged_before <> '')
                           AS flagged_before
                FROM read_parquet({restated}) GROUP BY ALL""",
        "ano_mes",
        months,
        "ano_mes, detected_run",
    )

    ptax = _files(root, [silver.ptax_daily_path(m) for m in months])
    rows["ptax_daily"] = _merge(
        con,
        root,
        "ptax_daily",
        ptax and f"SELECT dt, sell::DOUBLE AS sell FROM read_parquet({ptax})",
        ym,
        months,
        "dt",
    )
    return ReportingStats(rows=rows)


def latest_complete_date(con: duckdb.DuckDBPyConnection, root: Path) -> str | None:
    path = root / reporting_path("aum_flows_daily")
    if not path.exists():
        return None
    value = one(con, f"SELECT max(dt) FROM {lit(path)} WHERE complete")[0]
    return str(value) if value else None
