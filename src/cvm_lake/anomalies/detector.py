"""Builds ``gold/anomalies/ano_mes=YYYYMM`` for one month (ADR 0028).

Loads silver of the month and the ``history_months`` before it with the peer group, computes
``features.sql``, applies the rules and layer flags of ``rules.py`` and writes one row per
(fund, class, date, anomaly type). ``first_seen_run`` is carried over from the previous version
of the partition, so a finding is "new" only once.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

import duckdb
import pyarrow as pa

from cvm_lake import gold
from cvm_lake.anomalies import rules
from cvm_lake.anomalies.settings import AnomalyConfig
from cvm_lake.db import copy_to_parquet, lit, one
from cvm_lake.periods import month_bounds, shift_month

FEATURES_SQL = (Path(__file__).with_name("features.sql")).read_text(encoding="utf-8")

ANOMALY_SCHEMA = pa.schema(
    [
        ("fund_id", pa.string()),  # CNPJ: the fund before Resolution 175, the class after it
        ("class_id", pa.string()),  # subclass id, '' when the class has no subclasses
        ("dt", pa.date32()),
        ("anomaly_type", pa.string()),
        ("score", pa.float64()),
        ("severity", pa.string()),
        ("peer_group", pa.string()),
        ("features", pa.string()),  # JSON
        ("model_version", pa.string()),
        ("first_seen_run", pa.string()),
    ]
)

FEATURE_COLUMNS = (
    "quota",
    "prev_quota",
    "equity",
    "prev_equity",
    "subscriptions",
    "redemptions",
    "shareholders",
    "prev_shareholders",
    "log_ret",
    "next_log_ret",
    "excess_ret",
    "z_return",
    "z_excess",
    "z_residual",
    "residual",
    "quota_ratio",
    "equity_ratio",
    "shareholder_ratio",
    "peer_median",
    "n_peers",
    "market_z",
    "reverted",
    "gap_market_days",
    "unchanged_reports",
    "n_prior",
)


def anomalies_path(ym: str) -> str:
    return f"gold/anomalies/ano_mes={ym}/part-0.parquet"


@dataclass(frozen=True)
class AnomalyStats:
    month: str
    rows: int
    scored_reports: int
    by_severity: dict[str, int] = field(default_factory=dict)
    by_type: dict[str, int] = field(default_factory=dict)
    new_high: int = 0


def _load_inputs(con: duckdb.DuckDBPyConnection, root: Path, ym: str, cfg: AnomalyConfig) -> None:
    # history for the rolling windows, plus the next month (when present) so the last day of
    # the month can be checked for a reversal; rolling statistics only look backwards
    months = [shift_month(ym, -i) for i in range(cfg.history_months, -2, -1)]
    gold.enrich(con, root, months, table="anomaly_input")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE base AS
        SELECT cnpj, subclass_id, dt, peer_group, peer_source, anbima_class,
               quota::DOUBLE AS quota, equity::DOUBLE AS equity,
               subscriptions::DOUBLE AS subscriptions, redemptions::DOUBLE AS redemptions,
               shareholders::DOUBLE AS shareholders
        FROM anomaly_input
    """)
    con.execute("DROP TABLE anomaly_input")
    con.execute("CREATE OR REPLACE TEMP TABLE mad_floors (peer_group VARCHAR, mad_floor DOUBLE)")
    con.executemany("INSERT INTO mad_floors VALUES (?, ?)", list(cfg.mad_floor.items()))


def build_features(con: duckdb.DuckDBPyConnection, root: Path, ym: str, cfg: AnomalyConfig) -> None:
    """Temp tables ``features`` and ``flags`` for the month and its history."""
    _load_inputs(con, root, ym, cfg)
    sql = Template(FEATURES_SQL).substitute(
        window=cfg.window,
        market_day_share=cfg.market_day_share,
        min_peers=cfg.min_peers,
        min_history=cfg.min_history,
        residual_floor=cfg.residual_floor,
        unclassified=gold.UNCLASSIFIED,
    )
    con.execute(sql)
    con.execute(rules.flags_sql(cfg))


def detect_month(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    ym: str,
    cfg: AnomalyConfig,
    run_id: str,
) -> AnomalyStats:
    """Needs ``gold.load_registry`` on ``con`` and silver fund_daily of ``ym`` in ``root``."""
    build_features(con, root, ym, cfg)
    start, end = month_bounds(ym)
    in_month = f"dt BETWEEN DATE '{start}' AND DATE '{end}'"
    features = ", ".join(FEATURE_COLUMNS)

    con.execute(f"CREATE OR REPLACE TEMP TABLE month_flags AS SELECT * FROM flags WHERE {in_month}")
    candidates = " UNION ALL ".join(f"({s})" for s in rules.anomaly_selects(cfg, "month_flags"))
    detected = f"""
        SELECT cnpj, subclass_id, dt, anomaly_type, score, severity, peer_group,
               to_json(struct_pack({features}))::VARCHAR AS features
        FROM ({candidates}) x
    """
    duplicates = rules.duplicates_sql(con, root, ym)
    if duplicates:
        detected += f"""
        UNION ALL
        SELECT d.cnpj, d.subclass_id, d.dt, 'duplicate_report', d.copies::DOUBLE, 'medium',
               coalesce(f.peer_group, '{gold.UNCLASSIFIED}'),
               to_json(struct_pack(copies := d.copies,
                                   distinct_versions := d.distinct_versions))::VARCHAR
        FROM ({duplicates}) d
        LEFT JOIN month_flags f USING (cnpj, subclass_id, dt)
        """

    previous = root / anomalies_path(ym)
    if previous.exists():
        prior = f"SELECT fund_id, class_id, dt, anomaly_type, first_seen_run FROM {lit(previous)}"
    else:
        prior = (
            "SELECT NULL::VARCHAR fund_id, NULL::VARCHAR class_id, NULL::DATE dt,"
            " NULL::VARCHAR anomaly_type, NULL::VARCHAR first_seen_run WHERE false"
        )
    run = run_id.replace("'", "''")
    model = cfg.model_version.replace("'", "''")

    rows = copy_to_parquet(
        con,
        f"""
        WITH d AS ({detected}), p AS ({prior})
        SELECT d.cnpj AS fund_id, d.subclass_id AS class_id, d.dt, d.anomaly_type,
               d.score, d.severity, d.peer_group, d.features,
               '{model}' AS model_version,
               coalesce(p.first_seen_run, '{run}') AS first_seen_run
        FROM d
        LEFT JOIN p ON p.fund_id = d.cnpj AND p.class_id = d.subclass_id AND p.dt = d.dt
                   AND p.anomaly_type = d.anomaly_type
        WHERE d.{in_month}
        ORDER BY d.dt, d.cnpj, d.subclass_id, d.anomaly_type
        """,
        root,
        anomalies_path(ym),
        schema=ANOMALY_SCHEMA,
    )
    scored = one(con, "SELECT count(*) FROM month_flags")[0]
    path = lit(root / anomalies_path(ym))
    severity = dict(con.execute(f"SELECT severity, count(*) FROM {path} GROUP BY 1").fetchall())
    types = dict(con.execute(f"SELECT anomaly_type, count(*) FROM {path} GROUP BY 1").fetchall())
    new_high = one(
        con,
        f"SELECT count(*) FROM {path} WHERE severity = 'high' AND first_seen_run = '{run}'",
    )[0]
    con.execute("DROP TABLE month_flags; DROP TABLE flags; DROP TABLE features; DROP TABLE base")
    return AnomalyStats(
        month=ym,
        rows=rows,
        scored_reports=scored,
        by_severity=dict(Counter(severity)),
        by_type=dict(sorted(types.items())),
        new_high=new_high,
    )
