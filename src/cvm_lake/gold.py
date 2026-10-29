"""Gold layer: business tables built from silver.

- ``daily_flows``: AUM and flows per day, peer group and FIC flag.
- ``returns_brl_usd``: daily quota return per class/subclass in BRL and in USD (PTAX).

Both are partitioned by month. Building month M reads silver M-1 too, so the first day of M has
a previous quota and a previous PTAX.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from cvm_lake import silver
from cvm_lake.db import copy_to_parquet, lit, one
from cvm_lake.periods import month_bounds, shift_month

UNCLASSIFIED = "Não classificado"


def daily_flows_path(ym: str) -> str:
    return f"gold/daily_flows/ano_mes={ym}/part-0.parquet"


def returns_path(ym: str) -> str:
    return f"gold/returns_brl_usd/ano_mes={ym}/part-0.parquet"


def _existing(root: Path, paths: list[str]) -> list[Path]:
    return [root / p for p in paths if (root / p).exists()]


def _parquet_list(paths: list[Path]) -> str:
    return "[" + ", ".join(lit(p) for p in paths) + "]"


def load_registry(con: duckdb.DuckDBPyConnection, root: Path) -> None:
    path = root / silver.FUND_REGISTRY_PATH
    if path.exists():
        con.execute(f"CREATE OR REPLACE TEMP VIEW registry AS SELECT * FROM {lit(path)}")
    else:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE registry (
                cnpj VARCHAR, source VARCHAR, valid_from DATE, valid_to DATE, peer_group VARCHAR,
                cvm_class VARCHAR, anbima_class VARCHAR, is_fic BOOLEAN, class_type VARCHAR,
                situation VARCHAR, fund_cnpj VARCHAR, adapted_on DATE)
        """)


def enrich_months(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> None:
    """Temp table ``enriched``: silver fund_daily of M-1 and M plus peer group."""
    enrich(con, root, [shift_month(ym, -1), ym])


def enrich(
    con: duckdb.DuckDBPyConnection, root: Path, months: list[str], table: str = "enriched"
) -> None:
    """Temp table ``table``: silver fund_daily of ``months`` (those present) plus peer group.

    Peer group precedence (ADR 0017): the legacy class valid on the date, then the earliest
    legacy class for dates before the history starts (most history begins in 2005), then the
    current class.
    """
    paths = _existing(root, [silver.fund_daily_path(ym) for ym in months])
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {table} AS
        WITH d AS (SELECT * FROM read_parquet({_parquet_list(paths)})),
             h AS (SELECT * FROM registry WHERE source = 'cad_fi_hist'),
             h_first AS (
                SELECT cnpj, valid_from, peer_group FROM h
                QUALIFY row_number() OVER (PARTITION BY cnpj ORDER BY valid_from) = 1
             ),
             c AS (SELECT * FROM registry WHERE source = 'registro_classe'),
             j AS (
                SELECT d.*,
                       CASE WHEN h.valid_to IS NULL OR d.dt < h.valid_to THEN h.peer_group END
                           AS hist_peer,
                       CASE WHEN d.dt < hf.valid_from THEN hf.peer_group END AS backfill_peer,
                       c.peer_group AS current_peer,
                       c.is_fic,
                       c.anbima_class
                FROM d
                ASOF LEFT JOIN h ON d.cnpj = h.cnpj AND d.dt >= h.valid_from
                LEFT JOIN h_first hf ON d.cnpj = hf.cnpj
                LEFT JOIN c ON d.cnpj = c.cnpj
             )
        SELECT * EXCLUDE (hist_peer, backfill_peer, current_peer),
               coalesce(hist_peer, backfill_peer, current_peer, '{UNCLASSIFIED}') AS peer_group,
               CASE WHEN hist_peer IS NOT NULL THEN 'cad_fi_hist'
                    WHEN backfill_peer IS NOT NULL THEN 'cad_fi_hist_backfill'
                    WHEN current_peer IS NOT NULL THEN 'registro_classe' END AS peer_source
        FROM j
    """)


def load_ptax(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> None:
    paths = _existing(
        root, [silver.ptax_daily_path(shift_month(ym, -1)), silver.ptax_daily_path(ym)]
    )
    if paths:
        source = _parquet_list(paths)
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE ptax AS SELECT dt, sell FROM read_parquet({source})"
        )
    else:
        con.execute("CREATE OR REPLACE TEMP TABLE ptax (dt DATE, sell DECIMAL(18, 5))")


@dataclass(frozen=True)
class GoldStats:
    month: str
    daily_flows_rows: int
    returns_rows: int
    peer_coverage: float  # share of rows of month M with a peer group from the registry
    aum_silver: float
    aum_gold: float


def build_month(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> GoldStats:
    """Build gold for month M. Needs ``load_registry`` first and silver fund_daily of M."""
    start, end = month_bounds(ym)
    in_month = f"dt BETWEEN DATE '{start}' AND DATE '{end}'"
    enrich_months(con, root, ym)
    load_ptax(con, root, ym)

    flows = copy_to_parquet(
        con,
        f"""
        SELECT dt, peer_group, is_fic,
               count(DISTINCT cnpj) AS n_classes,
               count(*) AS n_reports,
               sum(equity) AS aum,
               sum(subscriptions) AS subscriptions,
               sum(redemptions) AS redemptions,
               sum(subscriptions) - sum(redemptions) AS net_flow,
               sum(shareholders)::BIGINT AS shareholders
        FROM enriched
        WHERE {in_month}
        GROUP BY ALL
        ORDER BY dt, peer_group, is_fic NULLS LAST
        """,
        root,
        daily_flows_path(ym),
    )

    returns = copy_to_parquet(
        con,
        f"""
        WITH l AS (
            SELECT cnpj, subclass_id, dt, peer_group, quota,
                   lag(dt) OVER w AS prev_dt,
                   lag(quota) OVER w AS prev_quota
            FROM enriched
            WINDOW w AS (PARTITION BY cnpj, subclass_id ORDER BY dt)
        )
        SELECT l.cnpj, l.subclass_id, l.dt, l.peer_group, l.quota, l.prev_dt, l.prev_quota,
               datediff('day', l.prev_dt, l.dt) AS gap_days,
               CASE WHEN l.quota > 0 AND l.prev_quota > 0
                    THEN l.quota::DOUBLE / l.prev_quota::DOUBLE - 1 END AS ret_brl,
               pt.dt AS ptax_dt, pt.sell AS ptax, pp.sell AS prev_ptax,
               CASE WHEN l.quota > 0 AND l.prev_quota > 0 AND pt.sell > 0 AND pp.sell > 0
                    THEN (l.quota::DOUBLE / pt.sell::DOUBLE)
                         / (l.prev_quota::DOUBLE / pp.sell::DOUBLE) - 1 END AS ret_usd
        FROM l
        ASOF LEFT JOIN ptax pt ON l.dt >= pt.dt
        ASOF LEFT JOIN ptax pp ON l.prev_dt >= pp.dt
        WHERE l.{in_month}
        ORDER BY l.cnpj, l.subclass_id, l.dt
        """,
        root,
        returns_path(ym),
    )

    coverage, aum_silver = one(
        con,
        f"""
        SELECT coalesce(avg(CASE WHEN peer_source IS NOT NULL THEN 1.0 ELSE 0.0 END), 1.0),
               coalesce(sum(equity), 0)::DOUBLE
        FROM enriched WHERE {in_month}
    """,
    )
    aum_gold = one(
        con, f"SELECT coalesce(sum(aum), 0)::DOUBLE FROM {lit(root / daily_flows_path(ym))}"
    )[0]
    con.execute("DROP TABLE enriched; DROP TABLE ptax")
    return GoldStats(
        month=ym,
        daily_flows_rows=flows,
        returns_rows=returns,
        peer_coverage=float(coverage),
        aum_silver=float(aum_silver),
        aum_gold=float(aum_gold),
    )
