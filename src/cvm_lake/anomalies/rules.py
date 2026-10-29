"""Layer 1 (rules) and the decision logic that turns features into anomaly rows.

Every anomaly type is one SQL ``SELECT`` over the temp table ``flags``; the detector unions
them. Severity (ADR 0028):

- high: decimal shift, impossible value, or two independent statistical layers agreeing
- medium: one statistical layer alone, a structural event, other data problems
- low: echoes, explained moves, closings, short gaps
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from cvm_lake import bronze, silver
from cvm_lake.anomalies.settings import AnomalyConfig


def flags_sql(cfg: AnomalyConfig) -> str:
    """Temp table ``flags``: one row per report with boolean layer flags. Reads ``features``.

    Layers not in ``cfg.layers`` are always false; ``cfg.robust = False`` swaps the robust z of
    layer 2 for the classic mean/standard deviation z (an experiment baseline).
    """

    def on(layer: str) -> str:
        return "true" if layer in cfg.layers else "false"

    z_ret = "z_return" if cfg.robust else "z_return_classic"
    shift = f"""
        CASE WHEN {{col}} IS NOT NULL
                  AND abs(round({{col}})) BETWEEN 1 AND {cfg.max_power}
                  AND abs({{col}} - round({{col}})) < {cfg.shift_tolerance}
             THEN round({{col}})::INTEGER ELSE 0 END"""
    return f"""
    CREATE OR REPLACE TEMP TABLE flags AS
    WITH a AS (
        SELECT *,
               {shift.format(col="quota_log10")} AS quota_shift,
               {shift.format(col="equity_log10")} AS equity_shift,
               coalesce(equity < 0 OR (quota <= 0 AND equity > 0) OR shareholders < 0
                        OR subscriptions < 0 OR redemptions < 0, false) AS impossible,
               (quota IS NULL OR equity IS NULL) AS missing,
               n_prior >= {cfg.min_history} AS has_history,
               {
        "coalesce(market_z >= " + str(cfg.market_z) + ", false)" if cfg.market_gate else "false"
    } AS market_move
        FROM features
    ),
    b AS (
        SELECT *,
               lag(quota_shift) OVER k AS prev_quota_shift,
               lag(equity_shift) OVER k AS prev_equity_shift,
               lag(impossible) OVER k AS prev_impossible,
               lag(missing) OVER k AS prev_missing
        FROM a
        WINDOW k AS (PARTITION BY cnpj, subclass_id ORDER BY dt)
    ),
    c AS (
        SELECT *,
               (quota_shift <> 0 AND quota_shift = -prev_quota_shift)
                 OR (equity_shift <> 0 AND equity_shift = -prev_equity_shift) AS echo,
               -- layer 2: own history. Layers 2 and 4 are silenced on a market move and the
               -- report after it (ADR 0028)
               {on("l2")} AND coalesce(has_history AND abs({z_ret}) > {cfg.z_return}
                        AND NOT coalesce(market_move, false), false) AS l2,
               -- layer 3: accounting identity residual
               {on("l3")} AND coalesce(has_history AND abs(residual) >= {cfg.min_abs_residual}
                        AND abs(z_residual) > {cfg.z_residual}, false) AS l3,
               -- layer 4: excess return over the peer group, vs the fund's own excess history
               {on("l4")} AND coalesce(has_history AND abs(z_excess) > {cfg.z_excess}
                        AND NOT coalesce(market_move, false), false) AS l4,
               coalesce((shareholder_ratio >= {cfg.shareholder_ratio}
                         OR shareholder_ratio <= 1.0 / {cfg.shareholder_ratio})
                        AND abs(shareholders - prev_shareholders)
                            >= {cfg.shareholder_min_change}, false) AS shareholder_jump
        FROM b
    )
    SELECT *,
           (quota_shift <> 0 OR equity_shift <> 0) AND NOT echo AS decimal_shift,
           -- a quota move that is undone by the next report: a one day glitch
           coalesce(abs(log_ret + next_log_ret) < 0.25 * abs(log_ret), false) AS reverted,
           -- a quota drop that the equity follows and that does not revert: amortization or
           -- income distribution. Undecided until the next report exists.
           (l2 OR l4) AND log_ret < 0 AND abs(residual) < {cfg.explained_residual}
               AND next_log_ret IS NOT NULL
               AND NOT coalesce(abs(log_ret + next_log_ret) < 0.25 * abs(log_ret), false)
               AS distribution,
           l3 AND shareholder_jump AND NOT (l2 OR l4) AS structural,
           l2::INTEGER + l3::INTEGER + l4::INTEGER AS layers
    FROM c
    """


def anomaly_selects(cfg: AnomalyConfig, table: str = "flags") -> list[str]:
    """One SELECT per anomaly type over ``table``, a ``flags`` table of the scored month."""
    t = table
    stat_ok = "NOT decimal_shift AND NOT echo"
    by_layers = "CASE WHEN layers >= 2 THEN 'high' ELSE 'medium' END"
    selects = [
        "SELECT *, 'decimal_shift' AS anomaly_type, 'high' AS severity,"
        " greatest(abs(coalesce(quota_log10, 0)), abs(coalesce(equity_log10, 0))) AS score"
        " FROM {t} WHERE decimal_shift",
        "SELECT *, 'decimal_shift_echo', 'low',"
        " greatest(abs(coalesce(quota_log10, 0)), abs(coalesce(equity_log10, 0)))"
        " FROM {t} WHERE echo",
        "SELECT *, 'impossible_value', 'high', 1.0"
        " FROM {t} WHERE impossible AND NOT coalesce(prev_impossible, false)",
        "SELECT *, 'missing_value', 'medium', 1.0"
        " FROM {t} WHERE missing AND NOT coalesce(prev_missing, false)",
        "SELECT *, 'fund_closing', 'low', 1.0 FROM {t} WHERE equity = 0 AND prev_equity > 0",
        f"SELECT *, 'reporting_gap',"
        f" CASE WHEN gap_market_days >= {cfg.gap_medium_days} THEN 'medium' ELSE 'low' END,"
        f" gap_market_days::DOUBLE FROM {t} WHERE gap_market_days >= 1",
        f"SELECT *, 'stale_quota', 'medium', unchanged_reports::DOUBLE"
        f" FROM {t} WHERE unchanged_reports = {cfg.stale_reports} AND equity > 0",
        f"SELECT *, 'return_outlier', {by_layers},"
        " greatest(CASE WHEN l2 THEN abs(z_return) ELSE 0 END,"
        "          CASE WHEN l4 THEN abs(z_excess) ELSE 0 END)"
        f" FROM {t} WHERE (l2 OR l4) AND {stat_ok} AND NOT distribution",
        f"SELECT *, 'equity_residual', {by_layers}, abs(z_residual)"
        f" FROM {t} WHERE l3 AND {stat_ok} AND NOT structural",
        f"SELECT *, 'structural_event', 'medium', abs(residual)"
        f" FROM {t} WHERE structural AND {stat_ok}",
        "SELECT *, 'shareholder_spike', 'medium',"
        " abs(ln((shareholders + 1) / (prev_shareholders + 1)))"
        f" FROM {t} WHERE shareholder_jump AND NOT l3 AND {stat_ok}",
        f"SELECT *, 'distribution_event', 'low', abs(log_ret)"
        f" FROM {t} WHERE distribution AND {stat_ok}",
    ]
    return [q.replace("{t}", t) for q in selects]


def duplicates_sql(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> str | None:
    """Keys reported more than once in the source file (silver keeps the last one)."""
    src = root / bronze.inf_diario_path(ym)
    if not src.exists():
        return None
    typed = silver.typed_bronze_sql(src, silver.bronze_layout(con, src))
    return f"""
        SELECT cnpj, subclass_id, dt, count(*) AS copies,
               count(DISTINCT (quota, equity, subscriptions, redemptions, shareholders))
                   AS distinct_versions
        FROM ({typed})
        WHERE dt IS NOT NULL
        GROUP BY ALL
        HAVING count(*) > 1
    """
