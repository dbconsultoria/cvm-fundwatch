"""Synthetic error injection for evaluation (ADR 0031).

Errors are written into a copy of silver ``fund_daily`` before features are computed, exactly as
a wrong source file would arrive. Each injection goes to a different fund, on a report that has
enough history for every layer and a next report. The next report after a one day error (or
after a stale run) is an *echo*: it is excluded from scoring, because a correct value right
after a wrong one looks like a second jump.
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import duckdb

from cvm_lake import silver
from cvm_lake.db import copy_to_parquet, lit
from cvm_lake.periods import month_bounds, shift_month

STALE_REPORTS = 6

# kind -> anomaly types that mean "detected as the right problem"
EXPECTED_TYPES: dict[str, frozenset[str]] = {
    "decimal_shift": frozenset({"decimal_shift"}),
    "quota_glitch": frozenset({"return_outlier", "equity_residual"}),
    "equity_jump": frozenset({"equity_residual", "structural_event"}),
    "stale_quota": frozenset({"stale_quota"}),
    "negative_equity": frozenset({"impossible_value"}),
    "missing_quota": frozenset({"missing_value"}),
    "shareholder_spike": frozenset({"shareholder_spike", "structural_event"}),
}
KINDS = tuple(EXPECTED_TYPES)


@dataclass(frozen=True)
class Injection:
    cnpj: str
    subclass_id: str
    dt: date  # first wrong report
    kind: str
    field: str
    factor: float | None  # multiplier, when the kind multiplies a value
    until: date  # last wrong report (== dt for one day errors)
    echo_dt: date | None  # next report after the error, excluded from scoring
    stale_value: str | None = None  # quota repeated by a stale run

    def as_dict(self) -> dict[str, object]:
        d = asdict(self)
        for k in ("dt", "until", "echo_dt"):
            d[k] = str(d[k]) if d[k] is not None else None
        return d


def _candidates(
    con: duckdb.DuckDBPyConnection, root: Path, ym: str, min_history: int, history_months: int
) -> list[tuple]:
    months = [shift_month(ym, -i) for i in range(history_months, -2, -1)]
    paths = [root / silver.fund_daily_path(m) for m in months]
    files = "[" + ", ".join(lit(p) for p in paths if p.exists()) + "]"
    start, end = month_bounds(ym)
    return con.execute(f"""
        WITH s AS (
            SELECT cnpj, subclass_id, dt, quota, equity, shareholders,
                   row_number() OVER k - 1 AS n_prior,
                   lag(quota) OVER k AS prev_quota,
                   lead(dt) OVER k AS next_dt,
                   lead(dt, {STALE_REPORTS - 1}) OVER k AS stale_until,
                   lead(dt, {STALE_REPORTS}) OVER k AS stale_echo
            FROM read_parquet({files})
            WINDOW k AS (PARTITION BY cnpj, subclass_id ORDER BY dt)
        )
        SELECT cnpj, subclass_id, dt, next_dt, stale_until, stale_echo,
               CAST(prev_quota AS VARCHAR), shareholders
        FROM s
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
          AND n_prior >= {min_history} AND next_dt IS NOT NULL
          AND quota > 0 AND equity > 0 AND prev_quota > 0 AND shareholders IS NOT NULL
        ORDER BY cnpj, subclass_id, dt
    """).fetchall()


def plan_injections(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    ym: str,
    counts: Mapping[str, int],
    seed: int,
    min_history: int,
    history_months: int,
) -> list[Injection]:
    """Deterministic for a given seed and silver content."""
    unknown = set(counts) - set(KINDS)
    if unknown:
        raise ValueError(f"unknown injection kinds {sorted(unknown)}")
    by_fund: dict[tuple[str, str], list[tuple]] = {}
    for row in _candidates(con, root, ym, min_history, history_months):
        by_fund.setdefault((row[0], row[1]), []).append(row)
    rng = random.Random(seed)
    funds = sorted(by_fund)
    rng.shuffle(funds)

    plan: list[Injection] = []
    pool = iter(funds)
    for kind in KINDS:  # fixed order, so adding a kind does not reshuffle the others
        for _ in range(counts.get(kind, 0)):
            injection = None
            while injection is None:
                fund = next(pool, None)
                if fund is None:
                    raise ValueError(f"not enough eligible funds in {ym} for the injection plan")
                injection = _make(kind, by_fund[fund], rng)
            plan.append(injection)
    return plan


def _make(kind: str, rows: list[tuple], rng: random.Random) -> Injection | None:
    if kind == "stale_quota":
        rows = [r for r in rows if r[4] is not None and r[5] is not None]
    if kind == "shareholder_spike":
        rows = [r for r in rows if r[7] >= 20]
    if not rows:
        return None
    cnpj, sub, dt, next_dt, stale_until, stale_echo, prev_quota, _ = rng.choice(rows)
    sign = rng.choice((-1, 1))
    if kind == "decimal_shift":
        field = rng.choice(("quota", "equity"))
        power = rng.choice((-3, -2, -1, 1, 2, 3))
        return Injection(cnpj, sub, dt, kind, field, 10.0**power, dt, next_dt)
    if kind == "quota_glitch":
        factor = math.exp(sign * rng.uniform(math.log(1.02), math.log(1.20)))
        return Injection(cnpj, sub, dt, kind, "quota", factor, dt, next_dt)
    if kind == "equity_jump":
        return Injection(
            cnpj, sub, dt, kind, "equity", 1 + sign * rng.uniform(0.05, 0.5), dt, next_dt
        )
    if kind == "stale_quota":
        return Injection(cnpj, sub, dt, kind, "quota", None, stale_until, stale_echo, prev_quota)
    if kind == "negative_equity":
        return Injection(cnpj, sub, dt, kind, "equity", -1.0, dt, next_dt)
    if kind == "missing_quota":
        return Injection(cnpj, sub, dt, kind, "quota", None, dt, next_dt)
    if kind == "shareholder_spike":
        return Injection(cnpj, sub, dt, kind, "shareholders", 10.0, dt, next_dt)
    raise ValueError(kind)


def apply_injections(
    con: duckdb.DuckDBPyConnection, root: Path, ym: str, injections: list[Injection]
) -> None:
    """Rewrite silver ``fund_daily`` of ``ym`` under ``root`` with the errors applied."""
    path = root / silver.fund_daily_path(ym)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE inj_target AS "
        f"SELECT * FROM read_parquet({lit(path)}, hive_partitioning = false)"
    )
    con.execute("""
        CREATE OR REPLACE TEMP TABLE inj (
            cnpj VARCHAR, subclass_id VARCHAR, dt DATE, until DATE, kind VARCHAR,
            field VARCHAR, factor DOUBLE, stale_value VARCHAR)
    """)
    con.executemany(
        "INSERT INTO inj VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (i.cnpj, i.subclass_id, i.dt, i.until, i.kind, i.field, i.factor, i.stale_value)
            for i in injections
        ],
    )
    match = "t.cnpj = i.cnpj AND t.subclass_id = i.subclass_id AND t.dt BETWEEN i.dt AND i.until"
    updates = {
        "quota_mult": "quota = CAST(t.quota * i.factor AS DECIMAL(38, 12))",
        "equity_mult": "equity = CAST(t.equity * i.factor AS DECIMAL(38, 2))",
        "shareholders_mult": "shareholders = CAST(t.shareholders * i.factor AS INTEGER)",
        "stale": "quota = CAST(i.stale_value AS DECIMAL(38, 12))",
        "missing": "quota = NULL",
    }
    conditions = {
        "quota_mult": "i.field = 'quota' AND i.factor IS NOT NULL",
        "equity_mult": "i.field = 'equity'",
        "shareholders_mult": "i.field = 'shareholders'",
        "stale": "i.kind = 'stale_quota'",
        "missing": "i.kind = 'missing_quota'",
    }
    for name, assignment in updates.items():
        con.execute(
            f"UPDATE inj_target t SET {assignment} FROM inj i WHERE {match} AND {conditions[name]}"
        )
    copy_to_parquet(
        con,
        "SELECT * FROM inj_target ORDER BY cnpj, subclass_id, dt",
        root,
        silver.fund_daily_path(ym),
        schema=silver.FUND_DAILY_SCHEMA,
    )
    con.execute("DROP TABLE inj_target; DROP TABLE inj")
