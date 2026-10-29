"""Restatements: values CVM changed after first publishing them (ADR 0032).

Before silver ``fund_daily`` of a month is rebuilt, the previous version (the one in the repo,
downloaded by the run) is kept aside. After the rebuild, every changed value is appended to
``gold/restatements/ano_mes=YYYYMM``. Rows are never removed: the table is the history of
corrections, a weak label for the anomaly detector.

``flagged_before`` lists the anomaly types the detector had on the *old* version of that report
(read from the previous ``gold/anomalies`` partition before anomalies are recomputed), so
"did we flag what CVM later corrected?" is a simple query.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

from cvm_lake import silver
from cvm_lake.anomalies.detector import anomalies_path
from cvm_lake.db import copy_to_parquet, lit, one

FIELDS = ("quota", "equity", "subscriptions", "redemptions", "shareholders", "portfolio_value")

RESTATEMENT_SCHEMA = pa.schema(
    [
        ("fund_id", pa.string()),
        ("class_id", pa.string()),
        ("dt", pa.date32()),
        ("field", pa.string()),  # a column of silver fund_daily, or '*' for a whole report
        ("change", pa.string()),  # changed | added | removed
        ("old_value", pa.string()),  # exact decimal text
        ("new_value", pa.string()),
        ("rel_change", pa.float64()),  # (new - old) / |old|
        ("flagged_before", pa.string()),  # anomaly types on the old version, comma separated
        ("old_source_sha256", pa.string()),
        ("new_source_sha256", pa.string()),
        ("old_commit", pa.string()),  # HF commit of the old version, when known
        ("detected_run", pa.string()),
    ]
)


def restatements_path(ym: str) -> str:
    return f"gold/restatements/ano_mes={ym}/part-0.parquet"


def keep_previous(root: Path, ym: str, work_dir: Path) -> Path | None:
    """Copy the current silver partition aside before it is rebuilt."""
    src = root / silver.fund_daily_path(ym)
    if not src.exists():
        return None
    dest = work_dir / "previous_silver" / f"{ym}.parquet"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)
    return dest


@dataclass(frozen=True)
class RestatementStats:
    month: str
    new_rows: int
    total_rows: int
    changed_reports: int
    flagged_before: int  # changed reports the old version had flagged


def _sources(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    rows = con.execute(f"SELECT DISTINCT source_sha256 FROM {lit(path)}").fetchall()
    return sorted(r[0] for r in rows)


def detect_restatements(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    ym: str,
    previous: Path,
    run_id: str,
    old_commit: str | None,
) -> RestatementStats | None:
    """Append the differences between ``previous`` and the rebuilt silver partition.

    Returns ``None`` when the source file did not change (nothing can have been restated).
    """
    current = root / silver.fund_daily_path(ym)
    old_src, new_src = _sources(con, previous), _sources(con, current)
    if old_src == new_src:
        return None

    old_anomalies = root / anomalies_path(ym)
    if old_anomalies.exists():
        flags = (
            "SELECT fund_id, class_id, dt, "
            "string_agg(DISTINCT anomaly_type, ',' ORDER BY anomaly_type) AS types "
            f"FROM {lit(old_anomalies)} GROUP BY ALL"
        )
    else:
        flags = (
            "SELECT NULL::VARCHAR fund_id, NULL::VARCHAR class_id, NULL::DATE dt, "
            "NULL::VARCHAR types WHERE false"
        )
    key = "cnpj, subclass_id, dt"
    per_field = " UNION ALL ".join(
        f"""SELECT {key}, '{f}' AS field, 'changed' AS change,
                   CAST(o.{f} AS VARCHAR) AS old_value, CAST(n.{f} AS VARCHAR) AS new_value,
                   CASE WHEN o.{f} <> 0 THEN (n.{f}::DOUBLE - o.{f}::DOUBLE) / abs(o.{f}::DOUBLE)
                   END AS rel_change
            FROM o JOIN n USING ({key})
            WHERE o.{f} IS DISTINCT FROM n.{f}"""
        for f in FIELDS
    )
    run = run_id.replace("'", "''")
    commit = "NULL" if old_commit is None else "'" + old_commit.replace("'", "''") + "'"
    old_sha, new_sha = ",".join(old_src), ",".join(new_src)
    new_rows_sql = f"""
        WITH o AS (SELECT * FROM read_parquet({lit(previous)}, hive_partitioning = false)),
             n AS (SELECT * FROM read_parquet({lit(current)}, hive_partitioning = false)),
             diff AS (
                {per_field}
                UNION ALL
                SELECT {key}, '*', 'removed', NULL, NULL, NULL FROM o ANTI JOIN n USING ({key})
                UNION ALL
                SELECT {key}, '*', 'added', NULL, NULL, NULL FROM n ANTI JOIN o USING ({key})
             ),
             f AS ({flags})
        SELECT d.cnpj AS fund_id, d.subclass_id AS class_id, d.dt, d.field, d.change,
               d.old_value, d.new_value, d.rel_change, coalesce(f.types, '') AS flagged_before,
               '{old_sha}' AS old_source_sha256, '{new_sha}' AS new_source_sha256,
               {commit} AS old_commit, '{run}' AS detected_run
        FROM diff d
        LEFT JOIN f ON f.fund_id = d.cnpj AND f.class_id = d.subclass_id AND f.dt = d.dt
    """
    con.execute(f"CREATE OR REPLACE TEMP TABLE new_restatements AS {new_rows_sql}")
    new_rows = one(con, "SELECT count(*) FROM new_restatements")[0]
    if new_rows == 0:
        con.execute("DROP TABLE new_restatements")
        return RestatementStats(ym, 0, 0, 0, 0)

    existing = root / restatements_path(ym)
    union = "SELECT * FROM new_restatements"
    if existing.exists():
        union = (
            f"SELECT * FROM read_parquet({lit(existing)}, hive_partitioning = false) "
            f"WHERE detected_run <> '{run}' UNION ALL {union}"
        )
    total = copy_to_parquet(
        con,
        f"SELECT * FROM ({union}) ORDER BY detected_run, dt, fund_id, class_id, field",
        root,
        restatements_path(ym),
        schema=RESTATEMENT_SCHEMA,
    )
    changed, flagged = one(
        con,
        "SELECT count(DISTINCT (fund_id, class_id, dt)), "
        "count(DISTINCT (fund_id, class_id, dt)) FILTER (WHERE flagged_before <> '') "
        "FROM new_restatements",
    )
    con.execute("DROP TABLE new_restatements")
    return RestatementStats(ym, new_rows, total, changed, flagged)
