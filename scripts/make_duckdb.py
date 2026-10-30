"""Create a local DuckDB file with views over the lake on Hugging Face, for DBeaver or any
DuckDB client. The file holds view definitions only: every query reads the Parquet files on
Hugging Face. Rerun it to pick up tables that did not exist yet.

    uv run python scripts/make_duckdb.py                  # prod lake -> database/cvm_lake.duckdb
    uv run python scripts/make_duckdb.py --env dev --out database/cvm_lake_dev.duckdb

The dev lake is private: the client needs a Hugging Face secret, created once in it with
``CREATE PERSISTENT SECRET hf (TYPE huggingface, TOKEN 'hf_...');``. This script never stores a
token in the file.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from cvm_lake.config import load_settings

PARTITIONED = {
    "silver": ["fund_daily", "ptax_daily"],
    "gold": ["daily_flows", "returns_brl_usd", "anomalies", "restatements"],
}
SINGLE_FILE = {
    "silver": ["fund_registry"],
}
REPORTING = [
    "aum_flows_daily",
    "returns_monthly",
    "anomalies_daily",
    "anomalies_recent",
    "restatements_monthly",
    "ptax_daily",
]


def view_sources(base: str) -> dict[str, str]:
    views = {}
    for schema, tables in PARTITIONED.items():
        for t in tables:
            views[f"{schema}.{t}"] = (
                f"read_parquet('{base}/{schema}/{t}/*/*.parquet', hive_partitioning = true)"
            )
    for schema, tables in SINGLE_FILE.items():
        for t in tables:
            views[f"{schema}.{t}"] = f"read_parquet('{base}/{schema}/{t}/*.parquet')"
    for t in REPORTING:
        views[f"reporting.{t}"] = f"read_parquet('{base}/gold/reporting/{t}.parquet')"
    return views


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env", choices=["prod", "dev"], default="prod")
    parser.add_argument("--owner", default=os.environ.get("HF_OWNER", "dbconsultoria"))
    parser.add_argument("--out", type=Path, default=Path("database/cvm_lake.duckdb"))
    args = parser.parse_args()

    repo_id = load_settings(args.env).environment.hf_dataset_repo.format(owner=args.owner)
    base = f"hf://datasets/{repo_id}"
    args.out.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    token = os.environ.get("HF_TOKEN")
    if token:  # session only, needed to read a private lake while creating the views
        con.execute(f"CREATE SECRET hf (TYPE huggingface, TOKEN '{token}')")
    # Old storage format so older DuckDB drivers (such as the one DBeaver downloads) open it.
    con.execute(f"ATTACH '{args.out.as_posix()}' AS lake (STORAGE_VERSION 'v1.0.0')")
    con.execute("USE lake")
    for schema in ("silver", "gold", "reporting"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for name, source in view_sources(base).items():
        try:
            con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM {source}")
        except duckdb.HTTPException:
            print(f"{name:32} not in {repo_id} yet, skipped")
            continue
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchall()[0][0]
        print(f"{name:32} {rows:>12,} rows")
    con.execute("USE memory")
    con.execute("DETACH lake")
    print(f"views over {repo_id} written to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
