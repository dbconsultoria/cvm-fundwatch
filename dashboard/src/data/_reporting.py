"""Shared helper of the data loaders: DuckDB over the gold/reporting tables, CSV to stdout.

Each loader is a tiny script that Observable Framework runs at build time; its stdout becomes
the data file. Tables missing from REPORTING_DIR are read as empty tables with their schema, so
a young lake still builds a site.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv

from cvm_lake.reporting import SCHEMAS, reporting_path


def reporting_dir() -> Path:
    value = os.environ.get("REPORTING_DIR")
    if not value:
        raise SystemExit("REPORTING_DIR is not set: a folder with gold/reporting/*.parquet")
    return Path(value)


def connect() -> duckdb.DuckDBPyConnection:
    """A connection with one view per reporting table."""
    con = duckdb.connect()
    for name, schema in SCHEMAS.items():
        path = reporting_dir() / reporting_path(name)
        if path.exists():
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path.as_posix()}')")
        else:
            con.register(f"{name}_empty", pa.Table.from_pylist([], schema=schema))
            con.execute(f"CREATE VIEW {name} AS SELECT * FROM {name}_empty")
    return con


def emit_csv(sql: str) -> None:
    table = connect().sql(sql).to_arrow_table()
    pacsv.write_csv(table, sys.stdout.buffer)


def source() -> dict[str, str | None]:
    path = reporting_dir() / "source.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"repo_id": None, "commit": None}
