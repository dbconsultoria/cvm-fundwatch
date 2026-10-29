"""DuckDB helpers. All transforms run in an in-memory DuckDB inside the runner."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


def connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def one(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    """The single row of ``sql``. Uses ``fetchall``: in DuckDB 2 a partly fetched result
    (``fetchone``) keeps the connection busy and the next query fails."""
    rows = con.execute(sql).fetchall()
    if len(rows) != 1:
        raise ValueError(f"expected one row, got {len(rows)}")
    return rows[0]


def lit(path: Path) -> str:
    """A path as a SQL string literal."""
    return "'" + path.as_posix().replace("'", "''") + "'"


def nullable(schema: pa.Schema) -> pa.Schema:
    return pa.schema([f.with_nullable(True) for f in schema])


def copy_to_parquet(
    con: duckdb.DuckDBPyConnection,
    query: str,
    root: Path,
    rel_path: str,
    schema: pa.Schema | None = None,
) -> int:
    """Write ``query`` to ``root/rel_path`` atomically and return the row count.

    Goes through Arrow instead of ``COPY``: DuckDB 2 marks a column ``NOT NULL`` in the Parquet
    schema when the data happens to have no nulls, so partitions of one table would get
    different schemas. Every column is written as nullable, cast to ``schema`` when given.
    """
    table = con.sql(query).to_arrow_table()
    table = table.cast(nullable(schema if schema is not None else table.schema))
    dest = root / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(dest)
    return table.num_rows
