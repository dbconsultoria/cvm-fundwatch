"""CSV reading for bronze: every column as a string, empty fields kept as ``''``."""

from __future__ import annotations

import pyarrow as pa
from pyarrow import csv as pacsv

from cvm_lake.errors import SchemaError


def read_header(data: bytes, *, encoding: str, delimiter: str) -> list[str]:
    first_line = data.split(b"\n", 1)[0].rstrip(b"\r")
    return first_line.decode(encoding).split(delimiter)


def read_csv_as_strings(data: bytes, *, encoding: str, delimiter: str) -> pa.Table:
    header = read_header(data, encoding=encoding, delimiter=delimiter)
    if len(set(header)) != len(header):
        raise SchemaError(f"duplicate column names in header: {header}")
    try:
        return pacsv.read_csv(
            pa.BufferReader(data),
            read_options=pacsv.ReadOptions(encoding=encoding, block_size=1 << 24),
            parse_options=pacsv.ParseOptions(delimiter=delimiter),
            convert_options=pacsv.ConvertOptions(
                column_types=dict.fromkeys(header, pa.string()),
                strings_can_be_null=False,
                quoted_strings_can_be_null=False,
            ),
        )
    except pa.ArrowInvalid as exc:
        raise SchemaError(f"malformed CSV: {exc}") from exc
