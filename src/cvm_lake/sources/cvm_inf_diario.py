"""CVM Informe Diario: layout detection and extraction of one month from a zip.

Monthly zips hold ``inf_diario_fi_YYYYMM.csv``. Yearly HIST zips hold one member per month
(2005 to 2020) or a single ``inf_diario_fi_YYYY.csv`` (2000 to 2004). See docs/sources.md.
"""

from __future__ import annotations

import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from cvm_lake.errors import SchemaError
from cvm_lake.sources._csv import read_csv_as_strings

SOURCE = "cvm_inf_diario"

_VALUES = ("VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST")
LAYOUTS: dict[str, tuple[str, ...]] = {
    "hist_v0": ("CNPJ_FUNDO", "DT_COMPTC", *_VALUES),
    "v1": ("TP_FUNDO", "CNPJ_FUNDO", "DT_COMPTC", *_VALUES),
    "v2": ("TP_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "DT_COMPTC", *_VALUES),
}


@dataclass(frozen=True)
class MonthExtract:
    table: pa.Table
    member: str
    layout: str


def detect_layout(columns: Sequence[str]) -> str:
    cols = tuple(columns)
    for name, expected in LAYOUTS.items():
        if cols == expected:
            return name
    raise SchemaError(f"unknown inf_diario layout: {';'.join(cols)}")


def monthly_url(template: str, ym: str) -> str:
    return template.format(yyyymm=ym)


def yearly_url(template: str, ym: str) -> str:
    return template.format(yyyy=ym[:4])


def extract_month(zip_path: Path, ym: str, *, encoding: str, delimiter: str) -> MonthExtract:
    monthly, yearly = f"inf_diario_fi_{ym}.csv", f"inf_diario_fi_{ym[:4]}.csv"
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        if monthly in names:
            member = monthly
        elif yearly in names:
            member = yearly
        else:
            raise SchemaError(f"{zip_path.name} has no member for {ym}: {sorted(names)}")
        data = zf.read(member)

    table = read_csv_as_strings(data, encoding=encoding, delimiter=delimiter)
    layout = detect_layout(table.column_names)
    if member == yearly:
        table = table.filter(pc.starts_with(table["DT_COMPTC"], f"{ym[:4]}-{ym[4:]}-"))
    return MonthExtract(table=table, member=member, layout=layout)
