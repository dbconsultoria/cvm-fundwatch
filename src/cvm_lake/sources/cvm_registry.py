"""CVM fund registry (ADR 0008).

``registro_fundo_classe.zip`` is the current post 175 registry (fund, class, subclass).
``cad_fi_hist.zip`` member ``cad_fi_hist_classe.csv`` is the dated class history used for
dates before a fund adapted to Resolution 175.
"""

from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

from cvm_lake.errors import SchemaError
from cvm_lake.sources._csv import read_csv_as_strings

SOURCE = "cvm_registry"


@dataclass(frozen=True)
class RegistryTable:
    name: str
    archive: str  # key under sources.cvm_registry.archives in config
    member: str
    required_columns: tuple[str, ...]


TABLES: tuple[RegistryTable, ...] = (
    RegistryTable(
        "registro_fundo",
        "registro_fundo_classe",
        "registro_fundo.csv",
        ("ID_Registro_Fundo", "CNPJ_Fundo", "Tipo_Fundo", "Situacao", "Data_Adaptacao_RCVM175"),
    ),
    RegistryTable(
        "registro_classe",
        "registro_fundo_classe",
        "registro_classe.csv",
        (
            "ID_Registro_Fundo",
            "ID_Registro_Classe",
            "CNPJ_Classe",
            "Tipo_Classe",
            "Situacao",
            "Classificacao",
            "Classificacao_Anbima",
        ),
    ),
    RegistryTable(
        "registro_subclasse",
        "registro_fundo_classe",
        "registro_subclasse.csv",
        ("ID_Registro_Classe", "ID_Subclasse", "Situacao"),
    ),
    RegistryTable(
        "cad_fi_hist_classe",
        "cad_fi_hist",
        "cad_fi_hist_classe.csv",
        ("CNPJ_FUNDO", "CLASSE", "DT_INI_CLASSE", "DT_FIM_CLASSE"),
    ),
)


def extract_table(
    zip_path: Path, spec: RegistryTable, *, encoding: str, delimiter: str
) -> pa.Table:
    with zipfile.ZipFile(zip_path) as zf:
        try:
            data = zf.read(spec.member)
        except KeyError as exc:
            raise SchemaError(f"{zip_path.name} has no member {spec.member}") from exc
    table = read_csv_as_strings(data, encoding=encoding, delimiter=delimiter)
    missing = [c for c in spec.required_columns if c not in table.column_names]
    if missing:
        raise SchemaError(f"{spec.member} is missing columns {missing}")
    return table
