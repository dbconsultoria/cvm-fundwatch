"""Silver layer: typed, normalized and deduplicated.

``fund_daily`` has one schema for the three CVM layouts (ADR 0010, 0016). ``fund_registry``
holds class intervals from the legacy history and the current registry (ADR 0017).
``ptax_daily`` has one row per date.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pyarrow as pa

from cvm_lake import bronze
from cvm_lake.db import copy_to_parquet, lit, one
from cvm_lake.errors import SchemaError
from cvm_lake.periods import month_bounds

FUND_REGISTRY_PATH = "silver/fund_registry/part-0.parquet"

FUND_DAILY_SCHEMA = pa.schema(
    [
        ("cnpj", pa.string()),
        ("subclass_id", pa.string()),
        ("dt", pa.date32()),
        ("report_type", pa.string()),
        ("portfolio_value", pa.decimal128(38, 2)),
        ("quota", pa.decimal128(38, 12)),
        ("equity", pa.decimal128(38, 2)),
        ("subscriptions", pa.decimal128(38, 2)),
        ("redemptions", pa.decimal128(38, 2)),
        ("shareholders", pa.int32()),
        ("source_layout", pa.string()),
        ("source_sha256", pa.string()),
    ]
)

# layout -> (CNPJ column, subclass expression, report type expression)
LAYOUT_COLUMNS: dict[str, tuple[str, str, str]] = {
    "hist_v0": ("CNPJ_FUNDO", "''", "CAST(NULL AS VARCHAR)"),
    "v1": ("CNPJ_FUNDO", "''", "TP_FUNDO"),
    "v2": ("CNPJ_FUNDO_CLASSE", "ID_SUBCLASSE", "TP_FUNDO_CLASSE"),
}


def fund_daily_path(ym: str) -> str:
    return f"silver/fund_daily/ano_mes={ym}/part-0.parquet"


def ptax_daily_path(ym: str) -> str:
    return f"silver/ptax_daily/ano_mes={ym}/part-0.parquet"


def normalize_cnpj_sql(expr: str) -> str:
    """Strip punctuation, keep letters for the alphanumeric CNPJ (ADR 0006)."""
    return f"upper(regexp_replace({expr}, '[^0-9A-Za-z]', '', 'g'))"


@dataclass(frozen=True)
class FundDailyStats:
    month: str
    layout: str
    source_rows: int
    rows: int
    unparseable: int  # non empty source value that did not cast
    out_of_month: int  # dropped: date outside the partition month (or unparseable date)
    duplicates_removed: int  # same (cnpj, subclass_id, dt) more than once, last row kept


def bronze_layout(con: duckdb.DuckDBPyConnection, src: Path) -> str:
    layouts = [r[0] for r in con.execute(f"SELECT DISTINCT _layout FROM {lit(src)}").fetchall()]
    if len(layouts) != 1 or layouts[0] not in LAYOUT_COLUMNS:
        raise SchemaError(f"{src}: expected one known layout, found {layouts}")
    return layouts[0]


def typed_bronze_sql(src: Path, layout: str) -> str:
    """Bronze rows typed with the silver names, plus ``source_row`` and ``unparseable``."""
    cnpj_col, subclass_expr, type_expr = LAYOUT_COLUMNS[layout]
    return f"""
        SELECT
            {normalize_cnpj_sql(cnpj_col)} AS cnpj,
            {subclass_expr} AS subclass_id,
            TRY_CAST(DT_COMPTC AS DATE) AS dt,
            NULLIF({type_expr}, '') AS report_type,
            TRY_CAST(NULLIF(VL_TOTAL, '') AS DECIMAL(38, 2)) AS portfolio_value,
            TRY_CAST(NULLIF(VL_QUOTA, '') AS DECIMAL(38, 12)) AS quota,
            TRY_CAST(NULLIF(VL_PATRIM_LIQ, '') AS DECIMAL(38, 2)) AS equity,
            TRY_CAST(NULLIF(CAPTC_DIA, '') AS DECIMAL(38, 2)) AS subscriptions,
            TRY_CAST(NULLIF(RESG_DIA, '') AS DECIMAL(38, 2)) AS redemptions,
            TRY_CAST(NULLIF(NR_COTST, '') AS INTEGER) AS shareholders,
            _layout AS source_layout,
            _source_sha256 AS source_sha256,
            file_row_number AS source_row,
            (VL_TOTAL <> '' AND portfolio_value IS NULL)
              OR (VL_QUOTA <> '' AND quota IS NULL)
              OR (VL_PATRIM_LIQ <> '' AND equity IS NULL)
              OR (CAPTC_DIA <> '' AND subscriptions IS NULL)
              OR (RESG_DIA <> '' AND redemptions IS NULL)
              OR (NR_COTST <> '' AND shareholders IS NULL) AS unparseable
        FROM read_parquet({lit(src)}, file_row_number = true)
    """


def build_fund_daily(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> FundDailyStats:
    src = root / bronze.inf_diario_path(ym)
    layout = bronze_layout(con, src)
    start, end = month_bounds(ym)
    con.execute(f"CREATE OR REPLACE TEMP TABLE fd_typed AS {typed_bronze_sql(src, layout)}")
    total, unparseable, in_month = one(
        con,
        f"""
        SELECT count(*),
               count(*) FILTER (WHERE unparseable),
               count(*) FILTER (WHERE dt BETWEEN DATE '{start}' AND DATE '{end}')
        FROM fd_typed
    """,
    )

    columns = ", ".join(FUND_DAILY_SCHEMA.names)
    rows = copy_to_parquet(
        con,
        f"""
        SELECT {columns}
        FROM fd_typed
        WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'
        QUALIFY row_number() OVER (PARTITION BY cnpj, subclass_id, dt ORDER BY source_row DESC) = 1
        ORDER BY cnpj, subclass_id, dt
        """,
        root,
        fund_daily_path(ym),
        schema=FUND_DAILY_SCHEMA,
    )
    con.execute("DROP TABLE fd_typed")
    return FundDailyStats(
        month=ym,
        layout=layout,
        source_rows=total,
        rows=rows,
        unparseable=unparseable,
        out_of_month=total - in_month,
        duplicates_removed=in_month - rows,
    )


def latest_registry_snapshot(root: Path, table: str) -> Path | None:
    folder = root / "bronze" / "cvm_registry" / table
    snapshots = sorted(folder.glob("snapshot_date=*/part-0.parquet"))
    return snapshots[-1] if snapshots else None


@dataclass(frozen=True)
class RegistryStats:
    rows: int
    current_rows: int
    history_rows: int
    snapshot_date: str


def build_fund_registry(
    con: duckdb.DuckDBPyConnection, root: Path, peer_groups: Mapping[str, str]
) -> RegistryStats | None:
    """Class intervals per CNPJ. ``None`` when the current registry is not in bronze yet."""
    fundo = latest_registry_snapshot(root, "registro_fundo")
    classe = latest_registry_snapshot(root, "registro_classe")
    hist = latest_registry_snapshot(root, "cad_fi_hist_classe")
    if fundo is None or classe is None:
        return None
    snapshot = classe.parent.name.removeprefix("snapshot_date=")

    con.execute("CREATE OR REPLACE TEMP TABLE peer_map (raw VARCHAR, peer VARCHAR)")
    con.executemany("INSERT INTO peer_map VALUES (?, ?)", list(peer_groups.items()))
    peer = "coalesce(pm.peer, CASE WHEN {col} IS NOT NULL THEN 'Outros' END)"
    # Prefer an active row, then the most recent registration, then the highest id.
    not_cancelled_first = "(Situacao = 'Cancelado'), Data_Registro DESC"

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE reg_current AS
        WITH f AS (
            SELECT * FROM read_parquet({lit(fundo)})
            QUALIFY row_number() OVER (
                PARTITION BY ID_Registro_Fundo
                ORDER BY {not_cancelled_first}, CNPJ_Fundo) = 1
        ), c AS (
            SELECT * FROM read_parquet({lit(classe)})
            QUALIFY row_number() OVER (
                PARTITION BY CNPJ_Classe
                ORDER BY {not_cancelled_first},
                         TRY_CAST(ID_Registro_Classe AS BIGINT) DESC) = 1
        )
        SELECT
            {normalize_cnpj_sql("c.CNPJ_Classe")} AS cnpj,
            NULLIF(c.Classificacao, '') AS cvm_class,
            NULLIF(c.Classificacao_Anbima, '') AS anbima_class,
            CASE c.Classe_Cotas WHEN 'S' THEN true WHEN 'N' THEN false END AS is_fic,
            NULLIF(c.Tipo_Classe, '') AS class_type,
            NULLIF(c.Situacao, '') AS situation,
            {normalize_cnpj_sql("f.CNPJ_Fundo")} AS fund_cnpj,
            TRY_CAST(NULLIF(f.Data_Adaptacao_RCVM175, '') AS DATE) AS adapted_on,
            NULLIF(trim(c.Denominacao_Social), '') AS name
        FROM c LEFT JOIN f USING (ID_Registro_Fundo)
    """)
    if hist is not None:
        hist_source = f"""
            SELECT {normalize_cnpj_sql("CNPJ_FUNDO")} AS cnpj,
                   TRY_CAST(DT_INI_CLASSE AS DATE) AS valid_from,
                   TRY_CAST(NULLIF(DT_FIM_CLASSE, '') AS DATE) + 1 AS valid_to,
                   NULLIF(CLASSE, '') AS cvm_class
            FROM read_parquet({lit(hist)})
        """
    else:
        hist_source = """
            SELECT NULL::VARCHAR cnpj, NULL::DATE valid_from, NULL::DATE valid_to,
                   NULL::VARCHAR cvm_class WHERE false
        """

    rows = copy_to_parquet(
        con,
        f"""
        WITH h AS ({hist_source})
        SELECT h.cnpj, 'cad_fi_hist' AS source, h.valid_from,
               -- history stops when the fund adapted to 175; the current registry takes over
               CASE WHEN c.adapted_on IS NULL THEN h.valid_to
                    ELSE least(coalesce(h.valid_to, c.adapted_on), c.adapted_on) END AS valid_to,
               {peer.format(col="h.cvm_class")} AS peer_group,
               h.cvm_class, CAST(NULL AS VARCHAR) AS anbima_class, c.is_fic,
               CAST(NULL AS VARCHAR) AS class_type, CAST(NULL AS VARCHAR) AS situation,
               c.fund_cnpj, c.adapted_on, c.name
        FROM h
        LEFT JOIN reg_current c USING (cnpj)
        LEFT JOIN peer_map pm ON pm.raw = h.cvm_class
        WHERE h.valid_from IS NOT NULL AND (c.adapted_on IS NULL OR h.valid_from < c.adapted_on)
        UNION ALL
        SELECT c.cnpj, 'registro_classe', coalesce(c.adapted_on, DATE '1900-01-01'),
               CAST(NULL AS DATE), {peer.format(col="c.cvm_class")},
               c.cvm_class, c.anbima_class, c.is_fic, c.class_type, c.situation,
               c.fund_cnpj, c.adapted_on, c.name
        FROM reg_current c
        LEFT JOIN peer_map pm ON pm.raw = c.cvm_class
        ORDER BY cnpj, source, valid_from
        """,
        root,
        FUND_REGISTRY_PATH,
    )
    current_rows = one(con, "SELECT count(*) FROM reg_current")[0]
    con.execute("DROP TABLE reg_current; DROP TABLE peer_map")
    return RegistryStats(
        rows=rows,
        current_rows=current_rows,
        history_rows=rows - current_rows,
        snapshot_date=snapshot,
    )


@dataclass(frozen=True)
class PtaxStats:
    month: str
    rows: int
    duplicates_removed: int


def build_ptax_daily(con: duckdb.DuckDBPyConnection, root: Path, ym: str) -> PtaxStats | None:
    src = root / bronze.ptax_path(ym)
    if not src.exists():
        return None
    source_rows = one(con, f"SELECT count(*) FROM {lit(src)}")[0]
    rows = copy_to_parquet(
        con,
        f"""
        SELECT CAST(dataHoraCotacao[1:10] AS DATE) AS dt,
               CAST(cotacaoCompra AS DECIMAL(18, 5)) AS buy,
               CAST(cotacaoVenda AS DECIMAL(18, 5)) AS sell,
               CAST(dataHoraCotacao AS TIMESTAMP) AS quoted_at
        FROM read_parquet({lit(src)})
        QUALIFY row_number() OVER (PARTITION BY dt ORDER BY quoted_at DESC) = 1
        ORDER BY dt
        """,
        root,
        ptax_daily_path(ym),
    )
    return PtaxStats(month=ym, rows=rows, duplicates_removed=source_rows - rows)
