from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cvm_lake import silver
from cvm_lake.db import connect
from cvm_lake.sources.cvm_inf_diario import LAYOUTS

from fakes import (
    CAD_FI_HIST_CLASSE,
    INF_HIST_V0_200501,
    INF_V1_202309,
    INF_V1_YEAR_2000,
    INF_V2_202610,
    PTAX_202504_BODY,
    REGISTRO_CLASSE,
    REGISTRO_FUNDO,
    write_bronze_inf,
    write_bronze_ptax,
    write_bronze_registry,
)

PEERS = {
    "Renda Fixa": "Renda Fixa",
    "Referenciado": "Renda Fixa",
    "Ações": "Ações",
    "Multimercado": "Multimercado",
}


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def _rows(root: Path, rel: str) -> list[dict[str, object]]:
    return pq.read_table(root / rel).to_pylist()


# fund_daily ---------------------------------------------------------------------------------


def test_every_bronze_layout_has_a_silver_mapping() -> None:
    assert set(silver.LAYOUT_COLUMNS) == set(LAYOUTS)


def test_all_layouts_map_to_the_same_schema(root: Path) -> None:
    con = connect()
    for ym, lines in [
        ("200501", INF_HIST_V0_200501),
        ("202309", INF_V1_202309),
        ("202610", INF_V2_202610),
    ]:
        write_bronze_inf(root, ym, lines)
        silver.build_fund_daily(con, root, ym)
        schema = pq.read_schema(root / silver.fund_daily_path(ym))
        assert schema.equals(silver.FUND_DAILY_SCHEMA), ym
        assert all(f.nullable for f in schema), ym


def test_typed_values_are_exact(root: Path) -> None:
    write_bronze_inf(root, "202309", INF_V1_202309)
    stats = silver.build_fund_daily(connect(), root, "202309")
    assert (stats.layout, stats.rows, stats.unparseable) == ("v1", 3, 0)
    first = _rows(root, silver.fund_daily_path("202309"))[0]
    assert first == {
        "cnpj": "00017024000153",
        "subclass_id": "",
        "dt": date(2023, 9, 1),
        "report_type": "FI",
        "portfolio_value": Decimal("1110371.15"),
        "quota": Decimal("33.227143000000"),
        "equity": Decimal("1115244.80"),
        "subscriptions": Decimal("0.00"),
        "redemptions": Decimal("0.00"),
        "shareholders": 1,
        "source_layout": "v1",
        "source_sha256": "sha",
    }


def test_hist_v0_has_no_report_type_and_empty_fields_become_null(root: Path) -> None:
    con = connect()
    write_bronze_inf(root, "200501", INF_HIST_V0_200501)
    silver.build_fund_daily(con, root, "200501")
    assert _rows(root, silver.fund_daily_path("200501"))[0]["report_type"] is None

    jan_2000 = [INF_V1_YEAR_2000[0], INF_V1_YEAR_2000[1]]
    write_bronze_inf(root, "200001", jan_2000)
    silver.build_fund_daily(con, root, "200001")
    row = _rows(root, silver.fund_daily_path("200001"))[0]
    assert (row["portfolio_value"], row["shareholders"]) == (None, None)


def test_subclasses_are_separate_keys(root: Path) -> None:
    write_bronze_inf(root, "202610", INF_V2_202610)
    silver.build_fund_daily(connect(), root, "202610")
    keys = [(r["cnpj"], r["subclass_id"]) for r in _rows(root, silver.fund_daily_path("202610"))]
    assert ("11111111000111", "MZMRC1747322915") in keys
    assert ("11111111000111", "RBMFN1747320951") in keys


def test_out_of_month_rows_dropped_and_last_duplicate_wins(root: Path) -> None:
    header, first = INF_V1_202309[0], INF_V1_202309[1]
    lines = [
        header,
        first,
        first.replace("1115244.80", "999.99"),  # same key again, later in the file
        first.replace("2023-09-01", "2023-08-31"),  # previous month
    ]
    write_bronze_inf(root, "202309", lines)
    stats = silver.build_fund_daily(connect(), root, "202309")
    assert (stats.rows, stats.out_of_month, stats.duplicates_removed) == (1, 1, 1)
    assert _rows(root, silver.fund_daily_path("202309"))[0]["equity"] == Decimal("999.99")


def test_unparseable_values_are_counted(root: Path) -> None:
    lines = [INF_V1_202309[0], INF_V1_202309[1].replace("33.227143000000", "33,22")]
    write_bronze_inf(root, "202309", lines)
    stats = silver.build_fund_daily(connect(), root, "202309")
    assert stats.unparseable == 1
    assert _rows(root, silver.fund_daily_path("202309"))[0]["quota"] is None


# fund_registry ------------------------------------------------------------------------------

FUNDO_HEADER = REGISTRO_FUNDO[0]
CLASSE_HEADER = REGISTRO_CLASSE[0]
HIST_HEADER = CAD_FI_HIST_CLASSE[0]


def _fundo(id_: str, cnpj: str, adapted: str, situacao: str = "Em Funcionamento Normal") -> str:
    cols = dict.fromkeys(FUNDO_HEADER.split(";"), "")
    cols.update(
        ID_Registro_Fundo=id_,
        CNPJ_Fundo=cnpj,
        Situacao=situacao,
        Data_Registro="2000-01-01",
        Data_Adaptacao_RCVM175=adapted,
    )
    return ";".join(cols.values())


def _classe(id_fundo: str, id_: str, cnpj: str, classif: str, situacao: str, registro: str) -> str:
    cols = dict.fromkeys(CLASSE_HEADER.split(";"), "")
    cols.update(
        ID_Registro_Fundo=id_fundo,
        ID_Registro_Classe=id_,
        CNPJ_Classe=cnpj,
        Classificacao=classif,
        Classificacao_Anbima="Renda Fixa Duração Livre",
        Classe_Cotas="S",
        Situacao=situacao,
        Data_Registro=registro,
    )
    return ";".join(cols.values())


def _registry(root: Path) -> list[dict[str, object]]:
    write_bronze_registry(
        root,
        "registro_fundo",
        [
            FUNDO_HEADER,
            _fundo("1", "00000000000101", "2025-06-24"),
            _fundo("2", "00000000000202", ""),
        ],
    )
    write_bronze_registry(
        root,
        "registro_classe",
        [
            CLASSE_HEADER,
            # duplicate CNPJ: the cancelled row must lose even though it is newer
            _classe(
                "1", "10", "00000000000101", "Renda Fixa", "Em Funcionamento Normal", "2025-06-24"
            ),
            _classe("1", "11", "00000000000101", "Ações", "Cancelado", "2025-07-01"),
            _classe(
                "2", "20", "00000000000202", "Multimercado", "Em Funcionamento Normal", "2024-01-01"
            ),
        ],
    )
    write_bronze_registry(
        root,
        "cad_fi_hist_classe",
        [
            HIST_HEADER,
            "00.000.000/0001-01;2005-03-31;Referenciado;2005-03-31;2015-12-31",
            "00.000.000/0001-01;2005-03-31;Renda Fixa;2016-01-01;",
            "00.000.000/0001-01;2005-03-31;Ações;2025-07-01;",  # after adaptation: ignored
        ],
    )
    stats = silver.build_fund_registry(connect(), root, PEERS)
    assert stats is not None
    return _rows(root, silver.FUND_REGISTRY_PATH)


def test_registry_current_rows_dedupe_and_peer_mapping(root: Path) -> None:
    rows = _registry(root)
    current = {r["cnpj"]: r for r in rows if r["source"] == "registro_classe"}
    assert current["00000000000101"]["peer_group"] == "Renda Fixa"
    assert current["00000000000101"]["valid_from"] == date(2025, 6, 24)  # adaptation date
    assert current["00000000000101"]["is_fic"] is True
    assert current["00000000000202"]["valid_from"] == date(1900, 1, 1)  # not adapted


def test_registry_history_is_clipped_at_adaptation_and_mapped(root: Path) -> None:
    rows = _registry(root)
    hist = [
        (r["valid_from"], r["valid_to"], r["cvm_class"], r["peer_group"])
        for r in rows
        if r["source"] == "cad_fi_hist"
    ]
    assert hist == [
        (date(2005, 3, 31), date(2016, 1, 1), "Referenciado", "Renda Fixa"),
        (date(2016, 1, 1), date(2025, 6, 24), "Renda Fixa", "Renda Fixa"),
    ]


def test_registry_needs_current_snapshot(root: Path) -> None:
    assert silver.build_fund_registry(connect(), root, PEERS) is None


# ptax_daily ---------------------------------------------------------------------------------


def test_ptax_keeps_latest_quote_per_day(root: Path) -> None:
    write_bronze_ptax(root, "202504", PTAX_202504_BODY)
    stats = silver.build_ptax_daily(connect(), root, "202504")
    assert stats is not None
    assert (stats.rows, stats.duplicates_removed) == (2, 1)
    rows = _rows(root, silver.ptax_daily_path("202504"))
    assert rows[1]["dt"] == date(2025, 4, 23)
    assert rows[1]["quoted_at"].microsecond == 443000
    assert rows[1]["sell"] == Decimal("5.68800")
