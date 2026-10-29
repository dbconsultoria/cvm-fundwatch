from datetime import date
from pathlib import Path

import pytest

from cvm_lake.errors import SchemaError
from cvm_lake.sources import bcb_ptax, cvm_inf_diario, cvm_registry

from fakes import (
    INF_HIST_V0_200501,
    INF_V1_202309,
    INF_V1_YEAR_2000,
    INF_V2_202610,
    PTAX_202504_BODY,
    REGISTRO_FUNDO,
    cad_hist_zip,
    csv_bytes,
    inf_monthly_zip,
    ptax_body,
    registro_zip,
    zip_bytes,
)

OPTS = {"encoding": "latin-1", "delimiter": ";"}


def _write(tmp_path: Path, name: str, data: bytes) -> Path:
    path = tmp_path / name
    path.write_bytes(data)
    return path


# inf_diario -------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("lines", "layout"),
    [(INF_HIST_V0_200501, "hist_v0"), (INF_V1_202309, "v1"), (INF_V2_202610, "v2")],
)
def test_detect_layout(lines: list[str], layout: str) -> None:
    assert cvm_inf_diario.detect_layout(lines[0].split(";")) == layout


def test_unknown_layout_fails() -> None:
    header = [*INF_V2_202610[0].split(";"), "NEW_COLUMN"]
    with pytest.raises(SchemaError, match="unknown inf_diario layout"):
        cvm_inf_diario.detect_layout(header)


def test_extract_monthly_v1_keeps_exact_strings(tmp_path: Path) -> None:
    path = _write(tmp_path, "m.zip", inf_monthly_zip("202309", INF_V1_202309))
    ex = cvm_inf_diario.extract_month(path, "202309", **OPTS)
    assert ex.layout == "v1"
    assert ex.member == "inf_diario_fi_202309.csv"
    assert ex.table.num_rows == 3
    assert ex.table["VL_QUOTA"][0].as_py() == "33.227143000000"
    assert ex.table["CNPJ_FUNDO"][0].as_py() == "00.017.024/0001-53"


def test_extract_monthly_v2_keeps_empty_subclass_as_empty_string(tmp_path: Path) -> None:
    path = _write(tmp_path, "m.zip", inf_monthly_zip("202610", INF_V2_202610))
    ex = cvm_inf_diario.extract_month(path, "202610", **OPTS)
    assert ex.layout == "v2"
    assert ex.table["ID_SUBCLASSE"].to_pylist() == ["", "", "RBMFN1747320951", "MZMRC1747322915"]
    assert ex.table["ID_SUBCLASSE"].null_count == 0


def test_extract_from_hist_zip_with_monthly_members(tmp_path: Path) -> None:
    data = zip_bytes(
        {
            "inf_diario_fi_200501.csv": csv_bytes(INF_HIST_V0_200501),
            "inf_diario_fi_200502.csv": csv_bytes(INF_HIST_V0_200501[:1]),
        }
    )
    ex = cvm_inf_diario.extract_month(_write(tmp_path, "y.zip", data), "200501", **OPTS)
    assert (ex.layout, ex.member, ex.table.num_rows) == ("hist_v0", "inf_diario_fi_200501.csv", 1)


def test_extract_from_hist_zip_with_single_yearly_member_filters_month(tmp_path: Path) -> None:
    data = zip_bytes({"inf_diario_fi_2000.csv": csv_bytes(INF_V1_YEAR_2000)})
    ex = cvm_inf_diario.extract_month(_write(tmp_path, "y.zip", data), "200002", **OPTS)
    assert ex.member == "inf_diario_fi_2000.csv"
    assert ex.table["DT_COMPTC"].to_pylist() == ["2000-02-01", "2000-02-02"]
    assert ex.table["VL_TOTAL"].to_pylist() == ["", ""]


def test_extract_missing_member_fails(tmp_path: Path) -> None:
    path = _write(tmp_path, "m.zip", inf_monthly_zip("202309", INF_V1_202309))
    with pytest.raises(SchemaError, match="no member for 202310"):
        cvm_inf_diario.extract_month(path, "202310", **OPTS)


def test_ragged_row_fails(tmp_path: Path) -> None:
    lines = [*INF_V1_202309, "FI;00.017.024/0001-53;2023-09-06;1.00"]
    path = _write(tmp_path, "m.zip", inf_monthly_zip("202309", lines))
    with pytest.raises(SchemaError, match="malformed CSV"):
        cvm_inf_diario.extract_month(path, "202309", **OPTS)


# registry -----------------------------------------------------------------------------------


def _spec(name: str) -> cvm_registry.RegistryTable:
    return next(t for t in cvm_registry.TABLES if t.name == name)


def test_registry_decodes_latin1(tmp_path: Path) -> None:
    path = _write(tmp_path, "r.zip", registro_zip())
    table = cvm_registry.extract_table(path, _spec("registro_classe"), **OPTS)
    assert table["Classificacao"][0].as_py() == "Ações"
    assert table["Publico_Alvo"][0].as_py() == "Público Geral"
    fundo = cvm_registry.extract_table(path, _spec("registro_fundo"), **OPTS)
    assert fundo["Denominacao_Social"][0].as_py() == "FUNDO AÇÕES ITAÚ"


def test_registry_hist_classe_member(tmp_path: Path) -> None:
    path = _write(tmp_path, "h.zip", cad_hist_zip())
    table = cvm_registry.extract_table(path, _spec("cad_fi_hist_classe"), **OPTS)
    assert table.num_rows == 2
    assert table["DT_FIM_CLASSE"].to_pylist() == ["", ""]


def test_registry_missing_column_fails(tmp_path: Path) -> None:
    header = REGISTRO_FUNDO[0].replace("Data_Adaptacao_RCVM175", "Renamed")
    data = zip_bytes({"registro_fundo.csv": csv_bytes([header, REGISTRO_FUNDO[1]])})
    with pytest.raises(SchemaError, match="Data_Adaptacao_RCVM175"):
        cvm_registry.extract_table(_write(tmp_path, "r.zip", data), _spec("registro_fundo"), **OPTS)


# PTAX ---------------------------------------------------------------------------------------


def test_ptax_url_uses_month_day_year() -> None:
    url = bcb_ptax.period_url("https://x/odata/", date(2020, 3, 1), date(2020, 3, 31))
    assert url == (
        "https://x/odata/CotacaoDolarPeriodo(dataInicial=@dataInicial,"
        "dataFinalCotacao=@dataFinalCotacao)?@dataInicial='03-01-2020'"
        "&@dataFinalCotacao='03-31-2020'&$format=json"
    )


def test_ptax_parse_keeps_exact_decimal_text_and_duplicates() -> None:
    rows = bcb_ptax.parse_response(PTAX_202504_BODY)
    assert rows[0] == {
        "cotacaoCompra": "5.74900",
        "cotacaoVenda": "5.74960",
        "dataHoraCotacao": "2025-04-22 13:09:35.629",
    }
    assert len(rows) == 3  # bronze keeps the real duplicate of 2025-04-23; silver dedupes


def test_ptax_hash_ignores_odata_context() -> None:
    a = bcb_ptax.parse_response(PTAX_202504_BODY)
    b = bcb_ptax.parse_response(PTAX_202504_BODY.replace(b"was-p.bcnet", b"other-host"))
    assert bcb_ptax.content_sha256(a) == bcb_ptax.content_sha256(b)


def test_ptax_empty_and_malformed() -> None:
    assert bcb_ptax.parse_response(ptax_body([])) == []
    with pytest.raises(SchemaError):
        bcb_ptax.parse_response(b"<html>maintenance</html>")
    with pytest.raises(SchemaError, match="missing"):
        bcb_ptax.parse_response(ptax_body([{"cotacaoCompra": 1}]))
