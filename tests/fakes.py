"""Test doubles and fixture builders.

Rows are copied from real files inspected on 2026-10-29 (see docs/sources.md) unless the line
is marked ``# synthetic``. Files are built in memory with CRLF endings and Latin-1 encoding,
like the real sources.
"""

from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import wait_none

from cvm_lake import bronze, silver
from cvm_lake.http import HttpClient
from cvm_lake.sources import bcb_ptax, cvm_inf_diario
from cvm_lake.sources._csv import read_csv_as_strings

CVM = "https://dados.cvm.gov.br/dados"
INF_MONTHLY = CVM + "/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{}.zip"
INF_HIST = CVM + "/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{}.zip"
REGISTRO_URL = CVM + "/FI/CAD/DADOS/registro_fundo_classe.zip"
CAD_HIST_URL = CVM + "/FI/CAD/DADOS/cad_fi_hist.zip"
PTAX_BASE = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"


def csv_bytes(lines: list[str], encoding: str = "latin-1") -> bytes:
    return ("\r\n".join(lines) + "\r\n").encode(encoding)


def zip_bytes(members: dict[str, bytes]) -> bytes:
    """Deterministic zip: fixed timestamps, so equal members give equal bytes."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in members.items():
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, data)
    return buf.getvalue()


# CVM Informe Diario -------------------------------------------------------------------------

INF_V1_202309 = [
    "TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST",
    "FI;00.017.024/0001-53;2023-09-01;1110371.15;33.227143000000;1115244.80;0.00;0.00;1",
    "FI;00.017.024/0001-53;2023-09-04;1110911.36;33.241320000000;1115720.64;0.00;0.00;1",
    "FI;00.017.024/0001-53;2023-09-05;1111454.04;33.255017900000;1116180.40;0.00;0.00;1",
]

INF_V2_202610 = [
    "TP_FUNDO_CLASSE;CNPJ_FUNDO_CLASSE;ID_SUBCLASSE;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;"
    "CAPTC_DIA;RESG_DIA;NR_COTST",
    "CLASSES - FIF;00.017.024/0001-53;;2026-10-01;1119426.37;44.990762600000;1221524.87;0.00;2902.97;1",
    "CLASSES - FIF;00.017.024/0001-53;;2026-10-02;1120004.10;45.009590600000;1222036.06;0.00;0.00;1",
    # synthetic: two subclasses of one class on the same day
    "CLASSES - FIF;11.111.111/0001-11;RBMFN1747320951;2026-10-01;100.00;1.000000000000;100.00;0.00;0.00;2",
    "CLASSES - FIF;11.111.111/0001-11;MZMRC1747322915;2026-10-01;200.00;1.100000000000;200.00;0.00;0.00;3",
]

INF_HIST_V0_200501 = [
    "CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST",
    "00.000.432/0001-00;2005-01-03;85628263.94;17.695888000000;85482225.37;0.00;0.00;1",
]

INF_V1_YEAR_2000 = [
    "TP_FUNDO;CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST",
    "FITVM;01.465.738/0001-97;2000-01-03;;2.597231300000;2588000.00;91000.00;0.00;",
    "FITVM;01.465.738/0001-97;2000-02-01;;2.601000000000;2590000.00;0.00;0.00;",  # synthetic
    "FITVM;01.465.738/0001-97;2000-02-02;;2.602000000000;2591000.00;0.00;0.00;",  # synthetic
]


def inf_monthly_zip(ym: str, lines: list[str]) -> bytes:
    return zip_bytes({f"inf_diario_fi_{ym}.csv": csv_bytes(lines)})


# CVM registry (synthetic rows with the real headers; accents exercise Latin-1) -------------

REGISTRO_FUNDO = [
    "ID_Registro_Fundo;CNPJ_Fundo;Codigo_CVM;Data_Registro;Data_Constituicao;Tipo_Fundo;"
    "Denominacao_Social;Data_Cancelamento;Situacao;Data_Inicio_Situacao;Data_Adaptacao_RCVM175;"
    "Data_Inicio_Exercicio_Social;Data_Fim_Exercicio_Social;Patrimonio_Liquido;"
    "Data_Patrimonio_Liquido;Diretor;CNPJ_Administrador;Administrador;Tipo_Pessoa_Gestor;"
    "CPF_CNPJ_Gestor;Gestor",
    "1;00017024000153;100;2000-01-01;2000-01-01;FIF;FUNDO AÇÕES ITAÚ;;Em Funcionamento Normal;"
    "2000-01-01;2025-06-24;2026-01-01;2026-12-31;1221524.87;2026-10-02;;;;;;",
]
REGISTRO_CLASSE = [
    "ID_Registro_Fundo;ID_Registro_Classe;CNPJ_Classe;Codigo_CVM;Data_Registro;Data_Constituicao;"
    "Data_Inicio;Tipo_Classe;Denominacao_Social;Situacao;Data_Inicio_Situacao;Classificacao;"
    "Indicador_Desempenho;Classe_Cotas;Classificacao_Anbima;Tributacao_Longo_Prazo;"
    "Entidade_Investimento;Permitido_Aplicacao_CemPorCento_Exterior;Classe_ESG;Forma_Condominio;"
    "Exclusivo;Publico_Alvo;Patrimonio_Liquido;Data_Patrimonio_Liquido;CNPJ_Auditor;Auditor;"
    "CNPJ_Custodiante;Custodiante;CNPJ_Controlador;Controlador",
    "1;10;00017024000153;100;2025-06-24;2000-01-01;2025-06-24;Classes de Cotas de Fundos FIF;"
    "CLASSE ÚNICA AÇÕES;Em Funcionamento Normal;2025-06-24;Ações;;N;Ações Livre;N;N;N;N;Aberto;N;"
    "Público Geral;1221524.87;2026-10-02;;;;;;",
]
REGISTRO_SUBCLASSE = [
    "ID_Registro_Classe;ID_Subclasse;Codigo_CVM;Data_Constituicao;Data_Inicio;Denominacao_Social;"
    "Situacao;Data_Inicio_Situacao;Forma_Condominio;Exclusivo;Publico_Alvo;Previdenciario;"
    "Exclusivo_INR;Exclusivo_Previdencia_Complementar",
    "10;RBMFN1747320951;;2025-05-15;2025-05-15;SUBCLASSE A;Em Funcionamento Normal;2025-05-15;"
    "Aberto;N;Público Geral;N;N;N",
]
CAD_FI_HIST_CLASSE = [
    "CNPJ_FUNDO;DT_REG;CLASSE;DT_INI_CLASSE;DT_FIM_CLASSE",
    "00.000.432/0001-00;2005-03-31;Multimercado;2005-03-31;",
    "00.000.746/0001-03;2004-12-09;Ações;2004-11-29;",
]


def registro_zip() -> bytes:
    return zip_bytes(
        {
            "registro_classe.csv": csv_bytes(REGISTRO_CLASSE),
            "registro_fundo.csv": csv_bytes(REGISTRO_FUNDO),
            "registro_subclasse.csv": csv_bytes(REGISTRO_SUBCLASSE),
        }
    )


def cad_hist_zip() -> bytes:
    return zip_bytes(
        {
            "cad_fi_hist_admin.csv": csv_bytes(["CNPJ_FUNDO;ADMIN"]),
            "cad_fi_hist_classe.csv": csv_bytes(CAD_FI_HIST_CLASSE),
        }
    )


# BCB PTAX -------------------------------------------------------------------------------------

PTAX_202504_BODY = (
    b'{"@odata.context":"https://was-p.bcnet.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata'
    b'$metadata#_CotacaoDolarPeriodo","value":['
    b'{"cotacaoCompra":5.74900,"cotacaoVenda":5.74960,"dataHoraCotacao":"2025-04-22 13:09:35.629"},'
    b'{"cotacaoCompra":5.68740,"cotacaoVenda":5.68800,"dataHoraCotacao":"2025-04-23 13:06:30.416"},'
    b'{"cotacaoCompra":5.68740,"cotacaoVenda":5.68800,"dataHoraCotacao":"2025-04-23 13:06:30.443"}'
    b"]}"
)


def ptax_body(items: list[dict[str, object]]) -> bytes:
    return json.dumps({"@odata.context": "x", "value": items}).encode()


# Fake HTTP server -----------------------------------------------------------------------------


@dataclass
class Resource:
    body: bytes
    last_modified: str | None = "Thu, 29 Oct 2026 03:50:46 GMT"
    etag: str | None = '"v1"'


@dataclass
class FakeServer:
    resources: dict[str, Resource] = field(default_factory=dict)
    failures: dict[str, list[int | Exception]] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)

    @staticmethod
    def _key(url: str) -> str:
        return str(httpx.URL(url))

    def add(self, url: str, body: bytes, **kwargs: str | None) -> None:
        self.resources[self._key(url)] = Resource(body, **kwargs)

    def fail(self, url: str, *outcomes: int | Exception) -> None:
        self.failures[self._key(url)] = list(outcomes)

    def fail_forever(self, url: str, status: int = 503) -> None:
        self.fail(url, *([status] * 100))

    def handler(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.calls.append((request.method, url))
        queue = self.failures.get(url)
        if queue:
            outcome = queue.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            return httpx.Response(outcome)
        res = self.resources.get(url)
        if res is None:
            return httpx.Response(404)
        headers = {"content-length": str(len(res.body))}
        if res.last_modified:
            headers["last-modified"] = res.last_modified
        if res.etag:
            headers["etag"] = res.etag
        content = b"" if request.method == "HEAD" else res.body
        return httpx.Response(200, headers=headers, content=content)

    def client(self, max_attempts: int = 3) -> HttpClient:
        return HttpClient(
            transport=httpx.MockTransport(self.handler), wait=wait_none(), max_attempts=max_attempts
        )

    def count(self, method: str, url: str) -> int:
        key = self._key(url)
        return sum(1 for m, u in self.calls if m == method and u == key)


# Lake builders --------------------------------------------------------------------------------


def write_bronze_inf(root: Path, ym: str, lines: list[str]) -> None:
    """Bronze inf_diario partition exactly as the bronze layer writes it."""
    zip_path = root.parent / f"src-{ym}.zip"
    zip_path.write_bytes(inf_monthly_zip(ym, lines))
    ex = cvm_inf_diario.extract_month(zip_path, ym, encoding="latin-1", delimiter=";")
    table = bronze.with_source_metadata(
        ex.table,
        {
            "_layout": ex.layout,
            "_source_url": "u",
            "_source_member": ex.member,
            "_source_last_modified": None,
            "_source_sha256": "sha",
        },
    )
    bronze.write_parquet(table, root, bronze.inf_diario_path(ym))


def write_bronze_registry(
    root: Path, table: str, lines: list[str], snapshot: str = "2026-10-29"
) -> None:
    data = read_csv_as_strings(csv_bytes(lines), encoding="latin-1", delimiter=";")
    path = bronze.registry_path(table, date.fromisoformat(snapshot))
    bronze.write_parquet(data, root, path)


def write_bronze_ptax(root: Path, ym: str, body: bytes) -> None:
    rows = bcb_ptax.parse_response(body)
    bronze.write_parquet(bcb_ptax.to_table(rows), root, bronze.ptax_path(ym))


def write_silver_fund_daily(root: Path, ym: str, rows: list[dict[str, object]]) -> None:
    """Silver rows with defaults; values given as str are converted to Decimal."""
    defaults: dict[str, object] = {
        "subclass_id": "",
        "report_type": "CLASSES - FIF",
        "portfolio_value": None,
        "subscriptions": "0",
        "redemptions": "0",
        "shareholders": 1,
        "source_layout": "v2",
        "source_sha256": "sha",
    }
    full = []
    for row in rows:
        r = {**defaults, **row}
        for k in ("portfolio_value", "quota", "equity", "subscriptions", "redemptions"):
            if isinstance(r[k], str):
                r[k] = Decimal(r[k])
        full.append(r)
    table = pa.Table.from_pylist(full, schema=silver.FUND_DAILY_SCHEMA)
    path = root / silver.fund_daily_path(ym)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
