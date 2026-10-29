"""BCB PTAX via the Olinda OData API (``CotacaoDolarPeriodo``). Dates go as ``MM-DD-YYYY``."""

from __future__ import annotations

import hashlib
import json
from datetime import date

import pyarrow as pa

from cvm_lake.errors import SchemaError

SOURCE = "bcb_ptax"
FIELDS = ("cotacaoCompra", "cotacaoVenda", "dataHoraCotacao")


def period_url(base_url: str, start: date, end: date) -> str:
    return (
        f"{base_url.rstrip('/')}/CotacaoDolarPeriodo("
        "dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)"
        f"?@dataInicial='{start:%m-%d-%Y}'&@dataFinalCotacao='{end:%m-%d-%Y}'&$format=json"
    )


def parse_response(body: bytes) -> list[dict[str, str]]:
    """Rows with every value as the exact text the API sent (no float rounding)."""
    try:
        payload = json.loads(body, parse_float=str, parse_int=str)
        values = payload["value"]
    except (ValueError, KeyError, TypeError) as exc:
        raise SchemaError(f"unexpected PTAX response: {body[:200]!r}") from exc
    rows = []
    for item in values:
        missing = [f for f in FIELDS if f not in item]
        if missing:
            raise SchemaError(f"PTAX item missing {missing}: {item}")
        rows.append({f: str(item[f]) for f in FIELDS})
    return rows


def content_sha256(rows: list[dict[str, str]]) -> str:
    """Hash of the data only. The raw body also carries an ``@odata.context`` host name."""
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def to_table(rows: list[dict[str, str]]) -> pa.Table:
    return pa.table({f: pa.array([r[f] for r in rows], pa.string()) for f in FIELDS})
