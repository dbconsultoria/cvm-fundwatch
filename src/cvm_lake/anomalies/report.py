"""Alerts: new high severity anomalies, within the daily budget, as GitHub Issue Markdown."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

from cvm_lake.anomalies.detector import anomalies_path
from cvm_lake.anomalies.settings import AnomalyConfig
from cvm_lake.db import lit


@dataclass(frozen=True)
class Alert:
    dt: date
    fund_id: str
    class_id: str
    anomaly_type: str
    score: float
    peer_group: str
    features: str


def format_cnpj(value: str) -> str:
    if len(value) == 14:
        return f"{value[:2]}.{value[2:5]}.{value[5:8]}/{value[8:12]}-{value[12:]}"
    return value


def select_alerts(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    months: list[str],
    run_id: str,
    today: date,
    cfg: AnomalyConfig,
) -> list[Alert]:
    """High severity rows first seen in this run, recent dates only, top ``budget`` per day."""
    paths = [root / anomalies_path(ym) for ym in months if (root / anomalies_path(ym)).exists()]
    if not paths:
        return []
    files = "[" + ", ".join(lit(p) for p in paths) + "]"
    oldest = today - timedelta(days=cfg.max_age_days)
    rows = con.execute(
        f"""
        SELECT dt, fund_id, class_id, anomaly_type, score, peer_group, features
        FROM read_parquet({files})
        WHERE severity = 'high' AND first_seen_run = ? AND dt >= ?
        QUALIFY row_number() OVER (PARTITION BY dt ORDER BY score DESC, fund_id, class_id)
                <= {cfg.budget_per_day}
        ORDER BY dt DESC, score DESC, fund_id, class_id
        """,
        [run_id, oldest],
    ).fetchall()
    return [Alert(*r) for r in rows]


def render_alerts(env_name: str, run_id: str, alerts: list[Alert], repo_id: str | None) -> str:
    lines = [
        f"### Run `{run_id}`: {len(alerts)} new high severity anomalies ({env_name})",
        "",
        "| Date | CNPJ | Subclass | Peer group | Type | Score |",
        "|---|---|---|---|---|---:|",
    ]
    for a in alerts:
        lines.append(
            f"| {a.dt} | {format_cnpj(a.fund_id)} | {a.class_id or '-'} | {a.peer_group} "
            f"| {a.anomaly_type} | {a.score:.1f} |"
        )
    if repo_id:
        lines += [
            "",
            f"Details (features JSON) in `gold/anomalies` of "
            f"[{repo_id}](https://huggingface.co/datasets/{repo_id}).",
        ]
    return "\n".join(lines) + "\n"
