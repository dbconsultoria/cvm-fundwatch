"""Full runs (bronze -> silver -> gold -> quality) against the fake server."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from cvm_lake import gold, silver
from cvm_lake.bronze import SOURCES
from cvm_lake.cli import main
from cvm_lake.config import load_settings
from cvm_lake.manifest import Manifest
from cvm_lake.periods import month_bounds
from cvm_lake.pipeline import SILVER_FUND_DAILY, RunReport, format_report, run_pipeline
from cvm_lake.sources.bcb_ptax import period_url

from fakes import (
    CAD_HIST_URL,
    INF_MONTHLY,
    INF_V1_202309,
    INF_V2_202610,
    PTAX_BASE,
    REGISTRO_URL,
    FakeServer,
    cad_hist_zip,
    inf_monthly_zip,
    ptax_body,
    registro_zip,
)

TODAY = date(2026, 10, 29)


@pytest.fixture
def server() -> FakeServer:
    s = FakeServer()
    s.add(INF_MONTHLY.format("202309"), inf_monthly_zip("202309", INF_V1_202309))
    s.add(INF_MONTHLY.format("202610"), inf_monthly_zip("202610", INF_V2_202610))
    s.add(REGISTRO_URL, registro_zip())
    s.add(CAD_HIST_URL, cad_hist_zip())
    item = {
        "cotacaoCompra": 5.0,
        "cotacaoVenda": 5.0006,
        "dataHoraCotacao": "2026-10-01 13:05:00.1",
    }
    s.add(period_url(PTAX_BASE, *month_bounds("202610")), ptax_body([item]))
    return s


def _run(
    root: Path, server: FakeServer, months: list[str], run_id: str
) -> tuple[RunReport, Manifest]:
    manifest = Manifest.load(root / "_manifest.json")
    work = root.parent / f"work-{run_id}"
    work.mkdir(parents=True)
    with server.client() as client:
        report = run_pipeline(
            settings=load_settings("dev"),
            root=root,
            manifest=manifest,
            run_id=run_id,
            today=TODAY,
            months=months,
            sources=set(SOURCES),
            client=client,
            work_dir=work,
        )
    manifest.save(root / "_manifest.json")
    return report, manifest


def test_full_run_builds_every_layer(tmp_path: Path, server: FakeServer) -> None:
    root = tmp_path / "lake"
    report, manifest = _run(root, server, ["202309", "202610"], "r1")
    assert not report.failed, format_report(report)
    for rel in [
        silver.fund_daily_path("202309"),
        silver.fund_daily_path("202610"),
        silver.FUND_REGISTRY_PATH,
        silver.ptax_daily_path("202610"),
        gold.daily_flows_path("202610"),
        gold.returns_path("202610"),
    ]:
        assert (root / rel).exists(), rel
    entry = manifest.get(SILVER_FUND_DAILY, "202610")
    assert entry is not None and entry.row_count == 4
    assert "QUALITY" in format_report(report)


def test_shrunk_partition_fails_and_keeps_previous_counts(
    tmp_path: Path, server: FakeServer
) -> None:
    root = tmp_path / "lake"
    _run(root, server, ["202610"], "r1")
    server.add(
        INF_MONTHLY.format("202610"),
        inf_monthly_zip("202610", INF_V2_202610[:2]),  # 4 rows -> 1 row
        last_modified="Fri, 30 Oct 2026 03:50:00 GMT",
    )
    report, manifest = _run(root, server, ["202610"], "r2")
    assert report.failed
    failed = [c for c in report.checks if c.blocking]
    assert [c.name for c in failed] == ["row_count_drop"]
    entry = manifest.get(SILVER_FUND_DAILY, "202610")
    assert entry is not None and (entry.row_count, entry.run_id) == (4, "r1")


def test_cli_rebuilds_from_local_bronze_without_network(tmp_path: Path, server: FakeServer) -> None:
    root = tmp_path / "lake"
    _run(root, server, ["202610"], "r1")
    (root / gold.returns_path("202610")).unlink()
    code = main(["run", "--env", "dev", "--out", str(root), "--months", "202610", "--no-extract"])
    assert code == 0
    assert (root / gold.returns_path("202610")).exists()


def test_cli_hf_target_stops_at_the_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, server: FakeServer
) -> None:
    monkeypatch.delenv("HF_OWNER", raising=False)
    monkeypatch.setenv("HF_TOKEN", "hf_x")
    root = tmp_path / "lake"
    code = main(["run", "--env", "prod", "--target", "hf", "--out", str(root), "--dry-run"])
    assert code == 2
    assert not root.exists(), "nothing may run before the guard passes"
    assert server.calls == []


def test_cli_writes_step_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, server: FakeServer
) -> None:
    root = tmp_path / "lake"
    _run(root, server, ["202610"], "r1")
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert (
        main(["run", "--env", "dev", "--out", str(root), "--months", "202610", "--no-extract"]) == 0
    )
    text = summary.read_text(encoding="utf-8")
    assert "## cvm-fundwatch ETL: `dev`" in text and "| 202610 | v2 | 4 |" in text
