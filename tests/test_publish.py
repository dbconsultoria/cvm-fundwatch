"""Hugging Face target against a fake repo. No network."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
import yaml

from cvm_lake import gold, silver
from cvm_lake.bronze import SOURCES, Status
from cvm_lake.config import PublishConfig, Settings, load_settings
from cvm_lake.manifest import MANIFEST_NAME, Manifest
from cvm_lake.periods import month_bounds
from cvm_lake.publish import (
    CARD_NAME,
    ChunkResult,
    PublishError,
    chunk_months,
    input_paths,
    run_on_store,
)
from cvm_lake.sources.bcb_ptax import period_url
from cvm_lake.summary import annotations, render_markdown

from fakes import (
    CAD_HIST_URL,
    INF_MONTHLY,
    INF_V1_202309,
    INF_V2_202610,
    PTAX_BASE,
    REGISTRO_URL,
    FakeServer,
    FakeStore,
    StaleParentError,
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


@pytest.fixture
def store(tmp_path: Path) -> FakeStore:
    remote = tmp_path / "remote"
    remote.mkdir()
    return FakeStore(remote=remote)


def _run(
    tmp_path: Path,
    server: FakeServer,
    store: FakeStore,
    months: list[str],
    run_id: str,
    *,
    dry_run: bool = False,
    settings: Settings | None = None,
) -> list[ChunkResult]:
    """Each run starts from an empty local folder, like a fresh GitHub runner."""
    root = tmp_path / f"runner-{run_id}"
    work = tmp_path / f"work-{run_id}"
    work.mkdir()
    with server.client() as client:
        return run_on_store(
            settings=settings or load_settings("dev"),
            store=store,
            root=root,
            run_id=run_id,
            today=TODAY,
            months=months,
            sources=set(SOURCES),
            client=client,
            work_dir=work,
            dry_run=dry_run,
        )


def test_first_run_publishes_one_commit(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    (chunk,) = _run(tmp_path, server, store, ["202610"], "r1")
    assert chunk.commit_sha == "c1"
    sha, title, files = store.commits[-1]
    assert (sha, title) == ("c1", "run r1")
    assert files == chunk.files
    expected = {
        MANIFEST_NAME,
        CARD_NAME,
        "bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet",
        "bronze/bcb_ptax/ano_mes=202610/part-0.parquet",
        "bronze/cvm_registry/registro_classe/snapshot_date=2026-10-29/part-0.parquet",
        silver.fund_daily_path("202610"),
        silver.ptax_daily_path("202610"),
        silver.FUND_REGISTRY_PATH,
        gold.daily_flows_path("202610"),
        gold.returns_path("202610"),
    }
    assert expected <= set(files)
    assert all((store.remote / f).exists() for f in files)


def test_second_run_reuses_remote_state(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    _run(tmp_path, server, store, ["202610"], "r1")
    (chunk,) = _run(tmp_path, server, store, ["202610"], "r2")

    statuses = {(b.source, b.partition): b.status for b in chunk.report.bronze}
    assert statuses[("cvm_inf_diario", "202610")] is Status.UNCHANGED
    assert server.count("GET", INF_MONTHLY.format("202610")) == 1, "bronze came from the repo"
    assert chunk.report.fund_daily[0].rows == 4, "silver rebuilt from the downloaded bronze"
    assert "bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet" not in chunk.files

    remote = Manifest.load(store.remote / MANIFEST_NAME)
    entry = remote.get("cvm_inf_diario", "202610")
    assert entry is not None
    assert (entry.run_id, entry.commit_sha, entry.checked_run_id) == ("r1", "c1", "r2")


def test_downloads_only_what_the_window_needs(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    _run(tmp_path, server, store, ["202309", "202610"], "r1")
    store.downloads.clear()
    _run(tmp_path, server, store, ["202610"], "r2")
    assert store.downloads[0] == [MANIFEST_NAME]
    wanted = store.downloads[1]
    assert "bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet" in wanted
    assert silver.fund_daily_path("202609") in wanted  # gold of 202610 needs M-1
    assert not any("202309" in p for p in wanted)


def test_dry_run_publishes_nothing(tmp_path: Path, server: FakeServer, store: FakeStore) -> None:
    (chunk,) = _run(tmp_path, server, store, ["202610"], "r1", dry_run=True)
    assert len(store.commits) == 1
    assert chunk.dry_run and chunk.commit_sha is None and chunk.files
    assert (tmp_path / "runner-r1" / gold.returns_path("202610")).exists()
    assert "Dry run" in render_markdown("dev", store.repo_id, [chunk])


def test_quality_failure_publishes_nothing(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    _run(tmp_path, server, store, ["202610"], "r1")
    server.add(
        INF_MONTHLY.format("202610"),
        inf_monthly_zip("202610", INF_V2_202610[:2]),
        last_modified="Fri, 30 Oct 2026 03:50:00 GMT",
    )
    (chunk,) = _run(tmp_path, server, store, ["202610"], "r2")
    assert chunk.report.failed
    assert [c[0] for c in store.commits] == ["c0", "c1"]
    md = render_markdown("dev", store.repo_id, [chunk])
    assert "Nothing was published" in md and "row_count_drop" in md
    assert any(
        a.startswith("::error title=Quality row_count_drop") for a in annotations(chunk.report)
    )


def test_backfill_is_split_into_atomic_chunks(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    settings = replace(load_settings("dev"), publish=PublishConfig(1, 95))
    chunks = _run(tmp_path, server, store, ["202309", "202610"], "r1", settings=settings)
    assert [c.report.months for c in chunks] == [["202309"], ["202610"]]
    assert [c[1] for c in store.commits[1:]] == ["run r1-c1", "run r1-c2"]
    assert chunk_months(["a", "b", "c"], 2) == [["a", "b"], ["c"]]


def test_too_many_files_fails_before_commit(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    settings = replace(load_settings("dev"), publish=PublishConfig(12, 3))
    with pytest.raises(PublishError, match="limit 3"):
        _run(tmp_path, server, store, ["202610"], "r1", settings=settings)
    assert len(store.commits) == 1


def test_concurrent_writer_makes_the_commit_fail(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    original_download = store.download

    def download_then_someone_commits(paths: list[str], dest: Path, revision: str | None) -> None:
        original_download(paths, dest, revision)
        if paths != [MANIFEST_NAME]:
            store.commits.append(("x1", "someone else", []))

    store.download = download_then_someone_commits  # type: ignore[method-assign]
    with pytest.raises(StaleParentError):
        _run(tmp_path, server, store, ["202610"], "r1")


def test_dataset_card_declares_configs(
    tmp_path: Path, server: FakeServer, store: FakeStore
) -> None:
    _run(tmp_path, server, store, ["202610"], "r1")
    text = (store.remote / CARD_NAME).read_text(encoding="utf-8")
    front = yaml.safe_load(text.split("---")[1])
    names = [c["config_name"] for c in front["configs"]]
    assert names[:2] == ["daily_flows", "returns_brl_usd"]
    assert front["configs"][0]["default"] is True
    assert "owner/cvm-fundos-lake-dev" in text


def test_input_paths_from_manifest(tmp_path: Path, server: FakeServer, store: FakeStore) -> None:
    _run(tmp_path, server, store, ["202610"], "r1")
    manifest = Manifest.load(store.remote / MANIFEST_NAME)
    paths = input_paths(manifest, ["202610"])
    assert "bronze/cvm_registry/cad_fi_hist_classe/snapshot_date=2026-10-29/part-0.parquet" in paths
    assert json.loads((store.remote / MANIFEST_NAME).read_text())["schema_version"] == 1
