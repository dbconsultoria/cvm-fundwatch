"""Hugging Face target: pull the state and inputs, run the pipeline, push one commit per chunk.

For each chunk of months (ADR 0023):

1. Read the repo head. Every download and the commit are pinned to it (``parent_commit``), so a
   concurrent writer makes the commit fail instead of being overwritten.
2. Download ``_manifest.json`` and only the partitions the run needs: bronze of the window
   (to rebuild silver when the source did not change), silver of the month before the window
   (for gold), and the latest registry snapshot.
3. Run bronze, silver, gold and quality locally.
4. If quality passed, commit the files written by the run plus the manifest and dataset card.
   A dry run stops before the commit; the workflow keeps the local lake as an artifact.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from cvm_lake import gold, silver
from cvm_lake.bronze import Status
from cvm_lake.card import render_card
from cvm_lake.config import Settings
from cvm_lake.http import HttpClient
from cvm_lake.manifest import MANIFEST_NAME, Manifest
from cvm_lake.periods import shift_month
from cvm_lake.pipeline import RunReport, run_pipeline
from cvm_lake.sources import bcb_ptax, cvm_inf_diario, cvm_registry
from cvm_lake.store import LakeStore

log = logging.getLogger(__name__)

CARD_NAME = "README.md"


class PublishError(RuntimeError):
    pass


@dataclass
class ChunkResult:
    report: RunReport
    files: list[str] = field(default_factory=list)
    commit_sha: str | None = None
    dry_run: bool = False


def chunk_months(months: list[str], size: int) -> list[list[str]]:
    return [months[i : i + size] for i in range(0, len(months), size)]


def input_paths(manifest: Manifest, months: list[str]) -> list[str]:
    """Remote files a run over ``months`` needs locally, from the manifest."""
    paths: set[str] = set()
    for ym in months:
        for source in (cvm_inf_diario.SOURCE, bcb_ptax.SOURCE):
            entry = manifest.get(source, ym)
            if entry:
                paths.add(entry.path)
    before = shift_month(months[0], -1)
    paths.add(silver.fund_daily_path(before))
    paths.add(silver.ptax_daily_path(before))
    for table in cvm_registry.TABLES:
        entry = manifest.get(cvm_registry.SOURCE, table.name)
        if entry:
            paths.add(entry.path)
    return sorted(paths)


def output_paths(report: RunReport, manifest: Manifest) -> list[str]:
    """Files written by this run, plus the manifest and the dataset card."""
    paths = {MANIFEST_NAME, CARD_NAME}
    for r in report.bronze:
        if r.status is Status.WRITTEN:
            entry = manifest.get(r.source, r.partition)
            if entry:
                paths.add(entry.path)
    paths.update(silver.fund_daily_path(s.month) for s in report.fund_daily)
    paths.update(silver.ptax_daily_path(p.month) for p in report.ptax)
    if report.registry is not None:
        paths.add(silver.FUND_REGISTRY_PATH)
    for g in report.gold:
        paths.update((gold.daily_flows_path(g.month), gold.returns_path(g.month)))
    return sorted(paths)


def run_on_store(
    *,
    settings: Settings,
    store: LakeStore,
    root: Path,
    run_id: str,
    today: date,
    months: list[str],
    sources: set[str],
    client: HttpClient | None,
    work_dir: Path,
    dry_run: bool,
    force: bool = False,
) -> list[ChunkResult]:
    results: list[ChunkResult] = []
    chunks = chunk_months(months, settings.publish.max_months_per_commit)
    for i, chunk in enumerate(chunks, start=1):
        chunk_run_id = run_id if len(chunks) == 1 else f"{run_id}-c{i}"
        head = store.head()
        revision = head.sha if head else None
        log.info("chunk %d/%d %s from %s @ %s", i, len(chunks), chunk, store.repo_id, revision)

        manifest_path = root / MANIFEST_NAME
        manifest_path.unlink(missing_ok=True)  # the remote manifest is the only state
        store.download([MANIFEST_NAME], root, revision)
        manifest = Manifest.load(manifest_path)
        if head:
            manifest.stamp_commit(head.title.removeprefix("run "), head.sha)
        store.download(input_paths(manifest, chunk), root, revision)

        report = run_pipeline(
            settings=settings,
            root=root,
            manifest=manifest,
            run_id=chunk_run_id,
            today=today,
            months=chunk,
            sources=sources,
            client=client,
            work_dir=work_dir,
            force=force,
        )
        manifest.save(manifest_path)
        (root / CARD_NAME).write_text(
            render_card(settings.environment.name, store.repo_id), encoding="utf-8"
        )
        result = ChunkResult(report=report, dry_run=dry_run)
        results.append(result)
        if report.failed:
            log.error("quality checks failed, nothing is published")
            break

        result.files = output_paths(report, manifest)
        limit = settings.publish.max_files_per_commit
        if len(result.files) > limit:
            raise PublishError(f"{len(result.files)} files in one commit, limit {limit}")
        if dry_run:
            log.info("dry run: would commit %d files to %s", len(result.files), store.repo_id)
            continue
        result.commit_sha = store.commit(root, result.files, f"run {chunk_run_id}", revision)
    return results
