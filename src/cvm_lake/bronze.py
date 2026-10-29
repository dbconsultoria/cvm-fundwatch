"""Bronze layer: source data as strings plus source metadata, one Parquet file per partition.

Bronze files are a pure function of the source file (no run id or timestamp inside), so
rebuilding a partition from the same source gives the same data (ADR 0012). Run lineage lives in
the manifest.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from enum import StrEnum
from functools import partial
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cvm_lake.config import Settings
from cvm_lake.http import (
    Download,
    HttpClient,
    NotFoundError,
    RemoteFileInfo,
    SourceUnavailableError,
)
from cvm_lake.manifest import Manifest, ManifestEntry
from cvm_lake.periods import month_bounds
from cvm_lake.sources import bcb_ptax, cvm_inf_diario, cvm_registry

log = logging.getLogger(__name__)

SOURCES = (cvm_inf_diario.SOURCE, cvm_registry.SOURCE, bcb_ptax.SOURCE)


class Status(StrEnum):
    WRITTEN = "written"
    UNCHANGED = "unchanged"  # Last-Modified and ETag match the manifest, nothing downloaded
    SAME_CONTENT = "same_content"  # downloaded, hash matches the manifest, nothing rewritten
    NOT_PUBLISHED = "not_published"  # 404 or empty: the source has no data for it (yet)
    UNAVAILABLE = "unavailable"  # source down after retries: skipped with a warning


@dataclass(frozen=True)
class PartitionResult:
    source: str
    partition: str
    status: Status
    rows: int | None = None
    detail: str = ""


def inf_diario_path(ym: str) -> str:
    return f"bronze/{cvm_inf_diario.SOURCE}/ano_mes={ym}/part-0.parquet"


def registry_path(table: str, snapshot: date) -> str:
    return (
        f"bronze/{cvm_registry.SOURCE}/{table}/snapshot_date={snapshot.isoformat()}/part-0.parquet"
    )


def ptax_path(ym: str) -> str:
    return f"bronze/{bcb_ptax.SOURCE}/ano_mes={ym}/part-0.parquet"


def with_source_metadata(table: pa.Table, meta: Mapping[str, str | None]) -> pa.Table:
    for name, value in meta.items():
        column = pa.repeat(pa.scalar(value, pa.string()), table.num_rows)
        table = table.append_column(name, column)
    return table


def write_parquet(table: pa.Table, root: Path, rel_path: str) -> None:
    dest = root / rel_path
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(dest)


@dataclass
class BronzeRun:
    settings: Settings
    root: Path
    client: HttpClient
    manifest: Manifest
    run_id: str
    today: date
    work_dir: Path
    force: bool = False
    _downloads: dict[str, Download] = field(default_factory=dict)
    _down_sources: set[str] = field(default_factory=set)

    def run(self, months: list[str], sources: set[str]) -> list[PartitionResult]:
        tasks: list[tuple[str, str, Callable[[], list[PartitionResult]]]] = []
        if cvm_inf_diario.SOURCE in sources:
            tasks += [(cvm_inf_diario.SOURCE, ym, partial(self._inf_diario, ym)) for ym in months]
        if cvm_registry.SOURCE in sources:
            archives = dict.fromkeys(t.archive for t in cvm_registry.TABLES)
            tasks += [(cvm_registry.SOURCE, a, partial(self._registry, a)) for a in archives]
        if bcb_ptax.SOURCE in sources:
            tasks += [(bcb_ptax.SOURCE, ym, partial(self._ptax, ym)) for ym in months]

        results: list[PartitionResult] = []
        for source, partition, task in tasks:
            if source in self._down_sources:
                results.append(
                    PartitionResult(
                        source, partition, Status.UNAVAILABLE, detail="skipped, source down"
                    )
                )
                continue
            try:
                results.extend(task())
            except SourceUnavailableError as exc:
                log.warning("%s %s unavailable, skipping the source: %s", source, partition, exc)
                self._down_sources.add(source)
                results.append(
                    PartitionResult(source, partition, Status.UNAVAILABLE, detail=str(exc))
                )
        return results

    # Shared helpers -------------------------------------------------------------------------

    def _download(self, url: str) -> Download:
        if url not in self._downloads:
            name = hashlib.sha1(url.encode()).hexdigest()[:12] + "-" + url.rsplit("/", 1)[-1]
            self._downloads[url] = self.client.download(url, self.work_dir / name)
        return self._downloads[url]

    def _headers_unchanged(self, prev: ManifestEntry | None, info: RemoteFileInfo) -> bool:
        return (
            not self.force
            and prev is not None
            and prev.url == info.url
            and info.last_modified is not None
            and prev.last_modified == info.last_modified
            and prev.etag == info.etag
        )

    def _same_content(self, prev: ManifestEntry | None, url: str, sha256: str) -> bool:
        return not self.force and prev is not None and prev.url == url and prev.sha256 == sha256

    def _mark_checked(
        self, source: str, partition: str, prev: ManifestEntry, dl: Download | None = None
    ) -> None:
        updated = replace(prev, checked_run_id=self.run_id)
        if dl is not None:
            updated = replace(updated, last_modified=dl.last_modified, etag=dl.etag)
        self.manifest.put(source, partition, updated)

    def _write(
        self,
        source: str,
        partition: str,
        table: pa.Table,
        rel_path: str,
        *,
        url: str,
        sha256: str,
        last_modified: str | None = None,
        etag: str | None = None,
    ) -> PartitionResult:
        write_parquet(table, self.root, rel_path)
        self.manifest.put(
            source,
            partition,
            ManifestEntry(
                url=url,
                sha256=sha256,
                row_count=table.num_rows,
                path=rel_path,
                run_id=self.run_id,
                written_at=datetime.now(UTC).isoformat(timespec="seconds"),
                last_modified=last_modified,
                etag=etag,
                checked_run_id=self.run_id,
            ),
        )
        log.info("%s %s written: %d rows -> %s", source, partition, table.num_rows, rel_path)
        return PartitionResult(source, partition, Status.WRITTEN, rows=table.num_rows)

    # Sources --------------------------------------------------------------------------------

    def _inf_diario(self, ym: str) -> list[PartitionResult]:
        source = cvm_inf_diario.SOURCE
        cfg = self.settings.source(source)
        candidates = (
            cvm_inf_diario.monthly_url(cfg["monthly_url"], ym),
            cvm_inf_diario.yearly_url(cfg["yearly_hist_url"], ym),
        )
        info = None
        for url in candidates:
            try:
                info = self.client.head(url)
                break
            except NotFoundError:
                continue
        if info is None:
            return [
                PartitionResult(source, ym, Status.NOT_PUBLISHED, detail="no monthly or HIST file")
            ]

        prev = self.manifest.get(source, ym)
        if self._headers_unchanged(prev, info):
            assert prev is not None
            self._mark_checked(source, ym, prev)
            return [PartitionResult(source, ym, Status.UNCHANGED, rows=prev.row_count)]

        dl = self._download(info.url)
        if self._same_content(prev, dl.url, dl.sha256):
            assert prev is not None
            self._mark_checked(source, ym, prev, dl)
            return [PartitionResult(source, ym, Status.SAME_CONTENT, rows=prev.row_count)]

        extract = cvm_inf_diario.extract_month(
            dl.path, ym, encoding=cfg["encoding"], delimiter=cfg["delimiter"]
        )
        if extract.table.num_rows == 0:
            return [
                PartitionResult(source, ym, Status.NOT_PUBLISHED, detail=f"no rows in {dl.url}")
            ]
        table = with_source_metadata(
            extract.table,
            {
                "_layout": extract.layout,
                "_source_url": dl.url,
                "_source_member": extract.member,
                "_source_last_modified": dl.last_modified,
                "_source_sha256": dl.sha256,
            },
        )
        return [
            self._write(
                source,
                ym,
                table,
                inf_diario_path(ym),
                url=dl.url,
                sha256=dl.sha256,
                last_modified=dl.last_modified,
                etag=dl.etag,
            )
        ]

    def _registry(self, archive: str) -> list[PartitionResult]:
        source = cvm_registry.SOURCE
        cfg = self.settings.source(source)
        url = cfg["archives"][archive]
        specs = [t for t in cvm_registry.TABLES if t.archive == archive]
        try:
            info = self.client.head(url)
        except NotFoundError:
            return [
                PartitionResult(source, s.name, Status.NOT_PUBLISHED, detail=url) for s in specs
            ]

        results = []
        for spec in specs:
            prev = self.manifest.get(source, spec.name)
            if self._headers_unchanged(prev, info):
                assert prev is not None
                self._mark_checked(source, spec.name, prev)
                results.append(PartitionResult(source, spec.name, Status.UNCHANGED, prev.row_count))
                continue
            dl = self._download(url)
            if self._same_content(prev, dl.url, dl.sha256):
                assert prev is not None
                self._mark_checked(source, spec.name, prev, dl)
                results.append(
                    PartitionResult(source, spec.name, Status.SAME_CONTENT, prev.row_count)
                )
                continue
            table = cvm_registry.extract_table(
                dl.path, spec, encoding=cfg["encoding"], delimiter=cfg["delimiter"]
            )
            table = with_source_metadata(
                table,
                {
                    "_source_url": dl.url,
                    "_source_member": spec.member,
                    "_source_last_modified": dl.last_modified,
                    "_source_sha256": dl.sha256,
                },
            )
            results.append(
                self._write(
                    source,
                    spec.name,
                    table,
                    registry_path(spec.name, self.today),
                    url=dl.url,
                    sha256=dl.sha256,
                    last_modified=dl.last_modified,
                    etag=dl.etag,
                )
            )
        return results

    def _ptax(self, ym: str) -> list[PartitionResult]:
        source = bcb_ptax.SOURCE
        start, end = month_bounds(ym)
        if start > self.today:
            return [PartitionResult(source, ym, Status.NOT_PUBLISHED, detail="future month")]
        url = bcb_ptax.period_url(self.settings.source(source)["base_url"], start, end)
        try:
            body = self.client.get_bytes(url)
        except NotFoundError:
            return [PartitionResult(source, ym, Status.NOT_PUBLISHED, detail="HTTP 404")]
        rows = bcb_ptax.parse_response(body)
        if not rows:
            return [PartitionResult(source, ym, Status.NOT_PUBLISHED, detail="empty response")]

        sha256 = bcb_ptax.content_sha256(rows)
        prev = self.manifest.get(source, ym)
        if self._same_content(prev, url, sha256):
            assert prev is not None
            self._mark_checked(source, ym, prev)
            return [PartitionResult(source, ym, Status.SAME_CONTENT, rows=prev.row_count)]
        table = with_source_metadata(
            bcb_ptax.to_table(rows), {"_source_url": url, "_source_sha256": sha256}
        )
        return [self._write(source, ym, table, ptax_path(ym), url=url, sha256=sha256)]
