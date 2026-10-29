"""Pipeline state: one entry per source partition, stored as ``_manifest.json`` in the lake."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path

MANIFEST_NAME = "_manifest.json"
SCHEMA_VERSION = 1


@dataclass
class ManifestEntry:
    url: str
    sha256: str
    row_count: int
    path: str  # lake relative path of the file last written for this entry
    run_id: str  # run that last wrote the file
    written_at: str  # ISO 8601 UTC
    last_modified: str | None = None
    etag: str | None = None
    checked_run_id: str | None = None  # last run that looked at the source
    commit_sha: str | None = None  # HF commit holding the file, set by publish (Phase 3)


class Manifest:
    def __init__(self, entries: dict[str, ManifestEntry] | None = None) -> None:
        self.entries: dict[str, ManifestEntry] = entries or {}

    @staticmethod
    def key(source: str, partition: str) -> str:
        return f"{source}/{partition}"

    def get(self, source: str, partition: str) -> ManifestEntry | None:
        return self.entries.get(self.key(source, partition))

    def put(self, source: str, partition: str, entry: ManifestEntry) -> None:
        self.entries[self.key(source, partition)] = entry

    def stamp_commit(self, run_id: str, sha: str) -> int:
        """Record ``sha`` on the entries written by ``run_id`` (ADR 0024).

        A commit cannot contain its own sha, so the next run fills it in from the repo head.
        Returns the number of entries stamped.
        """
        stamped = 0
        for entry in self.entries.values():
            if entry.run_id == run_id and entry.commit_sha is None:
                entry.commit_sha = sha
                stamped += 1
        return stamped

    @classmethod
    def load(cls, path: Path) -> Manifest:
        if not path.exists():
            return cls()
        raw = json.loads(path.read_text(encoding="utf-8"))
        if raw.get("schema_version") != SCHEMA_VERSION:
            raise ValueError(f"unsupported manifest schema_version {raw.get('schema_version')!r}")
        known = {f.name for f in fields(ManifestEntry)}
        return cls(
            {
                key: ManifestEntry(**{k: v for k, v in value.items() if k in known})
                for key, value in raw["entries"].items()
            }
        )

    def to_json(self) -> str:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "entries": {k: asdict(self.entries[k]) for k in sorted(self.entries)},
        }
        return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(self.to_json(), encoding="utf-8")
        tmp.replace(path)
