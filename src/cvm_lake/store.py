"""Remote lake storage. ``HfStore`` is the Hugging Face dataset repo; tests use a local fake
with the same interface."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import RepositoryNotFoundError

from cvm_lake.guard import GuardError

log = logging.getLogger(__name__)

EXPECTED_PRIVATE = {"dev": True, "prod": False}


@dataclass(frozen=True)
class HeadCommit:
    sha: str
    title: str


class LakeStore(Protocol):
    repo_id: str

    def verify(self, env_name: str) -> None: ...

    def head(self) -> HeadCommit | None: ...

    def download(self, paths: list[str], dest: Path, revision: str | None) -> None: ...

    def commit(
        self, root: Path, paths: list[str], message: str, parent: str | None
    ) -> str | None: ...


class HfStore:
    """A dataset repo on the Hub. Every call is authenticated, reads included."""

    repo_type = "dataset"

    def __init__(self, repo_id: str, token: str, api: HfApi | None = None) -> None:
        self.repo_id = repo_id
        self._api = api if api is not None else HfApi(token=token)

    def verify(self, env_name: str) -> None:
        try:
            info = self._api.repo_info(self.repo_id, repo_type=self.repo_type)
        except RepositoryNotFoundError as exc:
            raise GuardError(
                f"{self.repo_type} repo {self.repo_id} not found or token has no access"
            ) from exc
        expected = EXPECTED_PRIVATE[env_name]
        if bool(info.private) != expected:
            want = "private" if expected else "public"
            raise GuardError(f"{env_name} repo {self.repo_id} must be {want}")

    def head(self) -> HeadCommit | None:
        commits = self._api.list_repo_commits(self.repo_id, repo_type=self.repo_type)
        if not commits:
            return None
        return HeadCommit(sha=commits[0].commit_id, title=commits[0].title)

    def download(self, paths: list[str], dest: Path, revision: str | None) -> None:
        """Download exactly ``paths`` (those that exist) at ``revision`` into ``dest``."""
        if not paths:
            return
        self._api.snapshot_download(
            self.repo_id,
            repo_type=self.repo_type,
            revision=revision,
            local_dir=dest,
            allow_patterns=paths,
        )

    def commit(self, root: Path, paths: list[str], message: str, parent: str | None) -> str | None:
        """One atomic commit. ``parent`` makes it fail if someone else committed meanwhile."""
        operations = [CommitOperationAdd(path_in_repo=p, path_or_fileobj=root / p) for p in paths]
        info = self._api.create_commit(
            self.repo_id,
            operations,
            commit_message=message,
            repo_type=self.repo_type,
            parent_commit=parent,
        )
        sha = getattr(info, "oid", None)
        log.info("committed %d files to %s: %s", len(paths), self.repo_id, sha)
        return sha


class HfModelStore(HfStore):
    """The model repo of the Isolation Forest: same rules (visibility, pinned commits)."""

    repo_type = "model"

    def download_model(self, dest: Path) -> bool:
        """Download the current model into ``dest``. False when the repo has no model yet."""
        head = self.head()
        if head is None:
            return False
        self.download(["model.skops", "model.json"], dest, head.sha)
        return (dest / "model.skops").exists() and (dest / "model.json").exists()
