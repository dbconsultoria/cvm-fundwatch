from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from huggingface_hub.errors import RepositoryNotFoundError

from cvm_lake.config import load_settings
from cvm_lake.guard import GuardError, resolve_hf_target
from cvm_lake.store import HfStore

DEV = load_settings("dev").environment
PROD = load_settings("prod").environment
BASE = {"HF_OWNER": "someone", "HF_TOKEN": "hf_x"}
GH = {**BASE, "GITHUB_ACTIONS": "true"}


def test_local_dev_target() -> None:
    target = resolve_hf_target(DEV, BASE)
    assert (target.repo_id, target.token) == ("someone/cvm-fundos-lake-dev", "hf_x")


@pytest.mark.parametrize(
    ("env", "environ", "message"),
    [
        (DEV, {"HF_TOKEN": "x"}, "HF_OWNER"),
        (PROD, {"HF_OWNER": "o", "HF_TOKEN": " "}, "HF_TOKEN is not set for environment 'prod'"),
        (PROD, {**GH, "CVM_LAKE_ENV": "dev", "GITHUB_REF": "refs/heads/main"}, "declares"),
        (PROD, {**GH, "GITHUB_REF": "refs/heads/main"}, "declares CVM_LAKE_ENV=''"),
        (
            PROD,
            {**GH, "CVM_LAKE_ENV": "prod", "GITHUB_REF": "refs/heads/development"},
            "only from main",
        ),
    ],
)
def test_guard_rejects(env: object, environ: dict[str, str], message: str) -> None:
    with pytest.raises(GuardError, match=message):
        resolve_hf_target(env, environ)  # type: ignore[arg-type]


def test_guard_accepts_prod_on_main_in_actions() -> None:
    environ = {**GH, "CVM_LAKE_ENV": "prod", "GITHUB_REF": "refs/heads/main"}
    assert resolve_hf_target(PROD, environ).repo_id == "someone/cvm-fundos-lake"


def test_guard_rejects_repo_name_that_does_not_match_env() -> None:
    swapped = replace(PROD, hf_dataset_repo="{owner}/cvm-fundos-lake-dev")
    with pytest.raises(GuardError, match="does not match"):
        resolve_hf_target(swapped, BASE)


def test_store_verify_checks_visibility() -> None:
    api = MagicMock()
    api.repo_info.return_value = SimpleNamespace(private=False)
    store = HfStore("o/cvm-fundos-lake-dev", "t", api=api)
    with pytest.raises(GuardError, match="must be private"):
        store.verify("dev")
    HfStore("o/cvm-fundos-lake", "t", api=api).verify("prod")
    api.repo_info.assert_called_with("o/cvm-fundos-lake", repo_type="dataset")


def test_store_verify_missing_repo() -> None:
    api = MagicMock()
    api.repo_info.side_effect = RepositoryNotFoundError("nope", response=MagicMock())
    with pytest.raises(GuardError, match="not found"):
        HfStore("o/r-dev", "t", api=api).verify("dev")


def test_store_head_download_and_commit(tmp_path: Path) -> None:
    api = MagicMock()
    api.list_repo_commits.return_value = [SimpleNamespace(commit_id="abc", title="run r1")]
    api.create_commit.return_value = SimpleNamespace(oid="def")
    store = HfStore("o/r-dev", "t", api=api)

    head = store.head()
    assert head is not None and (head.sha, head.title) == ("abc", "run r1")

    store.download(["_manifest.json"], tmp_path, "abc")
    api.snapshot_download.assert_called_once_with(
        "o/r-dev",
        repo_type="dataset",
        revision="abc",
        local_dir=tmp_path,
        allow_patterns=["_manifest.json"],
    )

    (tmp_path / "a.txt").write_text("x")
    assert store.commit(tmp_path, ["a.txt"], "run r2", "abc") == "def"
    args, kwargs = api.create_commit.call_args
    assert args[0] == "o/r-dev"
    assert [op.path_in_repo for op in args[1]] == ["a.txt"]
    assert kwargs == {"commit_message": "run r2", "repo_type": "dataset", "parent_commit": "abc"}


def test_store_download_nothing_is_a_no_op() -> None:
    api = MagicMock()
    HfStore("o/r-dev", "t", api=api).download([], Path(), None)
    api.snapshot_download.assert_not_called()
