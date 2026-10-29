"""Environment guard (ADR 0022). Runs before any work: a misconfigured run must not touch data.

Local checks here; the remote checks (repo exists, visibility matches) are in
``HfStore.verify``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from cvm_lake.config import EnvironmentConfig


class GuardError(RuntimeError):
    """The run is not allowed to proceed."""


@dataclass(frozen=True)
class HfTarget:
    repo_id: str
    token: str


def resolve_hf_target(
    env: EnvironmentConfig,
    environ: Mapping[str, str],
    *,
    repo: str = "dataset",
    token_var: str = "HF_TOKEN",
) -> HfTarget:
    """``repo``: ``dataset`` (the lake) or ``model`` (the Isolation Forest)."""
    owner = environ.get("HF_OWNER", "").strip()
    if not owner:
        raise GuardError("HF_OWNER is not set")
    token = environ.get(token_var, "").strip()
    if not token:
        raise GuardError(f"{token_var} is not set for environment {env.name!r}")
    template = env.hf_dataset_repo if repo == "dataset" else env.hf_model_repo
    repo_id = template.format(owner=owner)

    if (env.name == "dev") != repo_id.endswith("-dev"):
        raise GuardError(f"repo {repo_id!r} does not match environment {env.name!r}")

    if environ.get("GITHUB_ACTIONS") == "true":
        # The GitHub Environment declares which lake it is for; the job must agree.
        declared = environ.get("CVM_LAKE_ENV", "")
        if declared != env.name:
            raise GuardError(
                f"GitHub Environment declares CVM_LAKE_ENV={declared!r}, run asked for {env.name!r}"
            )
        if env.name == "prod" and environ.get("GITHUB_REF") != "refs/heads/main":
            raise GuardError(f"prod runs only from main, not {environ.get('GITHUB_REF')!r}")

    return HfTarget(repo_id=repo_id, token=token)
