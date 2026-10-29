"""Structural checks on the GitHub workflows (no network, no GitHub)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
FILES = sorted(WORKFLOWS.glob("*.yml"))


def _load(name: str) -> dict[str, Any]:
    data = yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))
    data["on"] = data.pop(True, data.get("on"))  # YAML 1.1 reads the key `on` as True
    return data


def _steps(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    return [s for job in workflow["jobs"].values() for s in job.get("steps", [])]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_top_level_permissions_are_explicit(path: Path) -> None:
    wf = _load(path.name)
    assert "permissions" in wf
    assert "write-all" not in str(wf["permissions"])


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_actions_are_pinned(path: Path) -> None:
    for step in _steps(_load(path.name)):
        if "uses" in step:
            assert re.search(r"@v\d+(\.\d+\.\d+)?$", step["uses"]), step["uses"]


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_no_untrusted_expressions_in_scripts(path: Path) -> None:
    """User input reaches scripts through env vars only (script injection)."""
    for step in _steps(_load(path.name)):
        script = step.get("run", "")
        assert not re.search(r"\$\{\{\s*(inputs|github\.event)\.", script), step


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.name)
def test_jobs_have_timeouts(path: Path) -> None:
    for name, job in _load(path.name)["jobs"].items():
        if "uses" in job:  # a call to a reusable workflow: its jobs carry the timeouts
            continue
        assert "timeout-minutes" in job, name


def test_etl_design() -> None:
    wf = _load("etl.yml")
    assert wf["permissions"] == {"contents": "read"}
    crons = [s["cron"] for s in wf["on"]["schedule"]]
    assert crons == ["23 17 * * 1-5", "23 17 * * 6"]
    assert wf["on"]["workflow_dispatch"]["inputs"]["dry_run"]["default"] is True

    etl = wf["jobs"]["etl"]
    assert etl["concurrency"] == {"group": "etl-${{ matrix.env }}", "cancel-in-progress": False}
    assert etl["environment"] == "${{ matrix.env }}"
    assert etl["env"]["HF_TOKEN"] == "${{ secrets.HF_TOKEN }}"
    assert wf["jobs"]["approve"]["environment"] == "prod-approval"
    assert "secrets" not in str(wf["jobs"]["approve"])

    plan = wf["jobs"]["plan"]["steps"][0]["run"]
    saturday = next(s for s in crons if s.endswith("6"))
    assert f'"$SCHEDULE" = "{saturday}"' in plan, "plan must recognise the weekly cron"

    assert etl["permissions"] == {"contents": "read", "issues": "write"}
    issue = next(s for s in etl["steps"] if s.get("name") == "Open or update the alerts issue")
    assert "matrix.env == 'prod'" in issue["if"]
    run_step = next(s for s in etl["steps"] if s.get("name") == "Run pipeline")
    assert "--alerts-out alerts.md" in run_step["run"]

    upload = next(s for s in etl["steps"] if "upload-artifact" in s.get("uses", ""))
    assert upload["with"]["retention-days"] == 7
    assert "DRY_RUN == 'true'" in upload["if"]


def test_train_design() -> None:
    wf = _load("train.yml")
    assert wf["permissions"] == {"contents": "read"}
    assert [s["cron"] for s in wf["on"]["schedule"]] == ["17 6 * * 0"]
    for env, branch in (("dev", "development"), ("prod", "main")):
        job = wf["jobs"][env]
        assert job["uses"] == "./.github/workflows/train-env.yml"
        assert job["with"]["env"] == env and job["with"]["branch"] == branch

    called = _load("train-env.yml")
    jobs = called["jobs"]
    assert jobs["train"]["environment"] == "${{ inputs.env }}"
    assert jobs["train"]["env"]["HF_MODEL_TOKEN"] == "${{ secrets.HF_MODEL_TOKEN }}"
    assert jobs["approve"]["environment"] == "prod-approval"
    assert "inputs.env == 'prod'" in jobs["approve"]["if"]
    assert "needs.train.outputs.promote == 'true'" in jobs["publish"]["if"]
    assert jobs["publish"]["needs"] == ["train", "approve"]
    assert "secrets" not in str(jobs["approve"])


def test_keepalive_covers_scheduled_workflows() -> None:
    script = _load("keepalive.yml")["jobs"]["keepalive"]["steps"][0]["run"]
    for name in ("etl.yml", "train.yml"):
        assert name in script


def test_pages_design() -> None:
    wf = _load("pages.yml")
    assert wf["on"]["workflow_run"] == {
        "workflows": ["etl"],
        "types": ["completed"],
        "branches": ["main"],
    }
    assert wf["permissions"] == {"contents": "read"}
    build, deploy = wf["jobs"]["build"], wf["jobs"]["deploy"]
    assert build["environment"] == "prod"
    assert "cancelled" in build["if"]
    assert deploy["permissions"] == {"pages": "write", "id-token": "write"}
    assert deploy["environment"]["name"] == "github-pages"
    uses = [s.get("uses", "") for s in build["steps"]]
    assert "actions/upload-pages-artifact@v5" in uses


def test_ci_builds_the_dashboard() -> None:
    steps = _load("ci.yml")["jobs"]["dashboard"]["steps"]
    script = " ".join(s.get("run", "") for s in steps)
    assert "demo_data.py" in script and "npm run build" in script
