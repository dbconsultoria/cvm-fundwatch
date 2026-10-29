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

    upload = next(s for s in etl["steps"] if "upload-artifact" in s.get("uses", ""))
    assert upload["with"]["retention-days"] == 7
    assert "DRY_RUN == 'true'" in upload["if"]
