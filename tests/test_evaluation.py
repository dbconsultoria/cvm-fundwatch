"""Injection harness on a synthetic panel. These recall floors run in CI: a change that makes
the detector miss injected errors fails the build (ADR 0031)."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from cvm_lake import gold, silver
from cvm_lake.anomalies.detector import anomalies_path, detect_month
from cvm_lake.anomalies.evaluate import (
    load_experiment,
    render_markdown,
    run_experiment,
    save_results,
    score,
    variant_config,
)
from cvm_lake.anomalies.injection import KINDS, apply_injections, plan_injections
from cvm_lake.anomalies.settings import load_anomaly_config
from cvm_lake.cli import main
from cvm_lake.config import ConfigError
from cvm_lake.db import connect

from fakes import Panel

YM = "202609"
CFG = replace(load_anomaly_config(), window=20, min_history=10, history_months=1)
COUNTS = dict.fromkeys(KINDS, 8)  # 56 injections, one fund each
SEED = 11

# Minimum recall at the given severity or above, on the synthetic panel.
FLOORS = {
    "decimal_shift": ("high", 0.95),
    "negative_equity": ("high", 0.95),
    "missing_quota": ("medium", 0.95),
    "quota_glitch": ("medium", 0.9),
    "equity_jump": ("medium", 0.9),
    "stale_quota": ("medium", 0.9),
    "shareholder_spike": ("medium", 0.9),
}


def _injected_run(root: Path, panel: Panel, cfg=CFG, seed: int = SEED):
    panel.write(root)
    con = connect()
    plan = plan_injections(con, root, YM, COUNTS, seed, cfg.min_history, cfg.history_months)
    apply_injections(con, root, YM, plan)
    gold.load_registry(con, root)
    detect_month(con, root, YM, cfg, "eval")
    return plan, score(con, root / anomalies_path(YM), plan)


def test_plan_is_deterministic_and_uses_distinct_funds(tmp_path: Path) -> None:
    panel = Panel(n=80)
    panel.write(tmp_path / "a")
    con = connect()
    a = plan_injections(con, tmp_path / "a", YM, COUNTS, SEED, 10, 1)
    b = plan_injections(con, tmp_path / "a", YM, COUNTS, SEED, 10, 1)
    c = plan_injections(con, tmp_path / "a", YM, COUNTS, SEED + 1, 10, 1)
    assert a == b and a != c
    assert len({(i.cnpj, i.subclass_id) for i in a}) == len(a) == 56
    assert all(i.echo_dt is not None and i.echo_dt > i.until for i in a)
    stale = [i for i in a if i.kind == "stale_quota"]
    assert all(i.until > i.dt for i in stale)


def test_too_many_injections_for_the_panel_fails(tmp_path: Path) -> None:
    Panel(n=30).write(tmp_path)
    with pytest.raises(ValueError, match="not enough eligible funds"):
        plan_injections(connect(), tmp_path, YM, COUNTS, SEED, 10, 1)


def test_recall_floors_on_injected_errors(tmp_path: Path) -> None:
    _, result = _injected_run(tmp_path, Panel(n=80))
    metrics = result.as_dict()
    for kind, (level, floor) in FLOORS.items():
        got = metrics["kinds"][kind][f"recall_{level}"]
        assert got >= floor, f"{kind}: recall_{level} {got} < {floor}"
    # the clean panel has no anomalies, so every non echo flag is an injected error
    assert metrics["precision_lb_medium"] == 1.0


def test_decimal_shift_recall_on_a_crash_day(tmp_path: Path) -> None:
    """Layers 2 and 4 are silent on a market move; the rules must still catch the errors."""
    panel = Panel(n=80)
    for i in range(80):
        panel.scale(i, "2026-09-15", 0.92)
    _, result = _injected_run(tmp_path, panel)
    assert result.as_dict()["kinds"]["decimal_shift"]["recall_high"] >= 0.95


def test_rules_only_baseline_misses_statistical_errors(tmp_path: Path) -> None:
    cfg = variant_config(CFG, {"layers": []})
    _, result = _injected_run(tmp_path, Panel(n=80), cfg)
    kinds = result.as_dict()["kinds"]
    assert kinds["decimal_shift"]["recall_high"] >= 0.95
    assert kinds["quota_glitch"]["recall_medium"] < 0.5


def test_variant_config_overrides() -> None:
    v = variant_config(CFG, {"layers": ["l2"], "robust_stats": False, "market_gate": False})
    assert (v.layers, v.robust, v.market_gate) == (frozenset({"l2"}), False, False)
    with pytest.raises(ConfigError, match="unknown variant settings"):
        variant_config(CFG, {"windw": 5})


def test_run_experiment_end_to_end(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    Panel(n=80).write(lake)
    exp_file = tmp_path / "exp.yml"
    exp_file.write_text(
        "name: tiny\nseed: 3\n"
        "injections_per_month: {decimal_shift: 5, quota_glitch: 5}\n"
        "variants:\n  rules_only: {layers: []}\n  v1_full: {}\n",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "anomalies.yml"
    text = Path("config/anomalies.yml").read_text(encoding="utf-8")
    text = text.replace("history_months: 3", "history_months: 1").replace(
        "window: 63", "window: 20"
    )
    cfg_file.write_text(text.replace("min_history: 30", "min_history: 10"), encoding="utf-8")

    results = run_experiment(lake, [YM], load_experiment(exp_file), tmp_path / "work", cfg_file)
    month = results["months"][0]
    assert month["reports"] == 80 * 22
    assert set(month["variants"]) == {"rules_only", "v1_full"}
    assert month["variants"]["v1_full"]["kinds"]["quota_glitch"]["recall_medium"] == 1.0
    assert month["variants"]["rules_only"]["clean_flags_per_10k"]["medium"] == 0.0
    assert len(results["reproducibility"]["anomaly_config_sha256"]) == 64
    assert not (lake / anomalies_path(YM)).exists(), "the real lake is never modified"
    assert (lake / silver.fund_daily_path(YM)).exists()

    json_path, md_path = save_results(results, tmp_path / "out")
    assert json.loads(json_path.read_text())["experiment"] == "tiny"
    assert "| v1_full |" in md_path.read_text() and "| v1_full |" in render_markdown(results)


def test_cli_evaluate_saves_one_file_per_month(tmp_path: Path) -> None:
    lake = tmp_path / "lake"
    Panel(n=80).write(lake)
    exp_file = tmp_path / "exp.yml"
    exp_file.write_text(
        "name: cli\nseed: 1\ninjections_per_month: {decimal_shift: 4}\nvariants:\n  v1_full: {}\n",
        encoding="utf-8",
    )
    cfg_file = tmp_path / "anomalies.yml"
    text = Path("config/anomalies.yml").read_text(encoding="utf-8")
    text = text.replace("history_months: 3", "history_months: 1").replace(
        "window: 63", "window: 20"
    )
    cfg_file.write_text(text.replace("min_history: 30", "min_history: 10"), encoding="utf-8")
    out = tmp_path / "out"
    code = main(
        ["evaluate", "--lake", str(lake), "--months", YM, "--experiment", str(exp_file),
         "--out", str(out), "--anomaly-config", str(cfg_file)]
    )  # fmt: skip
    assert code == 0
    assert sorted(p.name for p in out.iterdir()) == [f"cli_{YM}.json", f"cli_{YM}.md"]
