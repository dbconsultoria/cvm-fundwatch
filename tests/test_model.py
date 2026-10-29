"""Isolation Forest layer: training smoke test on fixtures, champion vs challenger, safe
persistence and use in the detector (ADR 0033)."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import pytest

from cvm_lake import gold, modelops
from cvm_lake.anomalies.detector import anomalies_path, detect_month
from cvm_lake.anomalies.model import (
    FEATURES,
    META_FILE,
    ForestModel,
    Labeled,
    best_f1_threshold,
    budget_threshold,
)
from cvm_lake.anomalies.settings import load_anomaly_config
from cvm_lake.anomalies.train import DECISION_FILE, train_and_compare, write_outputs
from cvm_lake.cli import main
from cvm_lake.config import load_settings
from cvm_lake.db import connect

from fakes import Panel

EVAL = "202609"
BASE = load_anomaly_config()
assert BASE.forest is not None
CFG = replace(
    BASE,
    window=20,
    min_history=10,
    history_months=1,
    forest=replace(
        BASE.forest,
        train_months=3,
        n_estimators=50,
        max_train_rows=5_000,
        max_clean_alarms_per_10k=100,
        injections={"quota_glitch": 6, "equity_jump": 6, "stale_quota": 4, "shareholder_spike": 4},
    ),
)


@pytest.fixture(scope="module")
def lake(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("lake")
    Panel(n=80, start=date(2026, 5, 4), end=date(2026, 9, 30)).write(root)
    return root


@pytest.fixture(scope="module")
def trained(lake: Path, tmp_path_factory: pytest.TempPathFactory):
    return train_and_compare(lake, EVAL, CFG, None, tmp_path_factory.mktemp("work"))


def test_training_without_champion_promotes(trained) -> None:
    m = trained.metrics
    assert trained.promote and trained.reason == "no champion yet"
    assert m["train_months"] == ["202606", "202607", "202608"]
    assert m["validation_month"] == "202608"
    assert np.isfinite(trained.challenger.threshold)
    # injected errors rank far above normal reports on held out data
    assert m["challenger"]["average_precision"] > 0.8
    assert m["challenger"]["within_budget"]
    assert trained.challenger.meta["tag"] == f"if-{EVAL}-s{CFG.forest.seed}"


def test_equal_champion_is_kept(lake: Path, trained, tmp_path: Path) -> None:
    again = train_and_compare(lake, EVAL, CFG, trained.challenger, tmp_path)
    assert not again.promote
    assert again.reason == "champion has equal or higher average precision"
    assert (
        again.metrics["challenger"]["average_precision"]
        == again.metrics["champion"]["average_precision"]
    ), "same seed and data must give the same model"


def test_save_load_roundtrip(trained, tmp_path: Path) -> None:
    write_outputs(trained, tmp_path, "dev")
    loaded = ForestModel.load(tmp_path)
    x = np.random.default_rng(0).random((20, len(FEATURES)))
    assert np.array_equal(loaded.score(x), trained.challenger.score(x))
    assert loaded.threshold == trained.challenger.threshold
    assert json.loads((tmp_path / DECISION_FILE).read_text())["promote"] is True
    card = (tmp_path / "README.md").read_text(encoding="utf-8")
    assert "library_name: sklearn" in card and "promoted" in card


def test_load_refuses_unexpected_types(trained, tmp_path: Path, monkeypatch) -> None:
    write_outputs(trained, tmp_path, "dev")
    monkeypatch.setattr(
        "cvm_lake.anomalies.model.skops_io.get_untrusted_types",
        lambda file: ["os.system"],
    )
    with pytest.raises(ValueError, match="unexpected types"):
        ForestModel.load(tmp_path)


def test_load_refuses_other_features(trained, tmp_path: Path) -> None:
    write_outputs(trained, tmp_path, "dev")
    meta = json.loads((tmp_path / META_FILE).read_text())
    meta["features"] = ["something_else"]
    (tmp_path / META_FILE).write_text(json.dumps(meta))
    with pytest.raises(ValueError, match="other features"):
        ForestModel.load(tmp_path)


def test_forest_rows_only_where_no_other_layer(trained, tmp_path: Path) -> None:
    panel = Panel(n=80, start=date(2026, 5, 4), end=date(2026, 9, 30))
    panel.scale(0, "2026-09-10", 100, ("quota",), until="2026-09-10")  # a layer 1 finding
    panel.write(tmp_path)
    everything = replace(trained.challenger, threshold=float("-inf"))
    con = connect()
    gold.load_registry(con, tmp_path)
    stats = detect_month(con, tmp_path, EVAL, CFG, "r1", everything)
    rows = pq.read_table(tmp_path / anomalies_path(EVAL)).to_pylist()
    forest_rows = [r for r in rows if r["anomaly_type"] == "isolation_forest"]
    others = {(r["fund_id"], r["dt"]) for r in rows if r["anomaly_type"] != "isolation_forest"}
    assert forest_rows and all(r["severity"] == "low" for r in forest_rows)
    assert not others & {(r["fund_id"], r["dt"]) for r in forest_rows}
    assert all(r["model_version"].endswith("+" + trained.challenger.tag) for r in forest_rows)
    assert all(
        r["model_version"] == CFG.model_version
        for r in rows
        if r["anomaly_type"] != "isolation_forest"
    )
    assert stats.by_type["isolation_forest"] == len(forest_rows)
    assert '"forest_score"' in forest_rows[0]["features"]


def test_best_f1_and_budget_thresholds() -> None:
    data = Labeled(scores=np.array([0.1, 0.2, 0.3, 0.9, 0.95]), labels=np.array([0, 0, 0, 1, 1]))
    assert best_f1_threshold(data) == pytest.approx(0.9)
    assert data.at(0.9) == {"recall": 1.0, "precision": 1.0, "flagged": 2}
    clean = np.linspace(0, 1, 10_000)
    raised = budget_threshold(clean, 0.5, per_10k=10)
    assert int((clean >= raised).sum()) <= 10
    assert budget_threshold(clean, 0.9995, per_10k=10) == 0.9995  # already within budget


def test_load_forest_for_runs(trained, tmp_path: Path) -> None:
    env = load_settings("dev").environment
    assert modelops.load_forest(env, {}, None, tmp_path) is None  # no token: layer off
    write_outputs(trained, tmp_path / "m", "dev")
    model = modelops.load_forest(env, {}, tmp_path / "m", tmp_path)
    assert model is not None and model.tag == trained.challenger.tag
    assert modelops.load_forest(env, {}, tmp_path / "missing", tmp_path) is None


def test_cli_train_local_and_publish_guard(lake: Path, tmp_path: Path, monkeypatch) -> None:
    cfg_file = tmp_path / "anomalies.yml"
    text = Path("config/anomalies.yml").read_text(encoding="utf-8")
    for old, new in [("history_months: 3", "history_months: 1"), ("window: 63", "window: 20"),
                     ("min_history: 30", "min_history: 10"), ("train_months: 6", "train_months: 3"),
                     ("n_estimators: 200", "n_estimators: 20"),
                     ("quota_glitch: 300", "quota_glitch: 4"),
                     ("equity_jump: 300", "equity_jump: 4"),
                     ("stale_quota: 150", "stale_quota: 2"),
                     ("shareholder_spike: 150", "shareholder_spike: 2")]:  # fmt: skip
        assert old in text
        text = text.replace(old, new)
    cfg_file.write_text(text, encoding="utf-8")
    out = tmp_path / "out"
    code = main(
        ["train", "--env", "dev", "--lake", str(lake), "--eval-month", EVAL,
         "--out", str(out), "--anomaly-config", str(cfg_file)]
    )  # fmt: skip
    assert code == 0
    assert {p.name for p in out.iterdir()} >= {"model.skops", "model.json", "metrics.json"}

    # publishing a model the decision rejected is refused before any network access
    (out / DECISION_FILE).write_text('{"promote": false, "reason": "test"}')
    monkeypatch.delenv("HF_MODEL_TOKEN", raising=False)
    assert main(["publish-model", "--env", "dev", "--dir", str(out)]) == 1
    (out / DECISION_FILE).write_text('{"promote": true, "reason": "test"}')
    assert main(["publish-model", "--env", "dev", "--dir", str(out)]) == 2  # guard: no token
