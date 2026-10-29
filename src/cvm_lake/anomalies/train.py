"""Train the Isolation Forest and decide champion vs challenger (ADR 0033).

Months, for an evaluation month E (default: the last complete month):

- training: the ``train_months`` months before E (unsupervised, clean data)
- validation: E-1, the last training month, with injected errors (``threshold_seed``): the
  threshold is the best F1 on those errors, then raised to respect the clean alarm budget
- evaluation: E, held out, with other injected errors (``eval_seed``): the challenger and the
  champion are compared on average precision (threshold free) on the same errors

The challenger is promoted when its clean alarm rate on the evaluation month stays within the
budget times ``eval_alarm_tolerance``, and there is no champion or its average precision is
higher than the champion's.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np

from cvm_lake import gold
from cvm_lake.anomalies.detector import build_features
from cvm_lake.anomalies.evaluate import copy_inputs
from cvm_lake.anomalies.injection import Injection, apply_injections, plan_injections
from cvm_lake.anomalies.model import (
    FEATURES,
    ForestModel,
    Labeled,
    best_f1_threshold,
    budget_threshold,
    feature_rows,
    fit,
    library_versions,
)
from cvm_lake.anomalies.settings import DEFAULT_PATH, AnomalyConfig
from cvm_lake.db import connect
from cvm_lake.periods import month_bounds, shift_month

log = logging.getLogger(__name__)

DECISION_FILE, METRICS_FILE, CARD_FILE = "decision.json", "metrics.json", "README.md"


def months_for(eval_month: str, train_months: int, history_months: int) -> list[str]:
    """Every silver month training and evaluation read, oldest first."""
    first = shift_month(eval_month, -(train_months + history_months))
    return [shift_month(first, i) for i in range(train_months + history_months + 2)]


def _month_features(con: duckdb.DuckDBPyConnection, root: Path, ym: str, cfg: AnomalyConfig):
    build_features(con, root, ym, cfg)
    start, end = month_bounds(ym)
    con.execute(
        "CREATE OR REPLACE TEMP TABLE month_flags AS SELECT * FROM flags "
        f"WHERE dt BETWEEN DATE '{start}' AND DATE '{end}'"
    )
    keys, matrix = feature_rows(con, "month_flags")
    con.execute("DROP TABLE month_flags; DROP TABLE flags; DROP TABLE features; DROP TABLE base")
    return keys, matrix


def _labeled(
    keys: list[tuple[str, str, str]], scores: np.ndarray, plan: list[Injection]
) -> Labeled:
    echo = {(i.cnpj, i.subclass_id, i.echo_dt.isoformat()) for i in plan if i.echo_dt}
    spans: dict[tuple[str, str], list[Injection]] = {}
    for i in plan:
        spans.setdefault((i.cnpj, i.subclass_id), []).append(i)

    def injected(key: tuple[str, str, str]) -> bool:
        return any(
            i.dt.isoformat() <= key[2] <= i.until.isoformat() for i in spans.get(key[:2], [])
        )

    keep = [n for n, k in enumerate(keys) if k not in echo]
    labels = np.array([1 if injected(keys[n]) else 0 for n in keep], dtype=np.int8)
    return Labeled(scores=scores[keep], labels=labels)


@dataclass
class MonthData:
    """Features of one month, clean and with injected errors."""

    clean_scores_keys: list[tuple[str, str, str]]
    clean_matrix: np.ndarray
    injected_keys: list[tuple[str, str, str]]
    injected_matrix: np.ndarray
    plan: list[Injection]

    def labeled(self, model: ForestModel) -> Labeled:
        return _labeled(self.injected_keys, model.score(self.injected_matrix), self.plan)

    def clean_scores(self, model: ForestModel) -> np.ndarray:
        return model.score(self.clean_matrix)


def _month_data(lake: Path, ym: str, cfg: AnomalyConfig, seed: int, work: Path) -> MonthData:
    assert cfg.forest is not None
    clean, injected = work / f"{ym}-clean", work / f"{ym}-injected"
    copy_inputs(lake, clean, ym, cfg)
    copy_inputs(lake, injected, ym, cfg)
    con = connect()
    plan = plan_injections(
        con, injected, ym, cfg.forest.injections, seed, cfg.min_history, cfg.history_months
    )
    apply_injections(con, injected, ym, plan)
    gold.load_registry(con, clean)
    clean_keys, clean_matrix = _month_features(con, clean, ym, cfg)
    con.close()
    con = connect()
    gold.load_registry(con, injected)
    inj_keys, inj_matrix = _month_features(con, injected, ym, cfg)
    con.close()
    return MonthData(clean_keys, clean_matrix, inj_keys, inj_matrix, plan)


def _summary(model: ForestModel, data: MonthData, budget: float) -> dict[str, Any]:
    labeled = data.labeled(model)
    clean = data.clean_scores(model)
    alarms = 10_000 * float((clean >= model.threshold).sum()) / max(len(clean), 1)
    return {
        "average_precision": round(labeled.average_precision(), 4),
        "threshold": model.threshold,
        **{
            k: round(v, 4) if isinstance(v, float) else v
            for k, v in labeled.at(model.threshold).items()
        },
        "clean_alarms_per_10k": round(alarms, 2),
        "within_budget": alarms <= budget,
    }


@dataclass
class TrainResult:
    challenger: ForestModel
    promote: bool
    reason: str
    metrics: dict[str, Any]


def train_and_compare(
    lake: Path,
    eval_month: str,
    cfg: AnomalyConfig,
    champion: ForestModel | None,
    work: Path,
    config_path: Path | None = None,
) -> TrainResult:
    forest_cfg = cfg.forest
    if forest_cfg is None:
        raise ValueError("config has no isolation_forest section")
    train = [shift_month(eval_month, -i) for i in range(forest_cfg.train_months, 0, -1)]
    validation = train[-1]

    matrices = []
    for ym in train:
        con = connect()
        gold.load_registry(con, lake)
        _, matrix = _month_features(con, lake, ym, cfg)
        con.close()
        log.info("training features %s: %d reports", ym, len(matrix))
        matrices.append(matrix)
    estimator = fit(np.vstack(matrices), forest_cfg)
    challenger = ForestModel(estimator=estimator, threshold=float("inf"))

    val = _month_data(lake, validation, cfg, forest_cfg.threshold_seed, work / "validation")
    threshold = best_f1_threshold(val.labeled(challenger))
    threshold = budget_threshold(
        val.clean_scores(challenger), threshold, forest_cfg.max_clean_alarms_per_10k
    )
    challenger.threshold = threshold

    ev = _month_data(lake, eval_month, cfg, forest_cfg.eval_seed, work / "evaluation")
    # the threshold was set to the budget on the validation month; the held out month gets a
    # tolerance, otherwise ordinary month to month variation would reject half the models
    budget = forest_cfg.max_clean_alarms_per_10k * forest_cfg.eval_alarm_tolerance
    metrics: dict[str, Any] = {
        "eval_month": eval_month,
        "validation_month": validation,
        "train_months": train,
        "eval_alarm_limit_per_10k": budget,
        "challenger": _summary(challenger, ev, budget),
        "champion": _summary(champion, ev, budget) if champion else None,
        "champion_tag": champion.tag if champion else None,
    }
    ch = metrics["challenger"]
    if not ch["within_budget"]:
        promote, reason = False, "challenger exceeds the clean alarm budget"
    elif champion is None:
        promote, reason = True, "no champion yet"
    elif ch["average_precision"] > metrics["champion"]["average_precision"]:
        promote, reason = True, "higher average precision than the champion"
    else:
        promote, reason = False, "champion has equal or higher average precision"

    config_file = config_path or DEFAULT_PATH
    challenger.meta = {
        "tag": f"if-{eval_month}-s{forest_cfg.seed}",
        "model_version": cfg.model_version,
        "eval_month": eval_month,
        "validation_month": validation,
        "train_months": train,
        "train_reports": int(sum(len(m) for m in matrices)),
        "seed": forest_cfg.seed,
        "n_estimators": forest_cfg.n_estimators,
        "max_samples": forest_cfg.max_samples,
        "anomaly_config_sha256": hashlib.sha256(config_file.read_bytes()).hexdigest(),
        "libraries": library_versions(),
    }
    metrics["decision"] = {"promote": promote, "reason": reason}
    return TrainResult(challenger=challenger, promote=promote, reason=reason, metrics=metrics)


def write_outputs(result: TrainResult, out: Path, env_name: str) -> None:
    result.challenger.save(out)
    (out / METRICS_FILE).write_text(json.dumps(result.metrics, indent=2) + "\n", encoding="utf-8")
    (out / DECISION_FILE).write_text(
        json.dumps({"promote": result.promote, "reason": result.reason}) + "\n", encoding="utf-8"
    )
    (out / CARD_FILE).write_text(render_model_card(result, env_name), encoding="utf-8")


def render_model_card(result: TrainResult, env_name: str) -> str:
    m, meta = result.metrics, result.challenger.meta
    ch, champ = m["challenger"], m["champion"]
    rows = [("challenger (this model)", ch)] + ([("previous champion", champ)] if champ else [])
    table = "\n".join(
        f"| {name} | {x['average_precision']:.3f} | {x['recall']:.3f} | {x['precision']:.3f} "
        f"| {x['clean_alarms_per_10k']:.1f} |"
        for name, x in rows
    )
    return f"""---
library_name: sklearn
tags:
- anomaly-detection
- isolation-forest
- tabular
- finance
---

# cvm-fundwatch Isolation Forest ({env_name})

Layer 5 of the cvm-fundwatch anomaly detector: an Isolation Forest over per report features of
the CVM daily fund reports. Its findings are low severity and only where the rule and
statistical layers found nothing.

- Tag: `{meta["tag"]}`
- Trained on: {", ".join(meta["train_months"])} ({meta["train_reports"]:,} reports, sampled)
- Threshold: {result.challenger.threshold:.6f}: best F1 on errors injected into
  {meta["validation_month"]}, raised if needed to respect the clean alarm budget
- Evaluated on: {meta["eval_month"]} (held out) with other injected errors
- Features: {", ".join(FEATURES)}
- Seed {meta["seed"]}, {meta["n_estimators"]} trees, max_samples {meta["max_samples"]}
- Libraries: {", ".join(f"{k} {v}" for k, v in meta["libraries"].items())}

## Evaluation on {meta["eval_month"]}

| Model | Average precision | Recall at threshold | Precision at threshold | Clean alarms per 10k |
|---|---:|---:|---:|---:|
{table}

Decision: **{"promoted" if result.promote else "not promoted"}** ({result.reason}).

Precision is computed against injected errors only, so it is a lower bound: real anomalies in
the data count as false positives.

## Use

The model is stored with [skops](https://skops.readthedocs.io/) (no pickle). Load it with
`skops.io.load` after checking `skops.io.get_untrusted_types`; `model.json` holds the threshold
and the feature list. Scores are `-score_samples(X)`, higher is more anomalous.
"""
