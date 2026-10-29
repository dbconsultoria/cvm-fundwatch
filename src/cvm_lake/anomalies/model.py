"""Layer 5: Isolation Forest over the per report features (ADR 0033).

Features (one row per report with enough history), all non negative and log scaled so a few
extreme values do not stretch the random split ranges of the trees:

- ``abs_z_return``: |robust z of the log return| (layer 2), 0 on market moves
- ``abs_z_excess``: |robust z of the excess return over peers| (layer 4), 0 on market moves
- ``abs_residual``: |accounting identity residual| (layer 3), in percent
- ``flow_ratio``: (subscriptions + redemptions) / previous equity, in percent
- ``shareholder_change``: |ln((shareholders + 1) / (previous shareholders + 1))|
- ``unchanged_reports``: reports since the quota last changed

The threshold is not ``contamination``: it is chosen on injected errors (best F1), then raised
if needed to respect an alarm budget on clean data.

Models are stored with ``skops`` (no pickle). Loading accepts only sklearn and numpy types.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import sklearn
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score, precision_recall_curve
from skops import io as skops_io

from cvm_lake.anomalies.settings import ForestConfig

FEATURES = (
    "abs_z_return",
    "abs_z_excess",
    "abs_residual",
    "flow_ratio",
    "shareholder_change",
    "unchanged_reports",
)
FEATURE_SQL = """
    -- zero on market moves: like layers 2 and 4, the forest must not flag the market
    CASE WHEN market_move THEN 0 ELSE ln(1 + coalesce(abs(z_return), 0)) END AS abs_z_return,
    CASE WHEN market_move THEN 0 ELSE ln(1 + coalesce(abs(z_excess), 0)) END AS abs_z_excess,
    ln(1 + 100 * coalesce(abs(residual), 0)) AS abs_residual,
    ln(1 + 100 * coalesce(abs((coalesce(subscriptions, 0) + coalesce(redemptions, 0))
                              / nullif(prev_equity, 0)), 0)) AS flow_ratio,
    -- +1 on both sides: a drop to zero shareholders must not give an infinite value
    coalesce(abs(ln((shareholders + 1) / (prev_shareholders + 1))), 0) AS shareholder_change,
    ln(1 + coalesce(unchanged_reports, 0)) AS unchanged_reports
"""
MODEL_FILE, META_FILE = "model.skops", "model.json"
TRUSTED_PREFIXES = ("sklearn.", "numpy.", "builtins.")

Key = tuple[str, str, str]


def feature_rows(con: duckdb.DuckDBPyConnection, table: str) -> tuple[list[Key], np.ndarray]:
    """Keys and feature matrix of the reports in ``table`` (a flags table) with history."""
    rows = con.execute(f"""
        SELECT cnpj, subclass_id, CAST(dt AS VARCHAR), {FEATURE_SQL}
        FROM {table}
        WHERE has_history
        ORDER BY cnpj, subclass_id, dt
    """).fetchall()
    keys = [(r[0], r[1], r[2]) for r in rows]
    matrix = np.array([r[3:] for r in rows], dtype=np.float64).reshape(len(rows), len(FEATURES))
    return keys, matrix


@dataclass
class ForestModel:
    estimator: IsolationForest
    threshold: float
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def tag(self) -> str:
        return str(self.meta.get("tag", "if-unknown"))

    def score(self, matrix: np.ndarray) -> np.ndarray:
        """Higher is more anomalous."""
        if len(matrix) == 0:
            return np.zeros(0)
        return -self.estimator.score_samples(matrix)

    def save(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        skops_io.dump(self.estimator, directory / MODEL_FILE)
        meta = {**self.meta, "threshold": self.threshold, "features": list(FEATURES)}
        (directory / META_FILE).write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    @classmethod
    def load(cls, directory: Path) -> ForestModel:
        path = directory / MODEL_FILE
        untrusted = skops_io.get_untrusted_types(file=path)
        refused = [t for t in untrusted if not t.startswith(TRUSTED_PREFIXES)]
        if refused:
            raise ValueError(f"refusing to load {path}: unexpected types {refused}")
        estimator = skops_io.load(path, trusted=untrusted)
        if not isinstance(estimator, IsolationForest):
            raise ValueError(f"{path} does not hold an IsolationForest")
        meta = json.loads((directory / META_FILE).read_text(encoding="utf-8"))
        if tuple(meta.get("features", ())) != FEATURES:
            raise ValueError(f"{path} was trained on other features: {meta.get('features')}")
        return cls(estimator=estimator, threshold=float(meta["threshold"]), meta=meta)


def fit(matrix: np.ndarray, cfg: ForestConfig) -> IsolationForest:
    rng = np.random.default_rng(cfg.seed)
    if len(matrix) > cfg.max_train_rows:
        matrix = matrix[rng.choice(len(matrix), cfg.max_train_rows, replace=False)]
    forest = IsolationForest(
        n_estimators=cfg.n_estimators,
        max_samples=min(cfg.max_samples, len(matrix)),
        random_state=cfg.seed,
        n_jobs=1,  # deterministic
    )
    return forest.fit(matrix)


@dataclass(frozen=True)
class Labeled:
    """Scores of one month with injected errors; echoes already removed."""

    scores: np.ndarray
    labels: np.ndarray  # 1 = injected error

    def average_precision(self) -> float:
        if self.labels.sum() == 0:
            return 0.0
        return float(average_precision_score(self.labels, self.scores))

    def at(self, threshold: float) -> dict[str, float]:
        flagged = self.scores >= threshold
        tp = int((flagged & (self.labels == 1)).sum())
        return {
            "recall": tp / max(int(self.labels.sum()), 1),
            "precision": tp / max(int(flagged.sum()), 1),
            "flagged": int(flagged.sum()),
        }


def best_f1_threshold(data: Labeled) -> float:
    precision, recall, thresholds = precision_recall_curve(data.labels, data.scores)
    f1 = 2 * precision[:-1] * recall[:-1] / np.maximum(precision[:-1] + recall[:-1], 1e-12)
    return float(thresholds[int(np.argmax(f1))])


def budget_threshold(clean_scores: np.ndarray, threshold: float, per_10k: float) -> float:
    """Raise ``threshold`` until at most ``per_10k`` clean reports in 10,000 pass it."""
    if len(clean_scores) == 0:
        return threshold
    allowed = int(len(clean_scores) * per_10k / 10_000)
    if int((clean_scores >= threshold).sum()) <= allowed:
        return threshold
    ranked = np.sort(clean_scores)[::-1]
    return float(np.nextafter(ranked[allowed], np.inf)) if allowed < len(ranked) else threshold


def library_versions() -> dict[str, str]:
    return {"scikit-learn": sklearn.__version__, "numpy": np.__version__}
