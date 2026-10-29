"""``config/anomalies.yml``: versioned, fixed thresholds."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

import yaml

from cvm_lake.config import ConfigError

DEFAULT_PATH = Path(__file__).resolve().parents[3] / "config" / "anomalies.yml"
LAYERS = frozenset({"l2", "l3", "l4"})


@dataclass(frozen=True)
class AnomalyConfig:
    model_version: str
    history_months: int
    window: int
    min_history: int
    market_day_share: float
    stale_reports: int
    gap_medium_days: int
    shareholder_ratio: float
    shareholder_min_change: int
    max_power: int
    shift_tolerance: float
    z_return: float
    z_residual: float
    min_abs_residual: float
    residual_floor: float
    explained_residual: float
    z_excess: float
    min_peers: int
    market_z: float
    mad_floor: Mapping[str, float]
    budget_per_day: int
    max_age_days: int
    layers: frozenset[str] = frozenset({"l2", "l3", "l4"})
    robust: bool = True
    market_gate: bool = True

    def floor(self, peer_group: str) -> float:
        return self.mad_floor.get(peer_group, self.mad_floor["default"])


def load_anomaly_config(path: Path | None = None) -> AnomalyConfig:
    path = path or DEFAULT_PATH
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        cfg = AnomalyConfig(
            model_version=str(raw["model_version"]),
            history_months=int(raw["history_months"]),
            window=int(raw["window"]),
            min_history=int(raw["min_history"]),
            market_day_share=float(raw["market_day_share"]),
            stale_reports=int(raw["rules"]["stale_reports"]),
            gap_medium_days=int(raw["rules"]["gap_medium_days"]),
            shareholder_ratio=float(raw["rules"]["shareholder_ratio"]),
            shareholder_min_change=int(raw["rules"]["shareholder_min_change"]),
            max_power=int(raw["decimal_shift"]["max_power"]),
            shift_tolerance=float(raw["decimal_shift"]["tolerance"]),
            z_return=float(raw["robust"]["z_return"]),
            z_residual=float(raw["robust"]["z_residual"]),
            min_abs_residual=float(raw["robust"]["min_abs_residual"]),
            residual_floor=float(raw["robust"]["residual_floor"]),
            explained_residual=float(raw["robust"]["explained_residual"]),
            z_excess=float(raw["peers"]["z_excess"]),
            min_peers=int(raw["peers"]["min_peers"]),
            market_z=float(raw["peers"]["market_z"]),
            mad_floor={str(k): float(v) for k, v in raw["mad_floor"].items()},
            budget_per_day=int(raw["alerts"]["budget_per_day"]),
            max_age_days=int(raw["alerts"]["max_age_days"]),
            layers=frozenset(raw["layers"]),
            robust=bool(raw["robust_stats"]),
            market_gate=bool(raw["market_gate"]),
        )
    except (KeyError, TypeError, AttributeError, FileNotFoundError) as exc:
        raise ConfigError(f"invalid anomaly config {path}: {exc}") from exc
    if not cfg.layers <= LAYERS:
        raise ConfigError(
            f"unknown layers {sorted(cfg.layers - LAYERS)}, expected {sorted(LAYERS)}"
        )
    if "default" not in cfg.mad_floor:
        raise ConfigError("mad_floor needs a default")
    if cfg.window < cfg.min_history:
        raise ConfigError("window must be >= min_history")
    return cfg
