"""Configuration loading. Secrets never come from here, only from the environment."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "pipeline.yml"
ENVIRONMENTS = ("dev", "prod")


class ConfigError(ValueError):
    """The configuration file is missing or inconsistent."""


@dataclass(frozen=True)
class EnvironmentConfig:
    name: str
    hf_dataset_repo: str
    hf_model_repo: str
    branch: str


@dataclass(frozen=True)
class PublishConfig:
    max_months_per_commit: int
    max_files_per_commit: int


@dataclass(frozen=True)
class QualityConfig:
    max_row_drop_pct: float
    max_invalid_value_share: float
    min_peer_coverage: float


@dataclass(frozen=True)
class Settings:
    environment: EnvironmentConfig
    lookback_months: int
    sources: Mapping[str, Mapping[str, Any]]
    http_timeout_seconds: float
    http_max_attempts: int
    peer_groups: Mapping[str, str]
    quality: QualityConfig
    publish: PublishConfig

    def source(self, name: str) -> Mapping[str, Any]:
        try:
            return self.sources[name]
        except KeyError as exc:
            raise ConfigError(f"source {name!r} is not configured") from exc


def load_settings(env: str, path: Path | None = None) -> Settings:
    if env not in ENVIRONMENTS:
        raise ConfigError(f"unknown environment {env!r}, expected one of {ENVIRONMENTS}")
    path = path or DEFAULT_CONFIG_PATH
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc

    try:
        env_raw = raw["environments"][env]
        settings = Settings(
            environment=EnvironmentConfig(
                name=env,
                hf_dataset_repo=env_raw["hf_dataset_repo"],
                hf_model_repo=env_raw["hf_model_repo"],
                branch=env_raw["branch"],
            ),
            lookback_months=int(raw["window"]["lookback_months"]),
            sources=raw["sources"],
            http_timeout_seconds=float(raw["http"]["timeout_seconds"]),
            http_max_attempts=int(raw["http"]["max_attempts"]),
            peer_groups={str(k): str(v) for k, v in raw["peer_groups"].items()},
            quality=QualityConfig(
                max_row_drop_pct=float(raw["quality"]["max_row_drop_pct"]),
                max_invalid_value_share=float(raw["quality"]["max_invalid_value_share"]),
                min_peer_coverage=float(raw["quality"]["min_peer_coverage"]),
            ),
            publish=PublishConfig(
                max_months_per_commit=int(raw["publish"]["max_months_per_commit"]),
                max_files_per_commit=int(raw["publish"]["max_files_per_commit"]),
            ),
        )
    except (KeyError, TypeError, AttributeError) as exc:
        raise ConfigError(f"invalid config {path}: missing {exc}") from exc

    if settings.lookback_months < 1:
        raise ConfigError("window.lookback_months must be >= 1")
    if settings.http_max_attempts < 1:
        raise ConfigError("http.max_attempts must be >= 1")
    if settings.publish.max_months_per_commit < 1:
        raise ConfigError("publish.max_months_per_commit must be >= 1")
    return settings
