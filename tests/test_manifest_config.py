import json
from pathlib import Path

import pytest

from cvm_lake.config import ConfigError, load_settings
from cvm_lake.manifest import Manifest, ManifestEntry


def _entry(**overrides: object) -> ManifestEntry:
    base: dict[str, object] = {
        "url": "u",
        "sha256": "s",
        "row_count": 3,
        "path": "bronze/x/part-0.parquet",
        "run_id": "r1",
        "written_at": "2026-10-29T17:00:00+00:00",
        "last_modified": "lm",
    }
    base.update(overrides)
    return ManifestEntry(**base)  # type: ignore[arg-type]


def test_manifest_roundtrip(tmp_path: Path) -> None:
    m = Manifest()
    m.put("cvm_inf_diario", "202610", _entry())
    path = tmp_path / "_manifest.json"
    m.save(path)
    loaded = Manifest.load(path)
    assert loaded.get("cvm_inf_diario", "202610") == _entry()
    assert loaded.get("cvm_inf_diario", "202609") is None


def test_manifest_missing_file_is_empty(tmp_path: Path) -> None:
    assert Manifest.load(tmp_path / "nope.json").entries == {}


def test_manifest_ignores_unknown_fields_and_checks_version(tmp_path: Path) -> None:
    path = tmp_path / "m.json"
    raw = {"schema_version": 1, "entries": {"a/b": {**_entry().__dict__, "future": 1}}}
    path.write_text(json.dumps(raw), encoding="utf-8")
    assert Manifest.load(path).get("a", "b") == _entry()
    path.write_text(json.dumps({"schema_version": 99, "entries": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="schema_version"):
        Manifest.load(path)


def test_repo_config_loads_for_both_envs() -> None:
    dev, prod = load_settings("dev"), load_settings("prod")
    assert dev.environment.branch == "development"
    assert prod.environment.branch == "main"
    assert dev.lookback_months == 2
    assert "{yyyymm}" in dev.source("cvm_inf_diario")["monthly_url"]


def test_config_rejects_unknown_env_and_bad_values(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="unknown environment"):
        load_settings("staging")
    bad = tmp_path / "p.yml"
    bad.write_text("environments: {dev: {}}\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="invalid config"):
        load_settings("dev", bad)
