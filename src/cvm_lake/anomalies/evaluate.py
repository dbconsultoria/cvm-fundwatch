"""Evaluation without labels: synthetic injection (ADR 0031).

For each month and each detector variant of an experiment:

1. Copy the silver partitions the detector needs into a scratch lake (the real lake is never
   modified), plus the registry.
2. Run the variant on the clean copy: the flags there are the variant's base alarm rate.
3. Inject the same planned errors (fixed seed) and run the variant again.
4. Score: recall per error kind and severity level, and a precision lower bound. Precision is a
   lower bound because the clean data already contains real anomalies that count as false
   positives here.

Results carry everything needed to reproduce them: seed, config hashes, silver source hashes,
git commit and package versions.
"""

from __future__ import annotations

import hashlib
import json
import logging
import platform
import shutil
import subprocess
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import yaml

from cvm_lake import gold, silver
from cvm_lake.anomalies.detector import anomalies_path, detect_month
from cvm_lake.anomalies.injection import (
    EXPECTED_TYPES,
    Injection,
    apply_injections,
    plan_injections,
)
from cvm_lake.anomalies.settings import DEFAULT_PATH, AnomalyConfig, load_anomaly_config
from cvm_lake.config import ConfigError
from cvm_lake.db import connect, lit
from cvm_lake.periods import shift_month

log = logging.getLogger(__name__)

LEVELS = ("low", "medium", "high")
RANK = {level: i for i, level in enumerate(LEVELS)}
ECHO_TYPES = frozenset({"decimal_shift_echo"})


@dataclass(frozen=True)
class Experiment:
    name: str
    seed: int
    injections: dict[str, int]
    variants: dict[str, dict[str, Any]]
    path: Path


def load_experiment(path: Path) -> Experiment:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return Experiment(
            name=str(raw["name"]),
            seed=int(raw["seed"]),
            injections={str(k): int(v) for k, v in raw["injections_per_month"].items()},
            variants={str(k): dict(v or {}) for k, v in raw["variants"].items()},
            path=path,
        )
    except (KeyError, TypeError, AttributeError, FileNotFoundError) as exc:
        raise ConfigError(f"invalid experiment {path}: {exc}") from exc


def variant_config(base: AnomalyConfig, overrides: dict[str, Any]) -> AnomalyConfig:
    known = {"layers", "robust_stats", "market_gate", "market_z", "z_return", "z_excess",
             "z_residual"}  # fmt: skip
    unknown = set(overrides) - known
    if unknown:
        raise ConfigError(f"unknown variant settings {sorted(unknown)}")
    changes: dict[str, Any] = {
        k: v for k, v in overrides.items() if k not in {"layers", "robust_stats"}
    }
    if "layers" in overrides:
        changes["layers"] = frozenset(overrides["layers"])
    if "robust_stats" in overrides:
        changes["robust"] = bool(overrides["robust_stats"])
    return replace(base, **changes)


# Scoring ------------------------------------------------------------------------------------

Key = tuple[str, str, str]  # cnpj, subclass_id, ISO date


@dataclass
class Score:
    injected: dict[str, int] = field(default_factory=dict)
    detected: dict[str, dict[str, int]] = field(default_factory=dict)  # kind -> level -> n
    right_type: dict[str, int] = field(default_factory=dict)  # kind -> n (any severity)
    flagged: dict[str, int] = field(default_factory=dict)  # level -> flagged keys (no echoes)
    true_flags: dict[str, int] = field(default_factory=dict)  # level -> flagged injected keys

    def as_dict(self) -> dict[str, Any]:
        kinds = {}
        for kind, n in sorted(self.injected.items()):
            det = self.detected.get(kind, {})
            kinds[kind] = {
                "injected": n,
                **{f"recall_{lv}": round(det.get(lv, 0) / n, 4) if n else None for lv in LEVELS},
                "right_type": round(self.right_type.get(kind, 0) / n, 4) if n else None,
            }
        total = sum(self.injected.values())
        overall = {
            f"recall_{lv}": round(sum(d.get(lv, 0) for d in self.detected.values()) / total, 4)
            if total
            else None
            for lv in LEVELS
        }
        precision = {
            f"precision_lb_{lv}": round(self.true_flags.get(lv, 0) / self.flagged[lv], 4)
            if self.flagged.get(lv)
            else None
            for lv in LEVELS
        }
        return {"kinds": kinds, "overall": overall, **precision, "flagged_keys": self.flagged}


def score(con: duckdb.DuckDBPyConnection, anomalies: Path, injections: list[Injection]) -> Score:
    rows = con.execute(
        f"SELECT fund_id, class_id, CAST(dt AS VARCHAR), anomaly_type, severity "
        f"FROM {lit(anomalies)}"
    ).fetchall()
    echo: set[Key] = {
        (i.cnpj, i.subclass_id, i.echo_dt.isoformat()) for i in injections if i.echo_dt
    }
    span: dict[tuple[str, str], list[Injection]] = {}
    for i in injections:
        span.setdefault((i.cnpj, i.subclass_id), []).append(i)

    def injection_of(key: Key) -> Injection | None:
        for i in span.get((key[0], key[1]), []):
            if i.dt.isoformat() <= key[2] <= i.until.isoformat():
                return i
        return None

    best: dict[Key, int] = {}  # key -> highest severity rank, echoes removed
    types: dict[Key, set[str]] = {}
    for fund, cls, dt, kind, severity in rows:
        key = (fund, cls, dt)
        if kind in ECHO_TYPES or key in echo:
            continue
        best[key] = max(best.get(key, -1), RANK[severity])
        types.setdefault(key, set()).add(kind)

    result = Score()
    hit: dict[int, int] = {}  # id(injection) -> best rank over its span
    hit_types: dict[int, set[str]] = {}
    for key, rank in best.items():
        inj = injection_of(key)
        for lv in LEVELS:
            if rank >= RANK[lv]:
                result.flagged[lv] = result.flagged.get(lv, 0) + 1
                if inj is not None:
                    result.true_flags[lv] = result.true_flags.get(lv, 0) + 1
        if inj is not None:
            hit[id(inj)] = max(hit.get(id(inj), -1), rank)
            hit_types.setdefault(id(inj), set()).update(types[key])
    for lv in LEVELS:
        result.flagged.setdefault(lv, 0)

    for i in injections:
        result.injected[i.kind] = result.injected.get(i.kind, 0) + 1
        rank = hit.get(id(i), -1)
        det = result.detected.setdefault(i.kind, {})
        for lv in LEVELS:
            if rank >= RANK[lv]:
                det[lv] = det.get(lv, 0) + 1
        if hit_types.get(id(i), set()) & EXPECTED_TYPES[i.kind]:
            result.right_type[i.kind] = result.right_type.get(i.kind, 0) + 1
    return result


# Running an experiment ----------------------------------------------------------------------


def months_needed(ym: str, cfg: AnomalyConfig) -> list[str]:
    return [shift_month(ym, -i) for i in range(cfg.history_months, -2, -1)]


def copy_inputs(lake: Path, scratch: Path, ym: str, cfg: AnomalyConfig) -> None:
    if scratch.exists():
        shutil.rmtree(scratch)
    for rel in [silver.fund_daily_path(m) for m in months_needed(ym, cfg)] + [
        silver.FUND_REGISTRY_PATH
    ]:
        src = lake / rel
        if src.exists():
            (scratch / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, scratch / rel)
    if not (scratch / silver.fund_daily_path(ym)).exists():
        raise FileNotFoundError(f"silver fund_daily {ym} is not in {lake}")


def _run_variant(root: Path, ym: str, cfg: AnomalyConfig, run_id: str) -> Path:
    con = connect()
    gold.load_registry(con, root)
    detect_month(con, root, ym, cfg, run_id)
    con.close()
    return root / anomalies_path(ym)


def evaluate_month(
    lake: Path,
    ym: str,
    experiment: Experiment,
    base: AnomalyConfig,
    work: Path,
) -> dict[str, Any]:
    clean, injected = work / f"{ym}-clean", work / f"{ym}-injected"
    copy_inputs(lake, clean, ym, base)
    copy_inputs(lake, injected, ym, base)

    con = connect()
    plan = plan_injections(
        con, injected, ym, experiment.injections, experiment.seed, base.min_history,
        base.history_months,
    )  # fmt: skip
    apply_injections(con, injected, ym, plan)
    reports = con.execute(
        f"SELECT count(*) FROM {lit(clean / silver.fund_daily_path(ym))}"
    ).fetchall()[0][0]
    sources = sorted(
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT source_sha256 FROM {lit(clean / silver.fund_daily_path(ym))}"
        ).fetchall()
    )

    variants = {}
    log.info("%s: %d reports, %d injections", ym, reports, len(plan))
    for name, overrides in experiment.variants.items():
        log.info("%s: variant %s", ym, name)
        cfg = variant_config(base, overrides)
        clean_flags = score(con, _run_variant(clean, ym, cfg, "clean"), [])
        result = score(con, _run_variant(injected, ym, cfg, "injected"), plan)
        variants[name] = {
            "settings": overrides,
            **result.as_dict(),
            "clean_flagged_keys": clean_flags.flagged,
            "clean_flags_per_10k": {
                lv: round(10_000 * n / reports, 2) for lv, n in clean_flags.flagged.items()
            },
        }
    con.close()
    return {
        "month": ym,
        "reports": reports,
        "silver_source_sha256": sources,
        "injections": {k: v for k, v in sorted(experiment.injections.items())},
        "variants": variants,
    }


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git() -> dict[str, Any]:
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        dirty = bool(
            subprocess.run(
                ["git", "status", "--porcelain", "--untracked-files=no"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
        )
    except (OSError, subprocess.CalledProcessError):
        return {"commit": None, "dirty": None}
    return {"commit": sha, "dirty": dirty}


def run_experiment(
    lake: Path,
    months: list[str],
    experiment: Experiment,
    work: Path,
    anomaly_config_path: Path | None = None,
) -> dict[str, Any]:
    config_path = anomaly_config_path or DEFAULT_PATH
    base = load_anomaly_config(config_path)
    return {
        "experiment": experiment.name,
        "seed": experiment.seed,
        "model_version": base.model_version,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "reproducibility": {
            "experiment_config_sha256": _sha256(experiment.path),
            "anomaly_config_sha256": _sha256(config_path),
            "git": _git(),
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "pyarrow": pa.__version__,
        },
        "months": [evaluate_month(lake, ym, experiment, base, work) for ym in months],
    }


def render_markdown(results: dict[str, Any]) -> str:
    lines = [
        f"# Experiment `{results['experiment']}`",
        "",
        f"Seed {results['seed']}, model `{results['model_version']}`, git "
        f"`{results['reproducibility']['git']['commit']}`. Recall = injected errors flagged at "
        "that severity or above; precision is a lower bound (real anomalies in the clean data "
        "count as false positives). Clean flags = flagged reports per 10,000 without injection.",
        "",
    ]
    for month in results["months"]:
        kinds = list(month["injections"])
        lines += [f"## {month['month']} ({month['reports']:,} reports)", ""]
        header = "| Variant | " + " | ".join(kinds) + " | Recall (med+) | Recall (high) |"
        header += " Precision LB (med+) | Precision LB (high) | Clean flags/10k (med+) |"
        lines += [header, "|---" * (len(kinds) + 6) + "|"]
        for name, v in month["variants"].items():
            cells = [f"{v['kinds'][k]['recall_medium']:.2f}" for k in kinds]
            fmt = lambda x: "-" if x is None else f"{x:.3f}"  # noqa: E731
            lines.append(
                f"| {name} | " + " | ".join(cells)
                + f" | {fmt(v['overall']['recall_medium'])} | {fmt(v['overall']['recall_high'])}"
                + f" | {fmt(v['precision_lb_medium'])} | {fmt(v['precision_lb_high'])}"
                + f" | {v['clean_flags_per_10k']['medium']:.1f} |"
            )  # fmt: skip
        lines += ["", "Per kind columns: recall at medium severity or above.", ""]
    return "\n".join(lines)


def save_results(results: dict[str, Any], out: Path) -> tuple[Path, Path]:
    out.mkdir(parents=True, exist_ok=True)
    months = "-".join(m["month"] for m in results["months"])
    stem = f"{results['experiment']}_{months}"
    json_path, md_path = out / f"{stem}.json", out / f"{stem}.md"
    json_path.write_text(json.dumps(results, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    md_path.write_text(render_markdown(results) + "\n", encoding="utf-8")
    return json_path, md_path
