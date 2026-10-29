"""Model commands: ``train``, ``publish-model``, and loading the champion for ``run`` (ADR 0033).

Model repo access uses ``HF_MODEL_TOKEN`` (write to the model repo, read the dataset repo), a
token separate from the ETL's ``HF_TOKEN``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

from cvm_lake import silver
from cvm_lake.anomalies.model import META_FILE, MODEL_FILE, ForestModel
from cvm_lake.anomalies.settings import load_anomaly_config
from cvm_lake.anomalies.train import (
    CARD_FILE,
    DECISION_FILE,
    METRICS_FILE,
    months_for,
    render_model_card,
    train_and_compare,
    write_outputs,
)
from cvm_lake.config import ENVIRONMENTS, EnvironmentConfig, load_settings
from cvm_lake.guard import GuardError, resolve_hf_target
from cvm_lake.periods import month_of, shift_month, today_brt
from cvm_lake.store import HfModelStore, HfStore

log = logging.getLogger(__name__)

MODEL_TOKEN = "HF_MODEL_TOKEN"
PUBLISHED_FILES = (MODEL_FILE, META_FILE, METRICS_FILE, DECISION_FILE, CARD_FILE)


def model_store(env: EnvironmentConfig, environ: Mapping[str, str]) -> HfModelStore:
    target = resolve_hf_target(env, environ, repo="model", token_var=MODEL_TOKEN)
    store = HfModelStore(target.repo_id, target.token)
    store.verify(env.name)
    return store


def load_forest(
    env: EnvironmentConfig,
    environ: Mapping[str, str],
    model_dir: Path | None,
    work: Path,
) -> ForestModel | None:
    """The champion for an ETL run: from ``model_dir``, else from the model repo when
    ``HF_MODEL_TOKEN`` is set. Any problem only disables layer 5, never the run."""
    try:
        if model_dir is not None:
            return ForestModel.load(model_dir)
        if not environ.get(MODEL_TOKEN):
            log.info("no %s: Isolation Forest layer off", MODEL_TOKEN)
            return None
        dest = work / "champion"
        if not model_store(env, environ).download_model(dest):
            log.warning("model repo has no model yet: Isolation Forest layer off")
            return None
        return ForestModel.load(dest)
    except (GuardError, ValueError, OSError) as exc:
        log.warning("Isolation Forest layer off: %s", exc)
        return None


def add_parsers(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    train = sub.add_parser("train", help="train the Isolation Forest, compare with the champion")
    train.add_argument("--env", choices=ENVIRONMENTS, required=True)
    train.add_argument("--target", choices=["local", "hf"], default="local")
    train.add_argument("--lake", type=Path, default=Path("data/lake"))
    train.add_argument("--eval-month", help="held out month, default: the last complete month")
    train.add_argument("--out", type=Path, default=Path("model_out"))
    train.add_argument("--champion-dir", type=Path, help="local target: current champion")
    train.add_argument("--anomaly-config", type=Path)
    train.add_argument("-v", "--verbose", action="store_true")

    pub = sub.add_parser("publish-model", help="push a trained model to the HF model repo")
    pub.add_argument("--env", choices=ENVIRONMENTS, required=True)
    pub.add_argument("--dir", type=Path, required=True)
    pub.add_argument("-v", "--verbose", action="store_true")


def cmd_train(args: argparse.Namespace) -> int:
    env = load_settings(args.env).environment
    cfg = load_anomaly_config(args.anomaly_config)
    assert cfg.forest is not None
    eval_month = args.eval_month or shift_month(month_of(today_brt()), -1)
    lake: Path = args.lake
    champion = None
    with tempfile.TemporaryDirectory(prefix="cvm-train-") as tmp:
        work = Path(tmp)
        if args.target == "hf":
            try:
                models = model_store(env, os.environ)
                data_target = resolve_hf_target(env, os.environ, token_var=MODEL_TOKEN)
                data = HfStore(data_target.repo_id, data_target.token)
                data.verify(env.name)
            except GuardError as exc:
                log.error("guard: %s", exc)
                return 2
            head = data.head()
            months = months_for(eval_month, cfg.forest.train_months, cfg.history_months)
            paths = [silver.fund_daily_path(m) for m in months] + [silver.FUND_REGISTRY_PATH]
            data.download(paths, lake, head.sha if head else None)
            if models.download_model(work / "champion"):
                champion = ForestModel.load(work / "champion")
        elif args.champion_dir is not None:
            champion = ForestModel.load(args.champion_dir)

        result = train_and_compare(
            lake, eval_month, cfg, champion, work / "data", args.anomaly_config
        )
    write_outputs(result, args.out, args.env)
    print(json.dumps(result.metrics, indent=2))
    _github_outputs(result.promote, render_model_card(result, args.env))
    return 0


def _github_outputs(promote: bool, card: str) -> None:
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with Path(out).open("a", encoding="utf-8") as fh:
            fh.write(f"promote={'true' if promote else 'false'}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        body = card.split("---", 2)[-1]  # without the YAML front matter
        with Path(summary).open("a", encoding="utf-8") as fh:
            fh.write(body)


def cmd_publish_model(args: argparse.Namespace) -> int:
    env = load_settings(args.env).environment
    decision = json.loads((args.dir / DECISION_FILE).read_text(encoding="utf-8"))
    if not decision.get("promote"):
        log.error("decision says not to promote: %s", decision.get("reason"))
        return 1
    ForestModel.load(args.dir)  # refuse to publish something that does not load
    try:
        store = model_store(env, os.environ)
    except GuardError as exc:
        log.error("guard: %s", exc)
        return 2
    head = store.head()
    tag = json.loads((args.dir / META_FILE).read_text(encoding="utf-8")).get("tag", "model")
    sha = store.commit(args.dir, list(PUBLISHED_FILES), f"model {tag}", head.sha if head else None)
    log.info("published %s to %s: %s", tag, store.repo_id, sha)
    return 0
