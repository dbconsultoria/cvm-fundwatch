"""Command line entry point."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from cvm_lake import __version__
from cvm_lake.bronze import SOURCES
from cvm_lake.config import ENVIRONMENTS, Settings, load_settings
from cvm_lake.guard import GuardError, resolve_hf_target
from cvm_lake.http import HttpClient
from cvm_lake.manifest import MANIFEST_NAME, Manifest
from cvm_lake.periods import lookback_months, parse_months, today_brt
from cvm_lake.pipeline import format_report, run_pipeline
from cvm_lake.publish import ChunkResult, PublishError, run_on_store
from cvm_lake.store import HfStore
from cvm_lake.summary import annotations, render_markdown

log = logging.getLogger("cvm_lake")


def _months_arg(value: str) -> list[str]:
    try:
        return parse_months(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _sources_arg(value: str) -> set[str]:
    chosen = {s.strip() for s in value.split(",") if s.strip()}
    unknown = chosen - set(SOURCES)
    if unknown or not chosen:
        raise argparse.ArgumentTypeError(
            f"unknown sources {sorted(unknown)}, choose from {SOURCES}"
        )
    return chosen


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cvm-lake", description="cvm-fundwatch pipeline")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="extract sources and build the lake")
    run.add_argument("--env", choices=ENVIRONMENTS, required=True)
    run.add_argument(
        "--target", choices=["local", "hf"], default="local", help="hf: the Hugging Face repo"
    )
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="hf target: read the repo, build everything locally, publish nothing",
    )
    run.add_argument("--out", type=Path, default=Path("data/lake"), help="local lake root")
    window = run.add_mutually_exclusive_group()
    window.add_argument("--months", type=_months_arg, help="e.g. 202301-202412 or 202301,202305")
    window.add_argument("--lookback", type=int, help="months back from today, default from config")
    run.add_argument(
        "--sources", type=_sources_arg, default=set(SOURCES), help=f"comma list of {SOURCES}"
    )
    run.add_argument("--force", action="store_true", help="ignore the manifest, rebuild all")
    run.add_argument(
        "--no-extract",
        action="store_true",
        help="skip downloads, rebuild silver and gold from the local bronze only",
    )
    run.add_argument("--config", type=Path, help="pipeline.yml path")
    run.add_argument("-v", "--verbose", action="store_true")
    return parser


def default_run_id() -> str:
    gh_run = os.environ.get("GITHUB_RUN_ID")
    if gh_run:
        return f"gh-{gh_run}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return "local-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _window(args: argparse.Namespace, settings: Settings) -> list[str]:
    return args.months or lookback_months(today_brt(), args.lookback or settings.lookback_months)


def _emit(env: str, repo_id: str | None, chunks: list[ChunkResult]) -> None:
    for chunk in chunks:
        print(format_report(chunk.report))
        if os.environ.get("GITHUB_ACTIONS") == "true":
            for line in annotations(chunk.report):
                print(line)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as fh:
            fh.write(render_markdown(env, repo_id, chunks))


def cmd_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.env, args.config)
    months = _window(args, settings)
    run_id = default_run_id()
    root: Path = args.out
    sources = set() if args.no_extract else args.sources
    log.info("run %s env=%s target=%s months=%s", run_id, args.env, args.target, months)

    store = None
    if args.target == "hf":
        try:
            target = resolve_hf_target(settings.environment, os.environ)
            store = HfStore(target.repo_id, target.token)
            store.verify(settings.environment.name)
        except GuardError as exc:
            log.error("guard: %s", exc)
            return 2

    with (
        HttpClient(
            timeout=settings.http_timeout_seconds, max_attempts=settings.http_max_attempts
        ) as client,
        tempfile.TemporaryDirectory(prefix="cvm-lake-") as work,
    ):
        if store is None:
            manifest_path = root / MANIFEST_NAME
            manifest = Manifest.load(manifest_path)
            try:
                report = run_pipeline(
                    settings=settings,
                    root=root,
                    manifest=manifest,
                    run_id=run_id,
                    today=today_brt(),
                    months=months,
                    sources=sources,
                    client=client,
                    work_dir=Path(work),
                    force=args.force,
                )
            finally:
                manifest.save(manifest_path)
            chunks = [ChunkResult(report=report)]
        else:
            try:
                chunks = run_on_store(
                    settings=settings,
                    store=store,
                    root=root,
                    run_id=run_id,
                    today=today_brt(),
                    months=months,
                    sources=sources,
                    client=client,
                    work_dir=Path(work),
                    dry_run=args.dry_run,
                    force=args.force,
                )
            except PublishError as exc:
                log.error("publish: %s", exc)
                return 1

    _emit(args.env, store.repo_id if store else None, chunks)
    return 1 if any(c.report.failed for c in chunks) else 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    if not args.verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)
    return cmd_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
