"""Command line entry point."""

from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from cvm_lake import __version__
from cvm_lake.bronze import SOURCES, BronzeRun, PartitionResult
from cvm_lake.config import ENVIRONMENTS, load_settings
from cvm_lake.http import HttpClient
from cvm_lake.manifest import MANIFEST_NAME, Manifest
from cvm_lake.periods import lookback_months, parse_months, today_brt

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
    run.add_argument("--target", choices=["local"], default="local")
    run.add_argument("--out", type=Path, default=Path("data/lake"), help="local lake root")
    window = run.add_mutually_exclusive_group()
    window.add_argument("--months", type=_months_arg, help="e.g. 202301-202412 or 202301,202305")
    window.add_argument("--lookback", type=int, help="months back from today, default from config")
    run.add_argument(
        "--sources", type=_sources_arg, default=set(SOURCES), help=f"comma list of {SOURCES}"
    )
    run.add_argument("--force", action="store_true", help="ignore the manifest, rebuild all")
    run.add_argument("--config", type=Path, help="pipeline.yml path")
    run.add_argument("-v", "--verbose", action="store_true")
    return parser


def default_run_id() -> str:
    gh_run = os.environ.get("GITHUB_RUN_ID")
    if gh_run:
        return f"gh-{gh_run}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    return "local-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def format_summary(results: list[PartitionResult]) -> str:
    lines = [f"{'source':16} {'partition':22} {'status':14} {'rows':>9}  detail"]
    for r in results:
        rows = "" if r.rows is None else f"{r.rows:,}"
        lines.append(f"{r.source:16} {r.partition:22} {r.status:14} {rows:>9}  {r.detail}")
    counts = Counter(r.status.value for r in results)
    lines.append("totals: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    return "\n".join(lines)


def cmd_run(args: argparse.Namespace) -> int:
    settings = load_settings(args.env, args.config)
    today = today_brt()
    months = args.months or lookback_months(today, args.lookback or settings.lookback_months)
    run_id = default_run_id()
    root: Path = args.out
    manifest_path = root / MANIFEST_NAME
    manifest = Manifest.load(manifest_path)
    log.info("run %s env=%s months=%s sources=%s", run_id, args.env, months, sorted(args.sources))

    with (
        HttpClient(
            timeout=settings.http_timeout_seconds, max_attempts=settings.http_max_attempts
        ) as client,
        tempfile.TemporaryDirectory(prefix="cvm-lake-") as work,
    ):
        run = BronzeRun(
            settings=settings,
            root=root,
            client=client,
            manifest=manifest,
            run_id=run_id,
            today=today,
            work_dir=Path(work),
            force=args.force,
        )
        try:
            results = run.run(months, args.sources)
        finally:
            manifest.save(manifest_path)

    print(format_summary(results))
    return 0


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
