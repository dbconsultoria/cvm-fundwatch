"""One pipeline run: bronze -> silver -> gold -> anomalies -> quality checks.

Silver and gold are rebuilt for every month of the window that has bronze locally. That is cheap
(seconds per month) and keeps every run idempotent.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from cvm_lake import bronze, gold, quality, silver
from cvm_lake.anomalies.detector import AnomalyStats, detect_month
from cvm_lake.anomalies.report import Alert, select_alerts
from cvm_lake.anomalies.restatements import (
    RestatementStats,
    detect_restatements,
    keep_previous,
)
from cvm_lake.anomalies.settings import AnomalyConfig, load_anomaly_config
from cvm_lake.bronze import BronzeRun, PartitionResult
from cvm_lake.config import Settings
from cvm_lake.db import connect
from cvm_lake.http import HttpClient
from cvm_lake.manifest import Manifest, ManifestEntry
from cvm_lake.quality import CheckResult

log = logging.getLogger(__name__)

SILVER_FUND_DAILY = "silver_fund_daily"


@dataclass
class RunReport:
    run_id: str
    months: list[str]
    bronze: list[PartitionResult] = field(default_factory=list)
    fund_daily: list[silver.FundDailyStats] = field(default_factory=list)
    registry: silver.RegistryStats | None = None
    ptax: list[silver.PtaxStats] = field(default_factory=list)
    gold: list[gold.GoldStats] = field(default_factory=list)
    anomalies: list[AnomalyStats] = field(default_factory=list)
    restatements: list[RestatementStats] = field(default_factory=list)
    alerts: list[Alert] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return any(c.blocking for c in self.checks)


def run_pipeline(
    *,
    settings: Settings,
    root: Path,
    manifest: Manifest,
    run_id: str,
    today: date,
    months: list[str],
    sources: set[str],
    client: HttpClient | None,
    work_dir: Path,
    force: bool = False,
    anomaly_config: AnomalyConfig | None = None,
) -> RunReport:
    report = RunReport(run_id=run_id, months=months)
    anomaly_cfg = anomaly_config or load_anomaly_config()
    if client is not None and sources:
        report.bronze = BronzeRun(
            settings=settings,
            root=root,
            client=client,
            manifest=manifest,
            run_id=run_id,
            today=today,
            work_dir=work_dir,
            force=force,
        ).run(months, sources)

    con = connect()
    cfg = settings.quality
    built = [ym for ym in months if (root / bronze.inf_diario_path(ym)).exists()]

    for ym in built:
        old_version = keep_previous(root, ym, work_dir)
        stats = silver.build_fund_daily(con, root, ym)
        report.fund_daily.append(stats)
        previous = manifest.get(SILVER_FUND_DAILY, ym)
        report.checks += quality.check_fund_daily(
            con, root, stats, previous.row_count if previous else None, cfg
        )
        if old_version is not None:
            # before anomalies are recomputed: their old partition describes the old version
            restated = detect_restatements(
                con, root, ym, old_version, run_id, previous.commit_sha if previous else None
            )
            if restated is not None:
                report.restatements.append(restated)
                report.checks += quality.check_restatements(restated, stats.rows)

    report.registry = silver.build_fund_registry(con, root, settings.peer_groups)
    if report.registry is not None:
        report.checks += quality.check_registry(con, root)

    for ym in months:
        ptax_stats = silver.build_ptax_daily(con, root, ym)
        if ptax_stats is not None:
            report.ptax.append(ptax_stats)
            report.checks += quality.check_ptax(con, root, ptax_stats)

    gold.load_registry(con, root)
    for ym in built:
        gold_stats = gold.build_month(con, root, ym)
        report.gold.append(gold_stats)
        report.checks += quality.check_gold(gold_stats, cfg)

    for ym in built:
        anomaly_stats = detect_month(con, root, ym, anomaly_cfg, run_id)
        report.anomalies.append(anomaly_stats)
        report.checks += quality.check_anomalies(con, root, anomaly_stats)
    report.alerts = select_alerts(con, root, built, run_id, today, anomaly_cfg)
    con.close()

    if report.failed:
        log.error("quality checks failed, manifest keeps the previous silver row counts")
    else:
        now = datetime.now(UTC).isoformat(timespec="seconds")
        for stats in report.fund_daily:
            source = manifest.get(bronze.cvm_inf_diario.SOURCE, stats.month)
            manifest.put(
                SILVER_FUND_DAILY,
                stats.month,
                ManifestEntry(
                    url=bronze.inf_diario_path(stats.month),
                    sha256=source.sha256 if source else "",
                    row_count=stats.rows,
                    path=silver.fund_daily_path(stats.month),
                    run_id=run_id,
                    written_at=now,
                    checked_run_id=run_id,
                ),
            )
    return report


def format_report(report: RunReport) -> str:
    lines = [f"run {report.run_id}  months {', '.join(report.months) or '-'}", ""]
    if report.bronze:
        lines.append("BRONZE")
        lines.append(f"  {'source':16} {'partition':22} {'status':14} {'rows':>9}  detail")
        for r in report.bronze:
            rows = "" if r.rows is None else f"{r.rows:,}"
            lines.append(f"  {r.source:16} {r.partition:22} {r.status:14} {rows:>9}  {r.detail}")
        lines.append("")
    lines.append("SILVER")
    for s in report.fund_daily:
        lines.append(
            f"  fund_daily  {s.month}  {s.layout:8} {s.rows:>9,} rows"
            f"  (dropped: {s.out_of_month} out of month, {s.duplicates_removed} duplicates)"
        )
    if report.registry:
        r = report.registry
        lines.append(
            f"  fund_registry  snapshot {r.snapshot_date}  {r.current_rows:,} current"
            f" + {r.history_rows:,} history rows"
        )
    for p in report.ptax:
        lines.append(f"  ptax_daily  {p.month}  {p.rows} rows ({p.duplicates_removed} duplicates)")
    lines.append("")
    lines.append("GOLD")
    for g in report.gold:
        lines.append(
            f"  {g.month}  daily_flows {g.daily_flows_rows:,} rows  returns {g.returns_rows:,} rows"
            f"  peer coverage {g.peer_coverage:.2%}"
        )
    lines.append("")
    lines.append("ANOMALIES")
    for a in report.anomalies:
        sev = ", ".join(f"{k} {v}" for k, v in sorted(a.by_severity.items()))
        lines.append(f"  {a.month}  {a.rows:,} rows ({sev})  new high {a.new_high}")
    lines.append(f"  alerts: {len(report.alerts)}")
    for r in report.restatements:
        lines.append(
            f"  restatements {r.month}: {r.changed_reports:,} reports changed "
            f"({r.flagged_before:,} flagged before), {r.new_rows:,} new rows"
        )
    lines.append("")
    failed = [c for c in report.checks if not c.passed]
    lines.append(
        f"QUALITY  {len(report.checks)} checks, "
        f"{sum(c.blocking for c in failed)} failed, "
        f"{sum(not c.blocking for c in failed)} warnings"
    )
    for c in failed:
        lines.append(f"  {c.severity.upper():5} {c.scope} {c.name}: {c.detail}")
    return "\n".join(lines)
