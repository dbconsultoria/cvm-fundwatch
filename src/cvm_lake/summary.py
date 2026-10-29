"""Run output for GitHub Actions: a Markdown step summary and workflow annotations."""

from __future__ import annotations

from collections import Counter

from cvm_lake.bronze import Status
from cvm_lake.pipeline import RunReport
from cvm_lake.publish import ChunkResult


def _escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def render_markdown(env_name: str, repo_id: str | None, chunks: list[ChunkResult]) -> str:
    lines = [f"## cvm-fundwatch ETL: `{env_name}`", ""]
    for i, chunk in enumerate(chunks, start=1):
        r = chunk.report
        title = f"### Run `{r.run_id}`, months {', '.join(r.months)}"
        lines += [title if len(chunks) == 1 else f"{title} (chunk {i}/{len(chunks)})", ""]

        if chunk.report.failed:
            outcome = "**Quality checks failed. Nothing was published.**"
        elif chunk.dry_run:
            outcome = f"Dry run: {len(chunk.files)} files built, not published (see the artifact)."
        elif repo_id:
            outcome = (
                f"Published {len(chunk.files)} files to `{repo_id}` in commit `{chunk.commit_sha}`."
            )
        else:
            outcome = "Local run."
        lines += [outcome, ""]

        if r.bronze:
            counts = Counter(b.status.value for b in r.bronze)
            lines += [
                "**Bronze**: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())),
                "",
                "| Source | Partition | Status | Rows | Detail |",
                "|---|---|---|---:|---|",
            ]
            for b in r.bronze:
                rows = "" if b.rows is None else f"{b.rows:,}"
                lines.append(
                    f"| {b.source} | {b.partition} | {b.status} | {rows} | {_escape(b.detail)} |"
                )
            lines.append("")

        if r.fund_daily:
            lines += ["**Silver and gold rows per month**", ""]
            lines += ["| Month | Layout | fund_daily | daily_flows | returns | Peer coverage |"]
            lines += ["|---|---|---:|---:|---:|---:|"]
            gold_by_month = {g.month: g for g in r.gold}
            for s in r.fund_daily:
                g = gold_by_month.get(s.month)
                flows = f"{g.daily_flows_rows:,}" if g else ""
                rets = f"{g.returns_rows:,}" if g else ""
                cov = f"{g.peer_coverage:.2%}" if g else ""
                lines.append(f"| {s.month} | {s.layout} | {s.rows:,} | {flows} | {rets} | {cov} |")
            lines.append("")

        if r.anomalies:
            lines += ["**Anomalies**", ""]
            lines += [
                "| Month | Rows | High | Medium | Low | New high |",
                "|---|---:|---:|---:|---:|---:|",
            ]
            for a in r.anomalies:
                sev = a.by_severity
                lines.append(
                    f"| {a.month} | {a.rows:,} | {sev.get('high', 0):,} | {sev.get('medium', 0):,}"
                    f" | {sev.get('low', 0):,} | {a.new_high:,} |"
                )
            lines += ["", f"Alerts (new high, within the daily budget): {len(r.alerts)}", ""]

        if r.restatements:
            lines += ["**Restatements**", ""]
            lines += [
                "| Month | Reports changed | Flagged before | New rows |",
                "|---|---:|---:|---:|",
            ]
            for x in r.restatements:
                lines.append(
                    f"| {x.month} | {x.changed_reports:,} | {x.flagged_before:,} | {x.new_rows:,} |"
                )
            lines.append("")

        failed = [c for c in r.checks if not c.passed]
        lines.append(
            f"**Quality**: {len(r.checks)} checks, {sum(c.blocking for c in failed)} failed, "
            f"{sum(not c.blocking for c in failed)} warnings"
        )
        if failed:
            lines += ["", "| Severity | Scope | Check | Detail |", "|---|---|---|---|"]
            lines += [
                f"| {c.severity} | {c.scope} | {c.name} | {_escape(c.detail)} |" for c in failed
            ]
        lines.append("")

        if chunk.files:
            lines += ["<details><summary>Files in the commit</summary>", ""]
            lines += [f"- `{f}`" for f in chunk.files]
            lines += ["", "</details>", ""]
    return "\n".join(lines)


def annotations(report: RunReport) -> list[str]:
    """``::warning::`` / ``::error::`` lines, shown on the workflow run page."""
    out = []
    for b in report.bronze:
        if b.status is Status.UNAVAILABLE:
            out.append(f"::warning title=Source unavailable::{b.source} {b.partition}: {b.detail}")
    for c in report.checks:
        if not c.passed:
            level = "error" if c.blocking else "warning"
            out.append(f"::{level} title=Quality {c.name}::{c.scope}: {c.detail}")
    return out
