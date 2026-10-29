"""Quality checks on the written silver and gold files.

A check with severity ``fail`` that does not pass fails the run (and, from Phase 3, blocks the
upload). ``warn`` checks are reported only.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from cvm_lake import silver
from cvm_lake.anomalies.detector import AnomalyStats, anomalies_path
from cvm_lake.config import QualityConfig
from cvm_lake.db import lit, one
from cvm_lake.gold import GoldStats
from cvm_lake.silver import FundDailyStats, PtaxStats

# Latin-1 text decoded twice shows up as "Ã" followed by a byte in 0x80..0xBF ("Ã§", "Ã£").
MOJIBAKE_PATTERN = "Ã[\\x{0080}-\\x{00BF}]"


class Severity(StrEnum):
    FAIL = "fail"
    WARN = "warn"


@dataclass(frozen=True)
class CheckResult:
    name: str
    scope: str
    passed: bool
    severity: Severity
    detail: str = ""

    @property
    def blocking(self) -> bool:
        return not self.passed and self.severity is Severity.FAIL


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    value = one(con, sql)[0]
    return int(value) if value is not None else 0


def check_fund_daily(
    con: duckdb.DuckDBPyConnection,
    root: Path,
    stats: FundDailyStats,
    previous_rows: int | None,
    cfg: QualityConfig,
) -> list[CheckResult]:
    ym = stats.month
    path = root / silver.fund_daily_path(ym)
    scope = f"silver/fund_daily/{ym}"
    results = []

    schema = pq.read_schema(path)
    same = schema.equals(silver.FUND_DAILY_SCHEMA, check_metadata=False)
    results.append(
        CheckResult("schema", scope, same, Severity.FAIL, "" if same else f"got {schema}")
    )

    dups = _scalar(
        con,
        f"SELECT count(*) FROM (SELECT cnpj, subclass_id, dt FROM {lit(path)} "
        "GROUP BY ALL HAVING count(*) > 1)",
    )
    results.append(CheckResult("unique_key", scope, dups == 0, Severity.FAIL, f"{dups} keys"))

    results.append(
        CheckResult(
            "parseable_values",
            scope,
            stats.unparseable == 0,
            Severity.FAIL,
            f"{stats.unparseable} rows with a value that does not cast",
        )
    )
    results.append(
        CheckResult(
            "dates_in_month",
            scope,
            stats.out_of_month == 0,
            Severity.WARN,
            f"{stats.out_of_month} rows dropped",
        )
    )
    results.append(
        CheckResult(
            "no_duplicate_source_rows",
            scope,
            stats.duplicates_removed == 0,
            Severity.WARN,
            f"{stats.duplicates_removed} rows removed",
        )
    )

    if previous_rows:
        drop_pct = 100.0 * (previous_rows - stats.rows) / previous_rows
        results.append(
            CheckResult(
                "row_count_drop",
                scope,
                drop_pct <= cfg.max_row_drop_pct,
                Severity.FAIL,
                f"{previous_rows} -> {stats.rows} rows ({drop_pct:+.1f}% drop, "
                f"max {cfg.max_row_drop_pct}%)",
            )
        )

    invalid = _scalar(con, f"SELECT count(*) FROM {lit(path)} WHERE equity < 0 OR quota <= 0")
    share = invalid / stats.rows if stats.rows else 0.0
    results.append(
        CheckResult(
            "invalid_value_share",
            scope,
            share <= cfg.max_invalid_value_share,
            Severity.FAIL,
            f"{invalid} rows with equity < 0 or quota <= 0 ({share:.2%}, "
            f"max {cfg.max_invalid_value_share:.2%})",
        )
    )
    return results


def check_registry(con: duckdb.DuckDBPyConnection, root: Path) -> list[CheckResult]:
    path = root / silver.FUND_REGISTRY_PATH
    scope = "silver/fund_registry"
    dups = _scalar(
        con,
        f"SELECT count(*) FROM (SELECT cnpj, source, valid_from FROM {lit(path)} "
        "GROUP BY ALL HAVING count(*) > 1)",
    )
    text_columns = ["peer_group", "cvm_class", "anbima_class", "class_type", "situation"]
    condition = " OR ".join(f"regexp_matches({c}, '{MOJIBAKE_PATTERN}')" for c in text_columns)
    mojibake = _scalar(con, f"SELECT count(*) FROM {lit(path)} WHERE {condition}")
    return [
        CheckResult("unique_key", scope, dups == 0, Severity.FAIL, f"{dups} keys"),
        CheckResult(
            "encoding", scope, mojibake == 0, Severity.FAIL, f"{mojibake} rows look double decoded"
        ),
    ]


def check_ptax(con: duckdb.DuckDBPyConnection, root: Path, stats: PtaxStats) -> list[CheckResult]:
    path = root / silver.ptax_daily_path(stats.month)
    scope = f"silver/ptax_daily/{stats.month}"
    dups = _scalar(
        con, f"SELECT count(*) FROM (SELECT dt FROM {lit(path)} GROUP BY dt HAVING count(*) > 1)"
    )
    bad = _scalar(
        con,
        f"SELECT count(*) FROM {lit(path)} WHERE NOT (buy > 0 AND sell > 0 AND sell >= buy)",
    )
    return [
        CheckResult("unique_date", scope, dups == 0, Severity.FAIL, f"{dups} dates"),
        CheckResult("positive_rates", scope, bad == 0, Severity.FAIL, f"{bad} rows"),
    ]


def check_gold(stats: GoldStats, cfg: QualityConfig) -> list[CheckResult]:
    scope = f"gold/{stats.month}"
    diff = abs(stats.aum_gold - stats.aum_silver)
    tolerance = max(1.0, abs(stats.aum_silver) * 1e-9)
    return [
        CheckResult(
            "aum_reconciles_with_silver",
            scope,
            diff <= tolerance,
            Severity.FAIL,
            f"silver {stats.aum_silver:,.2f} vs gold {stats.aum_gold:,.2f}",
        ),
        CheckResult(
            "peer_coverage",
            scope,
            stats.peer_coverage >= cfg.min_peer_coverage,
            Severity.WARN,
            f"{stats.peer_coverage:.2%} of rows (min {cfg.min_peer_coverage:.0%})",
        ),
    ]


def check_anomalies(
    con: duckdb.DuckDBPyConnection, root: Path, stats: AnomalyStats
) -> list[CheckResult]:
    path = root / anomalies_path(stats.month)
    scope = f"gold/anomalies/{stats.month}"
    dups = _scalar(
        con,
        f"SELECT count(*) FROM (SELECT fund_id, class_id, dt, anomaly_type FROM {lit(path)} "
        "GROUP BY ALL HAVING count(*) > 1)",
    )
    high = stats.by_severity.get("high", 0)
    share = high / stats.scored_reports if stats.scored_reports else 0.0
    return [
        CheckResult("unique_key", scope, dups == 0, Severity.FAIL, f"{dups} keys"),
        # A flood of high findings more likely means a data or code problem than real errors.
        CheckResult(
            "high_share",
            scope,
            share <= 0.005,
            Severity.WARN,
            f"{high} high of {stats.scored_reports} reports ({share:.3%}, warn above 0.5%)",
        ),
    ]
