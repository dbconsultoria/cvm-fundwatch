from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cvm_lake import quality, silver
from cvm_lake.config import QualityConfig
from cvm_lake.db import connect
from cvm_lake.gold import GoldStats
from cvm_lake.quality import CheckResult
from cvm_lake.silver import FundDailyStats, PtaxStats

from fakes import write_silver_fund_daily

CFG = QualityConfig(max_row_drop_pct=20, max_invalid_value_share=0.05, min_peer_coverage=0.95)
YM = "202610"
STATS = FundDailyStats(YM, "v2", 2, 2, 0, 0, 0)


def _by_name(results: list[CheckResult]) -> dict[str, CheckResult]:
    return {r.name: r for r in results}


def _two_rows(root: Path, **second: object) -> None:
    write_silver_fund_daily(
        root,
        YM,
        [
            {"cnpj": "1", "dt": date(2026, 10, 1), "quota": "1", "equity": "1"},
            {"cnpj": "1", "dt": date(2026, 10, 2), "quota": "1", "equity": "1", **second},
        ],
    )


def test_clean_partition_passes(tmp_path: Path) -> None:
    _two_rows(tmp_path)
    results = quality.check_fund_daily(connect(), tmp_path, STATS, previous_rows=2, cfg=CFG)
    assert [r for r in results if not r.passed] == []


def test_duplicate_key_fails(tmp_path: Path) -> None:
    _two_rows(tmp_path, dt=date(2026, 10, 1))
    r = _by_name(quality.check_fund_daily(connect(), tmp_path, STATS, None, CFG))["unique_key"]
    assert r.blocking


def test_schema_drift_fails(tmp_path: Path) -> None:
    path = tmp_path / silver.fund_daily_path(YM)
    path.parent.mkdir(parents=True)
    table = pa.table(
        {
            "cnpj": ["1"],
            "dt": pa.array([date(2026, 10, 1)], pa.date32()),
            "quota": [1.0],
            "equity": [1.0],
            "subclass_id": [""],
        }
    )
    pq.write_table(table, path)
    r = _by_name(quality.check_fund_daily(connect(), tmp_path, STATS, None, CFG))["schema"]
    assert r.blocking


@pytest.mark.parametrize(
    ("previous", "rows", "blocking"), [(100, 81, False), (100, 79, True), (None, 1, False)]
)
def test_row_count_drop(tmp_path: Path, previous: int | None, rows: int, blocking: bool) -> None:
    _two_rows(tmp_path)
    stats = replace(STATS, rows=rows)
    results = _by_name(quality.check_fund_daily(connect(), tmp_path, stats, previous, CFG))
    assert ("row_count_drop" in results) == (previous is not None)
    if previous is not None:
        assert results["row_count_drop"].blocking is blocking


def test_mass_of_invalid_values_fails(tmp_path: Path) -> None:
    _two_rows(tmp_path, equity="-5")  # 1 of 2 rows = 50%
    r = _by_name(quality.check_fund_daily(connect(), tmp_path, STATS, None, CFG))
    assert r["invalid_value_share"].blocking


def test_parse_problems_fail_and_drops_warn(tmp_path: Path) -> None:
    _two_rows(tmp_path)
    stats = replace(STATS, unparseable=1, out_of_month=3, duplicates_removed=2)
    r = _by_name(quality.check_fund_daily(connect(), tmp_path, stats, None, CFG))
    assert r["parseable_values"].blocking
    assert not r["dates_in_month"].passed and not r["dates_in_month"].blocking
    assert not r["no_duplicate_source_rows"].blocking


def _registry_file(root: Path, rows: list[dict[str, str]]) -> None:
    path = root / silver.FUND_REGISTRY_PATH
    path.parent.mkdir(parents=True)
    cols = [
        "cnpj",
        "source",
        "valid_from",
        "peer_group",
        "cvm_class",
        "anbima_class",
        "class_type",
        "situation",
    ]
    pq.write_table(pa.Table.from_pylist([{c: r.get(c) for c in cols} for r in rows]), path)


def test_registry_detects_double_decoded_text(tmp_path: Path) -> None:
    _registry_file(
        tmp_path,
        [
            {"cnpj": "1", "source": "s", "valid_from": "2020-01-01", "cvm_class": "Ações"},
            {"cnpj": "2", "source": "s", "valid_from": "2020-01-01", "cvm_class": "AÃ§Ãµes"},
        ],
    )
    r = _by_name(quality.check_registry(connect(), tmp_path))
    assert r["encoding"].blocking
    assert "1 rows" in r["encoding"].detail
    assert r["unique_key"].passed


def test_ptax_checks(tmp_path: Path) -> None:
    path = tmp_path / silver.ptax_daily_path(YM)
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.table({"dt": ["2026-10-01", "2026-10-01"], "buy": [5.0, 5.1], "sell": [5.1, 5.0]}), path
    )
    r = _by_name(quality.check_ptax(connect(), tmp_path, PtaxStats(YM, 2, 0)))
    assert r["unique_date"].blocking
    assert r["positive_rates"].blocking  # sell < buy on the second row


def test_gold_reconciliation_and_coverage() -> None:
    ok = _by_name(quality.check_gold(GoldStats(YM, 1, 1, 0.99, 1e12, 1e12 + 0.5), CFG))
    assert ok["aum_reconciles_with_silver"].passed and ok["peer_coverage"].passed
    bad = _by_name(quality.check_gold(GoldStats(YM, 1, 1, 0.5, 100.0, 90.0), CFG))
    assert bad["aum_reconciles_with_silver"].blocking
    assert not bad["peer_coverage"].passed and not bad["peer_coverage"].blocking
