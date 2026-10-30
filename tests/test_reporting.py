from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cvm_lake import gold, reporting
from cvm_lake.anomalies.detector import detect_month
from cvm_lake.anomalies.restatements import detect_restatements, keep_previous
from cvm_lake.anomalies.settings import load_anomaly_config
from cvm_lake.db import connect

from fakes import Panel, write_silver_fund_daily

CFG = replace(load_anomaly_config(), window=20, min_history=10, history_months=1)
MONTHS = ["202608", "202609"]


def _build(root: Path, panel: Panel, months: list[str] = MONTHS) -> reporting.ReportingStats:
    panel.write(root)
    con = connect()
    gold.load_registry(con, root)
    for ym in months:
        gold.build_month(con, root, ym)
        detect_month(con, root, ym, CFG, "r1")
    stats = reporting.build_reporting(con, root, months)
    assert stats is not None
    return stats


def _read(root: Path, name: str) -> list[dict]:
    return pq.read_table(root / reporting.reporting_path(name)).to_pylist()


@pytest.fixture
def panel() -> Panel:
    p = Panel(n=30)
    p.scale(0, "2026-09-10", 100, ("quota",), until="2026-09-10")  # one decimal shift
    return p


def test_tables_and_schemas(tmp_path: Path, panel: Panel) -> None:
    stats = _build(tmp_path, panel)
    assert stats.rows["aum_flows_daily"] == len(panel.days)
    assert (
        stats.rows["ptax_daily"] == 0
        and not (tmp_path / reporting.reporting_path("ptax_daily")).exists()
    )
    for name, schema in reporting.SCHEMAS.items():
        path = tmp_path / reporting.reporting_path(name)
        if path.exists():
            assert pq.read_schema(path).equals(schema), name

    monthly = _read(tmp_path, "returns_monthly")
    assert [(r["ano_mes"], r["peer_group"], r["n_funds"]) for r in monthly] == [
        ("202608", "Multimercado", 30),
        ("202609", "Multimercado", 30),
    ]
    assert all(r["p25_ret_brl"] <= r["median_ret_brl"] <= r["p75_ret_brl"] for r in monthly)

    recent = _read(tmp_path, "anomalies_recent")
    shift = next(r for r in recent if r["anomaly_type"] == "decimal_shift")
    assert (shift["dt"], shift["severity"], shift["name"]) == (
        date(2026, 9, 10),
        "high",
        f"FUNDO {panel.funds[0]}",
    )
    daily = _read(tmp_path, "anomalies_daily")
    assert {(r["dt"], r["anomaly_type"], r["n"]) for r in daily} >= {
        (date(2026, 9, 10), "decimal_shift", 1)
    }


def test_a_run_replaces_only_its_months(tmp_path: Path, panel: Panel) -> None:
    _build(tmp_path, panel)
    before = {r["dt"]: r for r in _read(tmp_path, "aum_flows_daily")}

    # 202609 is rebuilt with every fund's equity doubled; 202608 must stay as it was
    for i in range(30):
        panel.scale(i, "2026-09-01", 2.0, ("equity",))
    con = connect()
    panel.write(tmp_path)
    gold.load_registry(con, tmp_path)
    gold.build_month(con, tmp_path, "202609")
    reporting.build_reporting(con, tmp_path, ["202609"])
    after = {r["dt"]: r for r in _read(tmp_path, "aum_flows_daily")}

    assert len(after) == len(before), "no duplicated dates"
    assert after[date(2026, 8, 31)]["aum"] == before[date(2026, 8, 31)]["aum"]
    assert after[date(2026, 9, 30)]["aum"] == pytest.approx(2 * before[date(2026, 9, 30)]["aum"])


def test_incomplete_latest_date(tmp_path: Path) -> None:
    panel = Panel(n=30)
    for i in range(1, 30):  # only one fund has reported the last date so far
        panel.drop(i, "2026-09-30")
    _build(tmp_path, panel)
    flags = {r["dt"]: r["complete"] for r in _read(tmp_path, "aum_flows_daily")}
    assert flags[date(2026, 9, 30)] is False
    assert flags[date(2026, 9, 29)] is True
    assert reporting.latest_complete_date(connect(), tmp_path) == "2026-09-29"


def test_completeness_weighs_classes_by_equity(tmp_path: Path) -> None:
    # Real case, 2026-10-28: 92% of the classes had reported (enough by count) but they held
    # only 94.8% of the expected equity, and total AUM looked 7% lower than the day before.
    panel = Panel(n=30)  # fund i holds (i + 1) / 465 of the equity
    panel.drop(29, "2026-09-30")  # the two largest: 28 of 30 classes, about 87% of the equity
    panel.drop(28, "2026-09-30")
    panel.drop(0, "2026-09-29")  # the smallest: 0.2% of the equity
    _build(tmp_path, panel)
    rows = {r["dt"]: r for r in _read(tmp_path, "aum_flows_daily")}
    assert rows[date(2026, 9, 30)]["coverage"] == pytest.approx(1 - 59 / 465, abs=0.01)
    assert rows[date(2026, 9, 30)]["complete"] is False
    assert rows[date(2026, 9, 29)]["coverage"] == pytest.approx(1 - 1 / 465, abs=0.001)
    assert rows[date(2026, 9, 29)]["complete"] is True
    assert rows[date(2026, 9, 1)]["coverage"] == 1.0, "August gives September its history"
    assert reporting.latest_complete_date(connect(), tmp_path) == "2026-09-29"


def test_a_file_without_coverage_is_read_as_null(tmp_path: Path, panel: Panel) -> None:
    _build(tmp_path, panel)
    path = tmp_path / reporting.reporting_path("aum_flows_daily")
    pq.write_table(pq.read_table(path).drop_columns(["coverage"]), path)  # written before 0036

    con = connect()
    gold.load_registry(con, tmp_path)
    reporting.build_reporting(con, tmp_path, ["202609"])
    rows = _read(tmp_path, "aum_flows_daily")
    assert pq.read_schema(path).equals(reporting.SCHEMAS["aum_flows_daily"])
    august = [r for r in rows if r["dt"].month == 8]
    assert august and all(r["coverage"] is None and r["complete"] for r in august[10:])
    assert all(r["coverage"] is not None for r in rows if r["dt"].month == 9)


def test_restatements_monthly(tmp_path: Path) -> None:
    rows = [
        {"cnpj": "1", "dt": date(2026, 9, 1), "quota": "1", "equity": "10"},
        {"cnpj": "2", "dt": date(2026, 9, 1), "quota": "1", "equity": "20"},
    ]
    write_silver_fund_daily(tmp_path, "202609", [{**r, "source_sha256": "a"} for r in rows])
    old = keep_previous(tmp_path, "202609", tmp_path / "w")
    changed = [rows[0], {**rows[1], "equity": "21"}]
    write_silver_fund_daily(tmp_path, "202609", [{**r, "source_sha256": "b"} for r in changed])
    con = connect()
    assert old is not None
    detect_restatements(con, tmp_path, "202609", old, "r2", None)
    gold.load_registry(con, tmp_path)
    gold.build_month(con, tmp_path, "202609")
    reporting.build_reporting(con, tmp_path, ["202609"])
    assert _read(tmp_path, "restatements_monthly") == [
        {"ano_mes": "202609", "detected_run": "r2", "changed_reports": 1, "flagged_before": 0}
    ]


def test_nothing_to_build() -> None:
    assert reporting.build_reporting(connect(), Path(), []) is None
