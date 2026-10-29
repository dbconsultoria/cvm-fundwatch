"""Anomaly detection v1 on synthetic panels: one fixture per anomaly type, the accounting
residual on a known series, and market crashes that must not be flagged."""

from __future__ import annotations

import math
from dataclasses import replace
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cvm_lake import gold, silver
from cvm_lake.anomalies.detector import ANOMALY_SCHEMA, anomalies_path, build_features, detect_month
from cvm_lake.anomalies.report import format_cnpj, render_alerts, select_alerts
from cvm_lake.anomalies.settings import AnomalyConfig, load_anomaly_config
from cvm_lake.db import connect

from fakes import INF_V2_202610, Panel, write_bronze_inf, write_silver_fund_daily

YM = "202609"
# Small windows so a two month panel (Aug history, Sep scored) is enough.
CFG = replace(load_anomaly_config(), window=20, min_history=10, history_months=1)


def detect(root: Path, panel: Panel | None = None, run_id: str = "r1", cfg: AnomalyConfig = CFG):
    if panel is not None:
        panel.write(root)
    con = connect()
    gold.load_registry(con, root)
    stats = detect_month(con, root, YM, cfg, run_id)
    rows = pq.read_table(root / anomalies_path(YM)).to_pylist()
    return stats, rows


def found(rows: list[dict], fund: str, kind: str | None = None) -> list[tuple[str, str, str]]:
    return sorted(
        (str(r["dt"]), r["anomaly_type"], r["severity"])
        for r in rows
        if r["fund_id"] == fund and (kind is None or r["anomaly_type"] == kind)
    )


def statistical(rows: list[dict]) -> list[dict]:
    kinds = {"return_outlier", "equity_residual", "structural_event", "shareholder_spike",
             "distribution_event"}  # fmt: skip
    return [r for r in rows if r["anomaly_type"] in kinds]


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def test_clean_panel_has_no_anomalies(root: Path) -> None:
    stats, rows = detect(root, Panel())
    assert rows == []
    assert stats.scored_reports == 30 * 22
    assert pq.read_schema(root / anomalies_path(YM)).equals(ANOMALY_SCHEMA)


# Market wide moves ----------------------------------------------------------------------------


def test_market_crash_is_not_flagged(root: Path) -> None:
    """Every fund drops 8% on the same day: nothing may be flagged, that day or after."""
    panel = Panel()
    for i in range(30):
        panel.scale(i, "2026-09-15", 0.92)
    _, rows = detect(root, panel)
    assert rows == []


def test_lagged_market_crash_is_not_flagged(root: Path) -> None:
    """Half the funds price the crash one day late (funds of funds, offshore funds)."""
    panel = Panel()
    for i in range(30):
        panel.scale(i, "2026-09-15" if i < 15 else "2026-09-16", 0.92)
    _, rows = detect(root, panel)
    assert rows == []


def test_crash_in_low_volatility_peer_group_is_not_flagged(root: Path) -> None:
    panel = Panel(peer="Renda Fixa", sigma=0.0002)
    for i in range(30):
        panel.scale(i, "2026-09-15", 0.98)
    _, rows = detect(root, panel)
    assert rows == []


# One fixture per anomaly type ----------------------------------------------------------------


def test_decimal_shift_and_echo(root: Path) -> None:
    panel = Panel()
    panel.scale(0, "2026-09-10", 100, ("quota",), until="2026-09-10")  # quota x100 for one day
    panel.scale(1, "2026-09-10", 0.001, ("equity",), until="2026-09-10")  # equity /1000
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[0]) == [
        ("2026-09-10", "decimal_shift", "high"),
        ("2026-09-11", "decimal_shift_echo", "low"),
    ]
    assert found(rows, panel.funds[1]) == [
        ("2026-09-10", "decimal_shift", "high"),
        ("2026-09-11", "decimal_shift_echo", "low"),
    ]
    shift = next(r for r in rows if r["fund_id"] == panel.funds[0])
    assert shift["score"] == pytest.approx(2.0, abs=0.01)


def test_decimal_shift_tolerance_and_powers() -> None:
    assert CFG.max_power == 3
    assert 10 ** (1 + CFG.shift_tolerance) < 10.24  # 10.0 to ~10.23 counts as x10, 10.5 does not


def test_impossible_value_flagged_once(root: Path) -> None:
    panel = Panel()
    panel.set(2, "2026-09-10", equity=-5.0)
    panel.set(2, "2026-09-11", equity=-5.0)
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[2], "impossible_value") == [
        ("2026-09-10", "impossible_value", "high")
    ]


def test_missing_value(root: Path) -> None:
    panel = Panel()
    panel.set(3, "2026-09-10", quota=None)
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[3], "missing_value") == [
        ("2026-09-10", "missing_value", "medium")
    ]


def test_reporting_gap(root: Path) -> None:
    panel = Panel()
    panel.drop(4, "2026-09-08", "2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14",
               "2026-09-15")  # fmt: skip
    panel.drop(5, "2026-09-10")
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[4], "reporting_gap") == [
        ("2026-09-16", "reporting_gap", "medium")
    ]
    assert found(rows, panel.funds[5], "reporting_gap") == [("2026-09-11", "reporting_gap", "low")]
    gap = next(r for r in rows if r["fund_id"] == panel.funds[4])
    assert gap["score"] == 6


def test_stale_quota_flagged_once(root: Path) -> None:
    panel = Panel()
    frozen = panel.rows[(panel.funds[6], panel.day("2026-09-18"))]
    for d in panel.days:
        if d > date(2026, 9, 18):
            panel.rows[(panel.funds[6], d)].update(quota=frozen["quota"], equity=frozen["equity"])
    _, rows = detect(root, panel)
    # 09-21 is the first unchanged report; the 5th one is 09-25
    assert found(rows, panel.funds[6]) == [("2026-09-25", "stale_quota", "medium")]


def test_fund_specific_jump_is_high_when_two_layers_agree(root: Path) -> None:
    panel = Panel()
    panel.scale(7, "2026-09-10", 1.05)  # quota and equity +5%, permanent
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[7]) == [("2026-09-10", "return_outlier", "high")]
    features = next(r for r in rows if r["fund_id"] == panel.funds[7])["features"]
    assert '"reverted":false' in features


def test_one_day_glitch_is_marked_reverted(root: Path) -> None:
    panel = Panel()
    panel.scale(8, "2026-09-10", 1.05, until="2026-09-10")
    _, rows = detect(root, panel)
    first = next(r for r in rows if r["fund_id"] == panel.funds[8])
    assert (str(first["dt"]), first["anomaly_type"], first["severity"]) == (
        "2026-09-10",
        "return_outlier",
        "high",
    )
    assert '"reverted":true' in first["features"]


def test_quota_drop_followed_by_equity_is_a_distribution(root: Path) -> None:
    panel = Panel()
    panel.scale(9, "2026-09-10", 0.95)  # amortization: quota and equity drop, no reversal
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[9]) == [("2026-09-10", "distribution_event", "low")]


def test_equity_jump_without_flows(root: Path) -> None:
    panel = Panel()
    panel.scale(10, "2026-09-10", 1.3, ("equity",))  # equity +30%, quota normal, no flows
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[10]) == [("2026-09-10", "equity_residual", "medium")]


def test_merger_is_a_structural_event(root: Path) -> None:
    panel = Panel()
    panel.scale(11, "2026-09-10", 1.3, ("equity",))
    panel.scale(11, "2026-09-10", 20, ("shareholders",))  # incorporated fund brings holders
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[11]) == [("2026-09-10", "structural_event", "medium")]


def test_shareholder_spike(root: Path) -> None:
    panel = Panel()
    panel.scale(12, "2026-09-10", 20, ("shareholders",))
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[12]) == [("2026-09-10", "shareholder_spike", "medium")]


def test_fund_closing_flagged_once(root: Path) -> None:
    panel = Panel()
    panel.scale(13, "2026-09-24", 0.0, ("equity", "quota"))
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[13], "fund_closing") == [("2026-09-24", "fund_closing", "low")]


def test_young_fund_gets_rules_only(root: Path) -> None:
    panel = Panel()
    for d in panel.days:
        if d < date(2026, 9, 21):
            del panel.rows[(panel.funds[14], d)]
    panel.scale(14, "2026-09-24", 1.10)  # big jump, but only 3 prior reports
    panel.scale(14, "2026-09-28", 1000, ("quota",), until="2026-09-28")
    _, rows = detect(root, panel)
    assert found(rows, panel.funds[14]) == [
        ("2026-09-28", "decimal_shift", "high"),
        ("2026-09-29", "decimal_shift_echo", "low"),
    ]


def test_duplicate_report_from_bronze(root: Path) -> None:
    lines = [*INF_V2_202610[:2], INF_V2_202610[1].replace("1221524.87", "1.00")]
    write_bronze_inf(root, "202610", lines)
    silver.build_fund_daily(connect(), root, "202610")
    con = connect()
    gold.load_registry(con, root)
    detect_month(con, root, "202610", CFG, "r1")
    rows = pq.read_table(root / anomalies_path("202610")).to_pylist()
    assert [(r["anomaly_type"], r["severity"], r["score"]) for r in rows] == [
        ("duplicate_report", "medium", 2.0)
    ]
    assert '"distinct_versions":2' in rows[0]["features"]


# Accounting identity -------------------------------------------------------------------------


def test_accounting_residual_on_a_known_series(root: Path) -> None:
    rows = [
        # equity_t - (equity_{t-1} * quota_t / quota_{t-1} + subs - reds), over equity_{t-1}
        ("2026-09-01", "1.00", "1000.00", "0", "0"),
        ("2026-09-02", "1.01", "1040.00", "50", "20"),  # expected 1040 -> 0
        ("2026-09-03", "1.01", "1050.00", "0", "0"),  # expected 1040 -> 10/1040
        ("2026-09-04", "1.00", "1029.60", "0", "0"),  # expected 1050 / 1.01
        ("2026-09-07", "1.00", "1029.60", "0", "29.60"),  # expected 1000 -> 29.60/1029.60
    ]
    write_silver_fund_daily(
        root,
        YM,
        [
            {"cnpj": "1", "dt": date.fromisoformat(d), "quota": q, "equity": e,
             "subscriptions": s, "redemptions": r}
            for d, q, e, s, r in rows
        ],
    )  # fmt: skip
    con = connect()
    gold.load_registry(con, root)
    build_features(con, root, YM, CFG)
    got = [r[0] for r in con.execute("SELECT residual FROM features ORDER BY dt").fetchall()]
    expected = [None, 0.0, 10 / 1040, (1029.60 - 1050 / 1.01) / 1050, 29.60 / 1029.60]
    assert got == pytest.approx(expected, abs=1e-12)


# first_seen_run and alerts --------------------------------------------------------------------


def test_first_seen_run_is_kept_across_runs(root: Path) -> None:
    panel = Panel()
    panel.scale(0, "2026-09-10", 100, ("quota",), until="2026-09-10")
    detect(root, panel, run_id="r1")
    panel.scale(1, "2026-09-17", 0.001, ("equity",), until="2026-09-17")
    stats, rows = detect(root, panel, run_id="r2")
    first_seen = {(r["fund_id"], r["anomaly_type"]): r["first_seen_run"] for r in rows}
    assert first_seen[(panel.funds[0], "decimal_shift")] == "r1"
    assert first_seen[(panel.funds[1], "decimal_shift")] == "r2"
    assert stats.new_high == 1


def test_alerts_only_new_recent_and_within_budget(root: Path) -> None:
    panel = Panel()
    for i in range(3):
        panel.scale(i, "2026-09-28", 100, ("quota",), until="2026-09-28")
    panel.scale(5, "2026-09-01", 100, ("quota",), until="2026-09-01")  # too old to alert
    detect(root, panel, run_id="r1")
    con = connect()
    tight = replace(CFG, budget_per_day=2, max_age_days=10)
    alerts = select_alerts(con, root, [YM], "r1", date(2026, 9, 30), tight)
    assert [(str(a.dt), a.anomaly_type) for a in alerts] == [("2026-09-28", "decimal_shift")] * 2
    assert select_alerts(con, root, [YM], "r2", date(2026, 9, 30), tight) == []
    md = render_alerts("prod", "r1", alerts, "o/cvm-fundos-lake")
    assert md.count("| 2026-09-28 | 00.000.000/0000-0") == 2  # formatted CNPJ
    assert "huggingface.co/datasets/o/cvm-fundos-lake" in md
    assert format_cnpj("123") == "123"


def test_shareholders_dropping_to_zero_give_a_finite_score(root: Path) -> None:
    panel = Panel()
    panel.set(12, "2026-09-10", shareholders=0)
    for d in panel.days:
        if d > date(2026, 9, 10):
            panel.rows[(panel.funds[12], d)]["shareholders"] = 0
    panel.scale(12, "2026-09-01", 10, ("shareholders",), until="2026-09-09")  # 1,000+ before
    _, rows = detect(root, panel)
    spike = [r for r in rows if r["fund_id"] == panel.funds[12] and r["dt"] == date(2026, 9, 10)]
    assert spike and all(math.isfinite(r["score"]) for r in spike)
    assert "Infinity" not in spike[0]["features"] and "inf" not in spike[0]["features"]
