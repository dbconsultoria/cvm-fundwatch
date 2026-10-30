from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cvm_lake import gold, silver
from cvm_lake.db import connect

from fakes import write_silver_fund_daily

A = "00000000000101"
B = "00000000000202"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return tmp_path / "lake"


def _registry(root: Path, rows: list[dict[str, object]]) -> None:
    schema = pa.schema(
        [
            ("cnpj", pa.string()),
            ("source", pa.string()),
            ("valid_from", pa.date32()),
            ("valid_to", pa.date32()),
            ("peer_group", pa.string()),
            ("cvm_class", pa.string()),
            ("anbima_class", pa.string()),
            ("is_fic", pa.bool_()),
            ("class_type", pa.string()),
            ("situation", pa.string()),
            ("fund_cnpj", pa.string()),
            ("adapted_on", pa.date32()),
        ]
    )
    full = [{f.name: r.get(f.name) for f in schema} for r in rows]
    path = root / silver.FUND_REGISTRY_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(full, schema=schema), path)


def _ptax(root: Path, ym: str, quotes: dict[str, str]) -> None:
    table = pa.table(
        {
            "dt": pa.array([date.fromisoformat(d) for d in quotes], pa.date32()),
            "buy": pa.array([Decimal(v) for v in quotes.values()], pa.decimal128(18, 5)),
            "sell": pa.array([Decimal(v) for v in quotes.values()], pa.decimal128(18, 5)),
        }
    )
    path = root / silver.ptax_daily_path(ym)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _build(root: Path, ym: str) -> gold.GoldStats:
    con = connect()
    gold.load_registry(con, root)
    return gold.build_month(con, root, ym)


def _read(root: Path, rel: str) -> list[dict[str, object]]:
    return pq.read_table(root / rel).to_pylist()


def test_returns_brl_and_usd_on_a_five_day_series(root: Path) -> None:
    quotas = ["10", "10.1", "10.0", "10.0", "11.0"]
    days = ["2026-10-01", "2026-10-02", "2026-10-05", "2026-10-06", "2026-10-07"]
    write_silver_fund_daily(
        root,
        "202610",
        [
            {"cnpj": A, "dt": date.fromisoformat(d), "quota": q, "equity": "100"}
            for d, q in zip(days, quotas, strict=True)
        ],
    )
    # 2026-10-05 has no PTAX (pretend holiday): the 10-02 rate is used
    _ptax(
        root,
        "202610",
        {"2026-10-01": "5.0", "2026-10-02": "5.0", "2026-10-06": "5.5", "2026-10-07": "5.5"},
    )
    _build(root, "202610")
    rows = _read(root, gold.returns_path("202610"))

    assert rows[0]["ret_brl"] is None and rows[0]["prev_dt"] is None
    assert rows[1]["ret_brl"] == pytest.approx(0.01)
    assert rows[1]["ret_usd"] == pytest.approx(0.01)
    assert rows[2]["ptax_dt"] == date(2026, 10, 2)
    assert rows[2]["ret_usd"] == pytest.approx(10.0 / 10.1 - 1)
    # BRL flat, USD/BRL from 5.0 to 5.5: the fund lost 1 - 5/5.5 in dollars
    assert rows[3]["ret_brl"] == pytest.approx(0.0)
    assert rows[3]["ret_usd"] == pytest.approx(5.0 / 5.5 - 1)
    assert rows[4]["ret_brl"] == pytest.approx(0.1)
    assert rows[3]["gap_days"] == 1 and rows[2]["gap_days"] == 3


def test_first_day_of_month_uses_previous_month(root: Path) -> None:
    write_silver_fund_daily(
        root, "202609", [{"cnpj": A, "dt": date(2026, 9, 30), "quota": "2", "equity": "1"}]
    )
    write_silver_fund_daily(
        root, "202610", [{"cnpj": A, "dt": date(2026, 10, 1), "quota": "3", "equity": "1"}]
    )
    _ptax(root, "202609", {"2026-09-30": "4.0"})
    _ptax(root, "202610", {"2026-10-01": "4.0"})
    stats = _build(root, "202610")
    (row,) = _read(root, gold.returns_path("202610"))
    assert (row["prev_dt"], row["ret_brl"], row["ret_usd"]) == (date(2026, 9, 30), 0.5, 0.5)
    assert stats.returns_rows == 1


def test_zero_quota_gives_no_return(root: Path) -> None:
    write_silver_fund_daily(
        root,
        "202610",
        [
            {"cnpj": A, "dt": date(2026, 10, 1), "quota": "0", "equity": "1"},
            {"cnpj": A, "dt": date(2026, 10, 2), "quota": "1", "equity": "1"},
        ],
    )
    _build(root, "202610")
    assert [r["ret_brl"] for r in _read(root, gold.returns_path("202610"))] == [None, None]


def test_daily_flows_sum_subclasses_and_reconcile(root: Path) -> None:
    write_silver_fund_daily(
        root,
        "202610",
        [
            {
                "cnpj": A,
                "subclass_id": "S1",
                "dt": date(2026, 10, 1),
                "quota": "1",
                "equity": "100",
                "subscriptions": "10",
                "redemptions": "4",
                "shareholders": 2,
            },
            {
                "cnpj": A,
                "subclass_id": "S2",
                "dt": date(2026, 10, 1),
                "quota": "1",
                "equity": "50",
                "subscriptions": "0",
                "redemptions": "1",
                "shareholders": 3,
            },
            {"cnpj": B, "dt": date(2026, 10, 1), "quota": "1", "equity": "1000"},
        ],
    )
    _registry(
        root,
        [
            {
                "cnpj": A,
                "source": "registro_classe",
                "valid_from": date(1900, 1, 1),
                "peer_group": "Renda Fixa",
                "is_fic": False,
            },
            {
                "cnpj": B,
                "source": "registro_classe",
                "valid_from": date(1900, 1, 1),
                "peer_group": "Renda Fixa",
                "is_fic": True,
            },
        ],
    )
    stats = _build(root, "202610")
    flows = {r["is_fic"]: r for r in _read(root, gold.daily_flows_path("202610"))}
    a = flows[False]
    assert (a["n_classes"], a["n_reports"], a["aum"], a["net_flow"], a["shareholders"]) == (
        1,
        2,
        Decimal("150.00"),
        Decimal("5.00"),
        5,
    )
    assert flows[True]["aum"] == Decimal("1000.00")
    assert stats.aum_gold == stats.aum_silver == 1150.0
    assert stats.peer_coverage == 1.0


def test_implausible_flows_stay_out_of_the_totals(root: Path) -> None:
    # Real case, CNPJ 88007505000129 in 2003-01: about R$ 6 million of equity and a reported
    # subscription of R$ 832,260,010,000. Equity did not move, so the flow is a source error.
    def row(day: int, equity: str, subs: str = "0", reds: str = "0", cnpj: str = A) -> dict:
        return {
            "cnpj": cnpj,
            "dt": date(2026, 10, day),
            "quota": "1",
            "equity": equity,
            "subscriptions": subs,
            "redemptions": reds,
        }

    write_silver_fund_daily(
        root, "202609", [{"cnpj": B, "dt": date(2026, 9, 30), "quota": "1", "equity": "500"}]
    )
    write_silver_fund_daily(
        root,
        "202610",
        [
            row(1, "6000000", subs="832260010000"),  # dropped: 138,710x the equity
            row(2, "6000000", subs="60000000"),  # kept: exactly 10x
            row(1, "0", reds="500", cnpj=B),  # kept: a closing fund redeems its last equity
            row(2, "100", subs="100", cnpj=B),  # kept: reopens with its first subscription
        ],
    )
    stats = _build(root, "202610")
    flows = {r["dt"]: r for r in _read(root, gold.daily_flows_path("202610"))}
    assert flows[date(2026, 10, 1)]["subscriptions"] == Decimal("0.00")
    assert flows[date(2026, 10, 1)]["redemptions"] == Decimal("500.00")
    assert flows[date(2026, 10, 2)]["subscriptions"] == Decimal("60000100.00")
    assert flows[date(2026, 10, 1)]["aum"] == Decimal("6000000.00"), "AUM is not touched"
    assert stats.aum_gold == stats.aum_silver


def test_peer_group_precedence(root: Path) -> None:
    days = [date(2004, 6, 1), date(2010, 6, 1), date(2025, 6, 23), date(2025, 6, 24)]
    write_silver_fund_daily(
        root,
        "200406",
        [
            {"cnpj": A, "dt": days[0], "quota": "1", "equity": "1"},
            {"cnpj": B, "dt": days[0], "quota": "1", "equity": "1"},
            {"cnpj": "99999999000199", "dt": days[0], "quota": "1", "equity": "1"},
        ],
    )
    write_silver_fund_daily(
        root, "201006", [{"cnpj": A, "dt": days[1], "quota": "1", "equity": "1"}]
    )
    write_silver_fund_daily(
        root, "202506", [{"cnpj": A, "dt": d, "quota": "1", "equity": "1"} for d in days[2:]]
    )
    _registry(
        root,
        [
            {
                "cnpj": A,
                "source": "cad_fi_hist",
                "valid_from": date(2005, 3, 31),
                "valid_to": date(2025, 6, 24),
                "peer_group": "Ações",
            },
            {
                "cnpj": A,
                "source": "registro_classe",
                "valid_from": date(2025, 6, 24),
                "peer_group": "Multimercado",
                "adapted_on": date(2025, 6, 24),
            },
            {
                "cnpj": B,
                "source": "registro_classe",
                "valid_from": date(1900, 1, 1),
                "peer_group": "Renda Fixa",
            },
        ],
    )
    con = connect()
    gold.load_registry(con, root)
    got = {}
    for ym in ("200406", "201006", "202506"):
        gold.enrich_months(con, root, ym)
        for cnpj, dt, peer, src in con.execute(
            "SELECT cnpj, dt, peer_group, peer_source FROM enriched"
        ).fetchall():
            got[(cnpj, dt)] = (peer, src)
    assert got[(A, days[0])] == ("Ações", "cad_fi_hist_backfill")  # before history starts
    assert got[(A, days[1])] == ("Ações", "cad_fi_hist")
    assert got[(A, days[2])] == ("Ações", "cad_fi_hist")  # day before adaptation
    assert got[(A, days[3])] == ("Multimercado", "registro_classe")  # adaptation day
    assert got[(B, days[0])] == ("Renda Fixa", "registro_classe")
    assert got[("99999999000199", days[0])] == (gold.UNCLASSIFIED, None)


def test_gold_works_without_registry_and_ptax(root: Path) -> None:
    write_silver_fund_daily(
        root, "202610", [{"cnpj": A, "dt": date(2026, 10, 1), "quota": "1", "equity": "1"}]
    )
    stats = _build(root, "202610")
    assert stats.peer_coverage == 0.0
    (row,) = _read(root, gold.returns_path("202610"))
    assert (row["peer_group"], row["ptax"]) == (gold.UNCLASSIFIED, None)
