from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cvm_lake import silver
from cvm_lake.anomalies.detector import ANOMALY_SCHEMA, anomalies_path
from cvm_lake.anomalies.restatements import (
    RESTATEMENT_SCHEMA,
    detect_restatements,
    keep_previous,
    restatements_path,
)
from cvm_lake.bronze import SOURCES
from cvm_lake.config import load_settings
from cvm_lake.db import connect
from cvm_lake.manifest import MANIFEST_NAME, Manifest
from cvm_lake.periods import month_bounds
from cvm_lake.publish import run_on_store
from cvm_lake.sources.bcb_ptax import period_url

from fakes import (
    INF_MONTHLY,
    INF_V2_202610,
    PTAX_BASE,
    FakeServer,
    FakeStore,
    inf_monthly_zip,
    ptax_body,
    write_silver_fund_daily,
)

YM = "202610"


def _silver(root: Path, rows: list[dict], sha: str) -> None:
    write_silver_fund_daily(root, YM, [{**r, "source_sha256": sha} for r in rows])


BASE = [
    {"cnpj": "1", "dt": date(2026, 10, 1), "quota": "1.5", "equity": "100.00"},
    {"cnpj": "1", "dt": date(2026, 10, 2), "quota": "1.6", "equity": "110.00"},
    {"cnpj": "2", "dt": date(2026, 10, 1), "quota": "2.0", "equity": "50.00"},
]


def _restate(tmp_path: Path, new_rows: list[dict], *, new_sha: str = "s2", run: str = "r2"):
    root = tmp_path / "lake"
    _silver(root, BASE, "s1")
    old = keep_previous(root, YM, tmp_path / "work")
    assert old is not None
    _silver(root, new_rows, new_sha)
    stats = detect_restatements(connect(), root, YM, old, run, "c1")
    rows = (
        pq.read_table(root / restatements_path(YM)).to_pylist()
        if (root / restatements_path(YM)).exists()
        else []
    )
    return stats, rows, root


def test_changed_value(tmp_path: Path) -> None:
    changed = [BASE[0], {**BASE[1], "equity": "121.00"}, BASE[2]]
    stats, rows, root = _restate(tmp_path, changed)
    assert stats is not None and (stats.new_rows, stats.changed_reports) == (1, 1)
    (row,) = rows
    assert (row["fund_id"], row["dt"], row["field"], row["change"]) == (
        "1",
        date(2026, 10, 2),
        "equity",
        "changed",
    )
    assert (row["old_value"], row["new_value"]) == ("110.00", "121.00")
    assert row["rel_change"] == pytest.approx(0.1)
    assert (row["old_commit"], row["detected_run"], row["flagged_before"]) == ("c1", "r2", "")
    assert pq.read_schema(root / restatements_path(YM)).equals(RESTATEMENT_SCHEMA)


def test_added_and_removed_reports(tmp_path: Path) -> None:
    new = [BASE[0], BASE[1], {"cnpj": "3", "dt": date(2026, 10, 1), "quota": "1", "equity": "1"}]
    _, rows, _ = _restate(tmp_path, new)
    assert sorted((r["fund_id"], r["field"], r["change"]) for r in rows) == [
        ("2", "*", "removed"),
        ("3", "*", "added"),
    ]


def test_same_source_means_no_restatement(tmp_path: Path) -> None:
    stats, rows, _ = _restate(tmp_path, BASE, new_sha="s1")
    assert stats is None and rows == []


def test_flagged_before_comes_from_the_old_anomalies(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    path = root / anomalies_path(YM)
    path.parent.mkdir(parents=True)
    flagged = {
        "fund_id": "1",
        "class_id": "",
        "dt": date(2026, 10, 2),
        "score": 1.0,
        "severity": "high",
        "peer_group": "x",
        "features": "{}",
        "model_version": "v",
        "first_seen_run": "r1",
    }
    table = pa.Table.from_pylist(
        [{**flagged, "anomaly_type": t} for t in ("return_outlier", "decimal_shift")],
        schema=ANOMALY_SCHEMA,
    )
    pq.write_table(table, path)
    changed = [BASE[0], {**BASE[1], "quota": "16.0"}, BASE[2]]
    stats, rows, _ = _restate(tmp_path, changed)
    assert stats is not None and stats.flagged_before == 1
    assert rows[0]["flagged_before"] == "decimal_shift,return_outlier"


def test_restatements_accumulate_across_runs(tmp_path: Path) -> None:
    root = tmp_path / "lake"
    _silver(root, BASE, "s1")
    con = connect()
    old = keep_previous(root, YM, tmp_path / "w1")
    _silver(root, [BASE[0], {**BASE[1], "equity": "111.00"}, BASE[2]], "s2")
    detect_restatements(con, root, YM, old, "r2", None)
    old = keep_previous(root, YM, tmp_path / "w2")
    _silver(root, [BASE[0], {**BASE[1], "equity": "112.00"}, BASE[2]], "s3")
    stats = detect_restatements(con, root, YM, old, "r3", None)
    assert stats is not None and stats.total_rows == 2
    rows = pq.read_table(root / restatements_path(YM)).to_pylist()
    assert [(r["detected_run"], r["old_value"], r["new_value"]) for r in rows] == [
        ("r2", "110.00", "111.00"),
        ("r3", "111.00", "112.00"),
    ]


def test_restatement_through_the_hf_flow(tmp_path: Path) -> None:
    """A month restated at the source: the diff is published with the old commit sha."""
    server = FakeServer()
    server.add(INF_MONTHLY.format(YM), inf_monthly_zip(YM, INF_V2_202610))
    item = {
        "cotacaoCompra": 5.0,
        "cotacaoVenda": 5.0006,
        "dataHoraCotacao": "2026-10-01 13:05:00.1",
    }
    server.add(period_url(PTAX_BASE, *month_bounds(YM)), ptax_body([item]))
    remote = tmp_path / "remote"
    remote.mkdir()
    store = FakeStore(remote=remote)

    def run(run_id: str):
        work = tmp_path / f"work-{run_id}"
        work.mkdir()
        with server.client() as client:
            return run_on_store(
                settings=load_settings("dev"),
                store=store,
                root=tmp_path / f"runner-{run_id}",
                run_id=run_id,
                today=date(2026, 10, 29),
                months=[YM],
                sources=set(SOURCES) - {"cvm_registry"},
                client=client,
                work_dir=work,
                dry_run=False,
            )

    run("r1")
    restated = [
        INF_V2_202610[0],
        INF_V2_202610[1].replace("1221524.87", "1221600.00"),
        *INF_V2_202610[2:],
    ]
    server.add(
        INF_MONTHLY.format(YM),
        inf_monthly_zip(YM, restated),
        last_modified="Sat, 31 Oct 2026 09:00:00 GMT",
    )
    (chunk,) = run("r2")
    assert restatements_path(YM) in chunk.files
    (row,) = pq.read_table(remote / restatements_path(YM)).to_pylist()
    assert (row["field"], row["old_value"], row["new_value"]) == (
        "equity",
        "1221524.87",
        "1221600.00",
    )
    assert row["old_commit"] == "c1", "commit of the version that was replaced"

    (chunk,) = run("r3")  # nothing changed at the source
    assert restatements_path(YM) not in chunk.files
    assert Manifest.load(remote / MANIFEST_NAME).get("silver_fund_daily", YM) is not None
    assert (remote / silver.fund_daily_path(YM)).exists()
