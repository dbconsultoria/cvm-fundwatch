"""End to end bronze runs against a fake server. No network."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from cvm_lake.bronze import SOURCES, BronzeRun, PartitionResult, Status
from cvm_lake.config import load_settings
from cvm_lake.errors import SchemaError
from cvm_lake.manifest import Manifest
from cvm_lake.periods import month_bounds
from cvm_lake.sources.bcb_ptax import period_url

from fakes import (
    CAD_HIST_URL,
    INF_HIST,
    INF_HIST_V0_200501,
    INF_MONTHLY,
    INF_V1_202309,
    INF_V1_YEAR_2000,
    INF_V2_202610,
    PTAX_202504_BODY,
    PTAX_BASE,
    REGISTRO_URL,
    FakeServer,
    cad_hist_zip,
    csv_bytes,
    inf_monthly_zip,
    ptax_body,
    registro_zip,
    zip_bytes,
)

TODAY = date(2026, 10, 29)
INF = "cvm_inf_diario"


def ptax_url(ym: str) -> str:
    return period_url(PTAX_BASE, *month_bounds(ym))


@pytest.fixture
def server() -> FakeServer:
    s = FakeServer()
    s.add(INF_MONTHLY.format("202610"), inf_monthly_zip("202610", INF_V2_202610))
    s.add(INF_MONTHLY.format("202309"), inf_monthly_zip("202309", INF_V1_202309))
    s.add(
        INF_HIST.format("2005"),
        zip_bytes({"inf_diario_fi_200501.csv": csv_bytes(INF_HIST_V0_200501)}),
    )
    s.add(
        INF_HIST.format("2000"), zip_bytes({"inf_diario_fi_2000.csv": csv_bytes(INF_V1_YEAR_2000)})
    )
    s.add(REGISTRO_URL, registro_zip())
    s.add(CAD_HIST_URL, cad_hist_zip())
    s.add(ptax_url("202504"), PTAX_202504_BODY)
    return s


class Lake:
    def __init__(self, root: Path, server: FakeServer) -> None:
        self.root = root
        self.server = server
        self.settings = load_settings("dev")
        self.runs = 0

    def run(
        self, months: list[str], sources: set[str] | None = None, *, force: bool = False
    ) -> dict[tuple[str, str], PartitionResult]:
        self.runs += 1
        manifest = Manifest.load(self.root / "_manifest.json")
        work = self.root.parent / f"work{self.runs}"
        work.mkdir()
        with self.server.client() as client:
            bronze = BronzeRun(
                settings=self.settings,
                root=self.root,
                client=client,
                manifest=manifest,
                run_id=f"run{self.runs}",
                today=TODAY,
                work_dir=work,
                force=force,
            )
            try:
                results = bronze.run(months, sources or set(SOURCES))
            finally:
                manifest.save(self.root / "_manifest.json")
        return {(r.source, r.partition): r for r in results}

    def manifest(self) -> Manifest:
        return Manifest.load(self.root / "_manifest.json")

    def read(self, rel: str) -> dict[str, list[object]]:
        return pq.read_table(self.root / rel).to_pydict()


@pytest.fixture
def lake(tmp_path: Path, server: FakeServer) -> Lake:
    return Lake(tmp_path / "lake", server)


def test_first_run_writes_every_layout(lake: Lake) -> None:
    res = lake.run(["200002", "200501", "202309", "202610"], {INF})
    assert {k[1]: (r.status, r.rows) for k, r in res.items()} == {
        "200002": (Status.WRITTEN, 2),
        "200501": (Status.WRITTEN, 1),
        "202309": (Status.WRITTEN, 3),
        "202610": (Status.WRITTEN, 4),
    }
    v2 = lake.read("bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet")
    assert set(v2["_layout"]) == {"v2"}
    assert v2["_source_url"][0] == INF_MONTHLY.format("202610")
    assert v2["_source_last_modified"][0] == "Thu, 29 Oct 2026 03:50:46 GMT"
    assert v2["ID_SUBCLASSE"][2] == "RBMFN1747320951"
    old = lake.read("bronze/cvm_inf_diario/ano_mes=200501/part-0.parquet")
    assert old["_layout"] == ["hist_v0"]
    assert old["_source_url"] == [INF_HIST.format("2005")]

    entry = lake.manifest().get(INF, "202610")
    assert entry is not None
    assert (entry.row_count, entry.run_id, entry.etag) == (4, "run1", '"v1"')


def test_rerun_with_same_headers_downloads_nothing(lake: Lake, server: FakeServer) -> None:
    lake.run(["202610"], {INF})
    res = lake.run(["202610"], {INF})
    assert res[(INF, "202610")].status == Status.UNCHANGED
    assert server.count("GET", INF_MONTHLY.format("202610")) == 1
    entry = lake.manifest().get(INF, "202610")
    assert entry is not None
    assert (entry.run_id, entry.checked_run_id) == ("run1", "run2")


def test_new_last_modified_with_same_content_is_not_rewritten(
    lake: Lake, server: FakeServer
) -> None:
    lake.run(["202610"], {INF})
    path = lake.root / "bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet"
    mtime = path.stat().st_mtime_ns
    server.add(
        INF_MONTHLY.format("202610"),
        inf_monthly_zip("202610", INF_V2_202610),
        last_modified="Sat, 31 Oct 2026 09:00:00 GMT",
        etag='"v2"',
    )
    res = lake.run(["202610"], {INF})
    assert res[(INF, "202610")].status == Status.SAME_CONTENT
    assert path.stat().st_mtime_ns == mtime
    entry = lake.manifest().get(INF, "202610")
    assert entry is not None
    assert (entry.run_id, entry.last_modified, entry.etag) == (
        "run1",
        "Sat, 31 Oct 2026 09:00:00 GMT",
        '"v2"',
    )


def test_restated_month_is_rewritten(lake: Lake, server: FakeServer) -> None:
    lake.run(["202610"], {INF})
    restated = [INF_V2_202610[0], INF_V2_202610[1].replace("1221524.87", "1221524.88")]
    server.add(
        INF_MONTHLY.format("202610"),
        inf_monthly_zip("202610", restated),
        last_modified="Fri, 30 Oct 2026 03:50:00 GMT",
    )
    res = lake.run(["202610"], {INF})
    assert (res[(INF, "202610")].status, res[(INF, "202610")].rows) == (Status.WRITTEN, 1)
    data = lake.read("bronze/cvm_inf_diario/ano_mes=202610/part-0.parquet")
    assert data["VL_PATRIM_LIQ"] == ["1221524.88"]


def test_forced_rerun_is_idempotent(lake: Lake) -> None:
    months = ["202309", "202610", "202504"]
    lake.run(months)
    files = sorted(p.relative_to(lake.root) for p in lake.root.rglob("*.parquet"))
    first = {p: pq.read_table(lake.root / p) for p in files}
    res = lake.run(months, force=True)
    assert all(r.status in {Status.WRITTEN, Status.NOT_PUBLISHED} for r in res.values())
    assert sorted(p.relative_to(lake.root) for p in lake.root.rglob("*.parquet")) == files
    for p, table in first.items():
        assert pq.read_table(lake.root / p).equals(table), p


def test_unavailable_source_is_skipped_and_others_continue(lake: Lake, server: FakeServer) -> None:
    server.fail_forever(INF_MONTHLY.format("202309"))
    res = lake.run(["202309", "202610"])
    assert res[(INF, "202309")].status == Status.UNAVAILABLE
    assert res[(INF, "202610")].status == Status.UNAVAILABLE
    assert res[(INF, "202610")].detail == "skipped, source down"
    assert server.count("HEAD", INF_MONTHLY.format("202610")) == 0
    assert res[("cvm_registry", "registro_classe")].status == Status.WRITTEN
    assert lake.manifest().get(INF, "202309") is None


def test_month_not_published_anywhere(lake: Lake) -> None:
    res = lake.run(["202611"], {INF, "bcb_ptax"})
    assert res[(INF, "202611")].status == Status.NOT_PUBLISHED
    assert res[("bcb_ptax", "202611")].detail == "future month"


def test_unknown_layout_fails_the_run(lake: Lake, server: FakeServer) -> None:
    header = INF_V2_202610[0] + ";NEW_COLUMN"
    server.add(INF_MONTHLY.format("202610"), inf_monthly_zip("202610", [header]))
    with pytest.raises(SchemaError, match="unknown inf_diario layout"):
        lake.run(["202610"], {INF})


def test_registry_snapshot(lake: Lake, server: FakeServer) -> None:
    res = lake.run([], {"cvm_registry"})
    assert {k[1]: r.status for k, r in res.items()} == {
        "registro_fundo": Status.WRITTEN,
        "registro_classe": Status.WRITTEN,
        "registro_subclasse": Status.WRITTEN,
        "cad_fi_hist_classe": Status.WRITTEN,
    }
    assert server.count("GET", REGISTRO_URL) == 1, "archive downloaded once for three tables"
    classe = lake.read(
        "bronze/cvm_registry/registro_classe/snapshot_date=2026-10-29/part-0.parquet"
    )
    assert classe["Classificacao"] == ["Ações"]
    assert classe["_source_member"] == ["registro_classe.csv"]
    res = lake.run([], {"cvm_registry"})
    assert {r.status for r in res.values()} == {Status.UNCHANGED}


def test_ptax_month(lake: Lake, server: FakeServer) -> None:
    res = lake.run(["202504"], {"bcb_ptax"})
    assert (res[("bcb_ptax", "202504")].status, res[("bcb_ptax", "202504")].rows) == (
        Status.WRITTEN,
        3,
    )
    data = lake.read("bronze/bcb_ptax/ano_mes=202504/part-0.parquet")
    assert data["cotacaoVenda"][0] == "5.74960"
    res = lake.run(["202504"], {"bcb_ptax"})
    assert res[("bcb_ptax", "202504")].status == Status.SAME_CONTENT

    server.add(ptax_url("202610"), ptax_body([]))
    assert lake.run(["202610"], {"bcb_ptax"})[("bcb_ptax", "202610")].status == (
        Status.NOT_PUBLISHED
    )
