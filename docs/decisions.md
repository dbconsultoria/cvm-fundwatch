# Decisions

Short ADR style entries. Status is `accepted`, `proposed` (waiting for review) or `superseded`.

## 0001 Python 3.12 with uv, hatchling build

- Status: accepted
- Context: reproducible environments locally and on GitHub hosted runners.
- Decision: `requires-python = ">=3.12,<3.13"`, `.python-version` = 3.12, `uv.lock` committed,
  CI installs with `uv sync --locked`. Hatchling as the build backend, `src/` layout.
- Consequence: a dependency change without a lock update fails CI.

## 0002 Detect the CVM layout from the file header, never from the month

- Status: accepted
- Context: there are three layouts (`hist_v0`, `v1`, `v2`, see `docs/sources.md`). The switch to
  `v2` happened when CVM regenerated files (around 2024-11), not at a data month boundary.
  202312 already has `v2` while 202311 still has `v1`.
- Decision: the bronze reader reads the header line and maps it to a known layout. An unknown
  header fails the run for that file.
- Consequence: a future regeneration of an old month in a new layout is handled without code
  changes, and a brand new layout is caught loudly.

## 0003 Decode CVM files as Latin-1

- Status: accepted
- Context: registry and metadata files are Latin-1. Daily files contain only ASCII.
- Decision: decode every CVM file as `latin-1`, write UTF-8 Parquet.
- Consequence: lossless for ASCII; correct for the registry. If CVM ever switches to UTF-8 the
  accented names in the registry would come out wrong, so a quality check will look for typical
  UTF-8 mojibake (`Ã§`, `Ã£`) in registry text columns.

## 0004 Tests never touch the network

- Status: accepted
- Decision: an autouse fixture in `tests/conftest.py` replaces `socket.connect` and
  `socket.create_connection`. A test that really needs the network must be marked `network`, and
  CI runs `-m "not network"`.
- Consequence: an accidental HTTP call in a test fails immediately.

## 0005 Action pinning

- Status: accepted
- Decision: pin GitHub actions to a major tag (`actions/checkout@v7`). `astral-sh/setup-uv` does
  not publish floating major tags (checked: `v10` returns 404), so it is pinned to an exact
  release (`v10.3.3`) and updated by hand or Dependabot.

## 0006 CNPJ as a 14 character string without punctuation

- Status: accepted
- Context: daily reports use `00.017.024/0001-53`, registry B uses `00017024000153`. Brazil is
  introducing alphanumeric CNPJ, so the value must never be cast to an integer.
- Decision: silver stores `cnpj` as the 14 character string with `.`, `/` and `-` removed.
  Bronze keeps the original string.

## 0007 Change detection: Last-Modified plus content hash

- Status: accepted
- Context: CVM regenerates the last ~12 months every Saturday, so `Last-Modified` changes even
  if the content may be identical. PTAX has no `Last-Modified` at all.
- Decision: the manifest stores `Last-Modified`, `ETag`, and the SHA-256 of the downloaded file.
  If `Last-Modified` is unchanged, skip without downloading (HEAD request). If it changed but the
  SHA-256 is the same, skip the rebuild and only update the manifest timestamp.
- Consequence: one extra HEAD per month, and a full download only when the header moved.

## 0008 Registry sources

- Status: accepted
- Decision: `registro_fundo_classe.zip` (current, post 175) plus `cad_fi_hist.zip` member
  `cad_fi_hist_classe.csv` (class history for pre 175 dates). Do not use `cad_fi.csv`.
- Consequence: we must map legacy classes (Referenciado, Curto Prazo, Divida Externa) to the post
  175 classification, and we build our own history of the current registry by snapshotting it.

## 0009 Reprocessing window

- Status: accepted
- Context: CVM refreshes M and M-1 daily and the previous ~12 months weekly (Saturday).
- Decision: daily run covers M and M-1. The Saturday run (or a Sunday run) uses a 13 month
  lookback; months with unchanged `Last-Modified` or hash are skipped, so it stays cheap.
- Consequence: restatements up to a year back are captured within a week.

## 0010 Silver grain is class or subclass per day

- Status: accepted
- Context: in `v2` a class with subclasses reports only subclass rows, never a class row.
- Decision: silver `fund_daily` key is `(cnpj, subclass_id, dt)` with `subclass_id = ''` for
  class level rows. Class level aggregates for gold are computed by summing subclasses.
- Consequence: no double counting of equity or flows in gold.

## 0011 ETL schedule time

- Status: accepted
- Context: CVM files for M and M-1 were refreshed at about 00:50 BRT (not 08:00 as documented);
  PTAX for the day was published at about 13:10 BRT.
- Decision: run at 17:23 UTC (14:23 BRT), Mon to Sat.

## 0012 Bronze is a pure function of the source file

- Status: accepted
- Context: "rerunning gives the same result" must hold for bronze too.
- Decision: bronze keeps every source column as a string (empty fields stay `''`, never null)
  and adds only source metadata: `_layout` (inf_diario), `_source_url`, `_source_member`,
  `_source_last_modified`, `_source_sha256`. No run id or ingestion timestamp in the file.
  Run lineage (`run_id`, `written_at`, `checked_run_id`, later `commit_sha`) lives in the
  manifest.
- Consequence: rebuilding a partition from the same source gives an identical table (tested).
  Bronze files of different layouts have different columns; read them with
  `union_by_name = true`.

## 0013 Registry bronze layout and snapshot cadence

- Status: accepted
- Context: the registry is four tables with different schemas, from two archives.
- Decision: `bronze/cvm_registry/<table>/snapshot_date=YYYY-MM-DD/part-0.parquet` for
  `registro_fundo`, `registro_classe`, `registro_subclasse` and `cad_fi_hist_classe`. A
  snapshot is written when the archive content changed, at most once per day (a rerun on the
  same day overwrites it).
- Consequence: about 7 MB of Parquet per snapshot (measured 2026-10-29). CVM regenerates the
  archives daily, so expect about 2.5 GB and about 300 files per table folder per year. Both are
  well inside HF limits. If it becomes a problem, add a minimum interval between snapshots.

## 0014 Month resolution for the daily report

- Status: accepted
- Decision: for each month try the monthly zip first; on 404 try the HIST yearly zip. Inside a
  zip, take member `inf_diario_fi_YYYYMM.csv`, else `inf_diario_fi_YYYY.csv` filtered on
  `DT_COMPTC`. Nothing is keyed on hard coded years.
- Consequence: if CVM moves 2021 to 2024 into HIST some day, backfills keep working.

## 0015 Failure policy for sources

- Status: accepted
- Decision:
  - 404 or an empty PTAX response: `not_published`, informational.
  - Network error, timeout, 408/425/429/5xx: retry with exponential backoff (2 to 60 s, 5
    attempts). Still failing: `unavailable`, warning, and the remaining partitions of that
    source are skipped in this run (no point waiting 5 times per month on a dead server).
  - Other 4xx (for example a 403 from Cloudflare): `unavailable` without retries.
  - Unknown CSV layout, missing registry column, malformed CSV or JSON: the run fails. That is
    a source change and needs a human.
- Consequence: an outage turns the run yellow (warnings), never red. A schema change turns it
  red.
