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

## 0016 Silver fund_daily typing and cleaning

- Status: accepted
- Decision:
  - Columns: `cnpj`, `subclass_id`, `dt`, `report_type`, `portfolio_value`, `quota`,
    `equity`, `subscriptions`, `redemptions`, `shareholders`, `source_layout`,
    `source_sha256`. `cnpj` is the fund CNPJ before 175 and the class CNPJ after it (the same
    value for 99.6% of classes), so one key spans both eras.
  - Money is `DECIMAL(38,2)`, quota `DECIMAL(38,12)`, no floats. 38 digits leaves room for
    decimal shift errors (x1000) that the anomaly layer has to see, not a cast failure.
  - Empty source fields become NULL. A non empty value that does not cast is counted and fails
    the run (quality check `parseable_values`); none exist in the inspected months.
  - Rows dated outside the partition month are dropped and reported (warning).
  - Duplicate `(cnpj, subclass_id, dt)`: the last row in the source file wins, reported.
  - Weekend and holiday rows (seen in 2005) are kept; they are data, the anomaly layer decides.
- Consequence: one schema for the three layouts, enforced by a test and a runtime check.

## 0017 Peer group

- Status: accepted
- Context: the anomaly detection compares a fund with its peers on the same day.
- Decision:
  - Peer group = CVM class, mapped by `peer_groups` in `config/pipeline.yml`. Legacy
    `Referenciado`, `Curto Prazo` and `Divida Externa` become `Renda Fixa` (they are Renda
    Fixa subtypes since ICVM 555). Unlisted values become `Outros`, missing ones
    `Nao classificado`.
  - `silver/fund_registry` holds intervals: `cad_fi_hist` rows (legacy history, cut at the
    fund's adaptation date) and one `registro_classe` row per class (valid from the
    adaptation date).
  - Precedence for a fund and date: legacy class valid on that date, then the earliest legacy
    class when the date is before the history starts (`cad_fi_hist_backfill`; most history
    starts in 2005), then the current class. `peer_source` records which one was used.
  - The ANBIMA classification and the FIC flag come from the current registry only and are
    carried along for later use (finer peers, FIC double counting).
- Consequence: coverage measured on real data: 95.9% (2005-01), 99.9% (2020-03),
  99.95% (2023-09), 99.8% (2026-10). Coverage below 95% is a warning.
- Registry duplicates (real): `registro_classe` repeats 13 CNPJs and `registro_fundo` repeats
  ids. Rule: not cancelled first, then the latest `Data_Registro`, then the highest id.

## 0018 Write Parquet through Arrow, not DuckDB COPY

- Status: accepted
- Context: DuckDB 2 marks a Parquet column `NOT NULL` when the data has no nulls, so
  partitions of one table got different schemas. DuckDB 2 also keeps a connection busy after
  a partial fetch (`fetchone`), which breaks the next query.
- Decision: queries are materialized with `to_arrow_table()`, every field made nullable (and
  cast to the declared schema where there is one) and written with `pyarrow`. Single row reads
  go through `db.one()`, which uses `fetchall`.
- Consequence: identical schema in every partition (verified on real data). Months are small
  enough (about 600k rows) to fit in memory on a runner.

## 0019 Silver and gold are rebuilt for the whole window on every run

- Status: accepted
- Decision: every month of the run window that has bronze is rebuilt in silver and gold, even
  if bronze did not change. Gold for month M reads silver M-1 for the previous quota and PTAX.
  The registry is rebuilt from the latest snapshot every run.
- Consequence: about 4 seconds per month, no change tracking between layers, and a rerun gives
  the same result. A registry change only reaches gold months inside the window; a full
  history refresh is a backfill run.

## 0020 Quality gate

- Status: accepted
- Decision: checks run on the written files. Blocking: schema, unique key, parseable values,
  row count drop vs the last accepted run (20%), share of negative equity or quota <= 0 (5%),
  registry key and encoding, PTAX unique date and rates, gold AUM equal to silver AUM.
  Warnings: rows dropped as out of month or duplicates, peer coverage below 95%. Any blocking
  failure exits with code 1 and the manifest keeps the previous silver row counts.
- Consequence: thresholds come from real data: the invalid share is 0.2% to 0.35% in recent
  months and 2.1% in 2005-01 (mostly quota = 0).

## 0021 The latest dates of a month are incomplete

- Status: accepted (to be handled in Phases 4 and 7)
- Context: on 2026-10-29 the file had 25,097 classes for 10-23, 23,770 for 10-27 and 28 for
  10-28. Funds report with a lag.
- Decision: gold keeps every date as reported, with `n_classes` per row. The reporting layer
  must flag recent dates with a low class count, and the anomaly layer must not treat a report
  that has not arrived yet as a gap.

## 0022 Environment guard

- Status: accepted
- Decision: before any download, the `hf` target checks:
  - `HF_OWNER` and `HF_TOKEN` are set. The repo id is built from `config/pipeline.yml`
    (`{owner}/cvm-fundos-lake-dev` or `{owner}/cvm-fundos-lake`), so no repo name lives in a
    secret or in code.
  - Dev targets a repo ending in `-dev`, prod a repo that does not.
  - In GitHub Actions, the job's Environment declares `CVM_LAKE_ENV` and it must equal
    `--env`; prod also requires `GITHUB_REF = refs/heads/main`.
  - Remote: the repo exists for this token, dev is private and prod is public.
- Consequence: a prod token in the dev job, a dev run pointed at prod or a prod repo made
  private all stop the run with exit code 2, before any work. Fine grained tokens (one per
  repo) are the last line of defence.

## 0023 One atomic commit per run chunk

- Status: accepted
- Decision:
  - A run reads the repo head once, downloads every input at that revision and commits with
    `parent_commit` set to it. If anything else committed meanwhile, the commit fails and
    nothing is half written. `concurrency: etl-<env>` in the workflow prevents that in the
    first place.
  - Inputs downloaded: `_manifest.json`, bronze of the window months (to rebuild silver when the
    source did not change), silver of the month before the window, the latest registry
    snapshot. Nothing else.
  - The commit holds the files written by the run, the manifest and the dataset card. Files
    whose bytes did not change are skipped by `huggingface_hub`.
  - A backfill is split into chunks of 12 months, one commit each, so a commit stays under 95
    files. A chunk that fails quality stops the backfill; earlier chunks stay published, each
    one complete.
- Consequence: a daily run is one commit of up to about 20 files. A full history backfill
  (2000 to today) is about 27 commits of up to about 80 files each.

## 0024 Commit sha in the manifest

- Status: accepted
- Context: a commit cannot contain its own sha.
- Decision: commits are titled `run <run_id>`. At the start of the next run, entries written
  by the run named in the head commit get that head sha as `commit_sha`.
- Consequence: Phase 5 can read the previous revision of a partition with
  `hf://datasets/<repo>@<commit_sha>/...`.

## 0025 Approval for manual prod runs

- Status: accepted
- Context: required reviewers on an Environment apply to every job that uses it, scheduled ones
  included. Putting them on `prod` would block the daily run until someone clicks approve.
- Decision: `prod` (main only, holds the token) has no reviewers. A manual prod run that is not
  a dry run first passes a job bound to a separate `prod-approval` environment that has
  required reviewers and no secrets.
- Consequence: the schedule runs unattended; a human approves every manual write to prod.

## 0026 Keepalive without commits

- Status: accepted (verify after the first 60 days)
- Context: GitHub disables scheduled workflows of public repos after 60 days without activity.
  A dummy commit to `main` would be blocked by branch protection.
- Decision: a monthly workflow calls the "enable workflow" API for `etl.yml` and itself, with
  `actions: write` only. This is the approach of the widely used keepalive actions.
- Consequence: no bot commits. If GitHub still disables the schedule, fall back to a commit
  to an unprotected branch.

## 0027 Dry run output

- Status: accepted
- Decision: a dry run reads the real repo, builds everything and uploads the local lake as an
  Actions artifact kept for 7 days. Manual dispatches default to dry run.
- Consequence: artifacts of a public GitHub repo can be downloaded by any signed in user. That
  includes dev data, which is acceptable because every byte comes from public sources.

## 0028 Anomaly detection v1

- Status: accepted
- Decision: rules plus three robust statistical layers with fixed thresholds in
  `config/anomalies.yml`, written to `gold/anomalies` per month. Details and the calibration
  history in `docs/anomaly_detection.md`. The main choices:
  - Rolling median and MAD over the previous 63 reports, never the current one.
  - Layer 4 scales the excess return by the fund's own excess history, not by the
    cross-sectional dispersion (which flagged 2% of all reports in heterogeneous peer groups).
  - Market gate: layers 2 and 4 are silent when the typical z of the peer group, ANBIMA class
    or the whole market is at least 2.0 on the date or the one before (lagged valuation).
  - The reversal check reads the next report; the last days of a month are re-evaluated when the
    next month arrives, since the window always recomputes M-1.
  - Decimal shift works from the second report; the statistical layers need 30 prior reports.
- Consequence: about 0.5% of reports flagged, about 0.1% high, and almost nothing on crash days
  (2020-03 and 2026-10-26). March 2020 was used for calibration, so it is in-sample.

## 0029 Anomaly output schema

- Status: accepted
- Decision: the columns asked for (`fund_id`, `class_id`, `dt`, `anomaly_type`, `score`,
  `severity`, `features`, `model_version`, `first_seen_run`) plus `peer_group`. `fund_id` is
  the silver `cnpj` (fund before 175, class after), `class_id` the silver `subclass_id`.
  `features` is a JSON string, not a struct, so adding a feature never changes the Parquet
  schema. Persistent conditions are reported once, on the first report of the streak.
  `first_seen_run` comes from the previous version of the partition.
- Consequence: the anomaly table stays small and stable; alerting on "new" is a filter.

## 0030 Alerts

- Status: accepted
- Decision: new high severity rows, at most 10 days old, at most 20 per date. Only prod posts
  them, as a comment on one GitHub Issue per month, from the ETL job with `issues: write`.
  Dry runs and failed runs post nothing.
- Consequence: a backfill or the 13 month Saturday rerun cannot flood the Issue; old findings
  are in the dataset, not in notifications.
