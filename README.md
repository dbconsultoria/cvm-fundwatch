# cvm-fundwatch

A zero cost data pipeline for Brazilian investment fund data. It runs entirely on GitHub Actions,
builds a bronze/silver/gold lake of Parquet files on Hugging Face datasets, detects anomalies in
the daily fund reports, and publishes a static dashboard on GitHub Pages.

> Status: Phase 2 (bronze, silver, gold and quality checks, local target). Publishing to
> Hugging Face, anomaly detection and the dashboard come in later phases.

## Sources

| Source | What | Docs |
|---|---|---|
| CVM Informe Diário | Daily quota, equity, flows, shareholders per fund/class/subclass | [docs/sources.md](docs/sources.md#cvm-informe-diario) |
| CVM registry | Fund type and class, used as the peer group | [docs/sources.md](docs/sources.md#cvm-registry) |
| BCB PTAX | Official USD/BRL rate | [docs/sources.md](docs/sources.md#bcb-ptax) |

## Architecture

TODO (Phase 8): diagram. Short version:

- Compute: DuckDB inside GitHub hosted runners.
- Storage: Hugging Face dataset repos (dev private, prod public). One commit per run.
- Orchestration: GitHub Actions schedules. Branch `development` maps to dev, `main` to prod.
- Dashboard: Observable Framework on GitHub Pages.

## Lake layout

| Table | Partition | Grain | Notes |
|---|---|---|---|
| `bronze/cvm_inf_diario` | `ano_mes` | source row | all columns as strings + source metadata |
| `bronze/cvm_registry/<table>` | `snapshot_date` | source row | 4 registry tables |
| `bronze/bcb_ptax` | `ano_mes` | source row | |
| `silver/fund_daily` | `ano_mes` | class or subclass per day | one typed schema for the three CVM layouts |
| `silver/fund_registry` | none | class interval | peer group over time |
| `silver/ptax_daily` | `ano_mes` | day | latest quote per date |
| `gold/daily_flows` | `ano_mes` | day x peer group x FIC flag | AUM, subscriptions, redemptions, net flow |
| `gold/returns_brl_usd` | `ano_mes` | class or subclass per day | daily return in BRL and USD |

Every run reprocesses a window of months (default: current and previous) and checks the result
before it counts as done. See [docs/decisions.md](docs/decisions.md) for the reasons.

## Run locally

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run pytest
uv run ruff check .
```

Build the bronze layer into `data/lake` (git ignored):

```bash
# current month and the previous one (default window from config/pipeline.yml)
uv run cvm-lake run --env dev --target local

# backfill a range; months before 2021 come from the CVM HIST yearly archives
uv run cvm-lake run --env dev --months 202301-202312

# one source only, ignoring the manifest
uv run cvm-lake run --env dev --sources bcb_ptax --months 202003 --force

# rebuild silver and gold from the local bronze, no downloads
uv run cvm-lake run --env dev --months 202003 --no-extract
```

The run prints a report per layer and exits with code 1 when a blocking quality check fails.

The run is incremental. `data/lake/_manifest.json` stores `Last-Modified`, `ETag` and a SHA-256
per source partition, so unchanged months are skipped without downloading. A source that is
down is skipped with a warning; a source whose file format changed fails the run.

Query the result with DuckDB:

```sql
-- median daily return of equity funds during the March 2020 crash, in BRL and USD
SELECT dt, median(ret_brl) AS brl, median(ret_usd) AS usd
FROM read_parquet('data/lake/gold/returns_brl_usd/*/*.parquet', hive_partitioning = true)
WHERE peer_group = 'Ações' AND dt BETWEEN '2020-03-09' AND '2020-03-13'
GROUP BY dt ORDER BY dt;
```

## License

MIT, see [LICENSE](LICENSE).

## Docs

- [docs/sources.md](docs/sources.md): what the source files really look like
- [docs/decisions.md](docs/decisions.md): architecture decision records
