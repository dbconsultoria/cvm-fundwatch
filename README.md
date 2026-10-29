# cvm-fundwatch

A zero cost data pipeline for Brazilian investment fund data. It runs entirely on GitHub Actions,
builds a bronze/silver/gold lake of Parquet files on Hugging Face datasets, detects anomalies in
the daily fund reports, and publishes a static dashboard on GitHub Pages.

> Status: Phase 1 (bronze extraction, local target). Silver, gold, publishing and anomaly
> detection come in later phases.

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
```

The run is incremental. `data/lake/_manifest.json` stores `Last-Modified`, `ETag` and a SHA-256
per source partition, so unchanged months are skipped without downloading. A source that is
down is skipped with a warning; a source whose file format changed fails the run.

Query the result with DuckDB:

```sql
SELECT ano_mes, _layout, count(*)
FROM read_parquet('data/lake/bronze/cvm_inf_diario/*/*.parquet',
                  hive_partitioning = true, union_by_name = true)
GROUP BY ALL ORDER BY 1;
```

## License

MIT, see [LICENSE](LICENSE).

## Docs

- [docs/sources.md](docs/sources.md): what the source files really look like
- [docs/decisions.md](docs/decisions.md): architecture decision records
