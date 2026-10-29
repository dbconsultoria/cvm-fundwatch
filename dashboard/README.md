# Dashboard

Static site built with [Observable Framework](https://observablehq.com/framework/) from the
`gold/reporting` tables of the lake, deployed to GitHub Pages by `.github/workflows/pages.yml`.

Data loaders (`src/data/*.py`) are Python: they run DuckDB over `gold/reporting/*.parquet` at
build time and write small CSV and JSON files. The browser never reads the lake.

## Build locally

From the repository root, with the Python environment synced (`uv sync`):

```bash
# reporting tables: from a local lake, or downloaded from the dataset repo
uv run cvm-lake run --env dev --months 202609-202610   # builds data/lake/gold/reporting
# or: uv run cvm-lake fetch-reporting --env prod --out reporting   (needs HF_OWNER, HF_TOKEN)
# or synthetic tables: uv run python dashboard/scripts/demo_data.py demo

cd dashboard
npm ci
export REPORTING_DIR=../data/lake                 # the folder that contains gold/reporting
export CVM_PYTHON=../.venv/bin/python             # Windows: ../.venv/Scripts/python.exe
npm run dev                                       # preview with live reload
npm run build                                     # static site in dist/
```

`REPORTING_DIR` points at the folder that **contains** `gold/reporting/`. `CVM_PYTHON` is the
interpreter of the project virtualenv, which has `duckdb`, `pyarrow` and `cvm_lake`.

## Pages

| Page | Content |
|---|---|
| Overview | AUM by peer group, monthly net flows, completeness of the last 120 days |
| Returns in BRL and USD | median monthly return per peer group, cumulative, with PTAX |
| Anomalies | findings per week and type, recent high and medium findings, CVM corrections |
| About the data | sources, caveats, links to the dataset and its viewer |
