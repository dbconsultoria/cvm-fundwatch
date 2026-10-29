"""Dataset card (README.md of the HF dataset repo).

The ``configs`` block makes the HF dataset viewer and SQL console show the main tables by name.
The text has no timestamps, so an unchanged card is not re-uploaded.
"""

from __future__ import annotations

CONFIGS = (
    ("daily_flows", "gold/daily_flows/*/*.parquet", "AUM and flows per day, peer group, FIC flag"),
    (
        "returns_brl_usd",
        "gold/returns_brl_usd/*/*.parquet",
        "daily return per class in BRL and USD",
    ),
    ("anomalies", "gold/anomalies/*/*.parquet", "anomalies found in the daily reports"),
    ("restatements", "gold/restatements/*/*.parquet", "values CVM changed after publishing"),
    ("fund_daily", "silver/fund_daily/*/*.parquet", "typed daily report per class or subclass"),
    ("fund_registry", "silver/fund_registry/*.parquet", "peer group intervals per CNPJ"),
    ("ptax_daily", "silver/ptax_daily/*/*.parquet", "USD/BRL PTAX per day"),
)


def render_card(env_name: str, repo_id: str, code_url: str | None = None) -> str:
    configs = []
    for i, (name, files, _) in enumerate(CONFIGS):
        configs.append(f'- config_name: {name}\n  data_files: "{files}"')
        if i == 0:
            configs[-1] += "\n  default: true"
    table_rows = "\n".join(f"| `{name}` | `{files}` | {desc} |" for name, files, desc in CONFIGS)
    title = "cvm-fundwatch lake" + (" (dev)" if env_name == "dev" else "")
    code = f"Code: {code_url}\n\n" if code_url else ""
    return f"""---
license: other
license_name: source-terms
license_link: https://dados.cvm.gov.br/
pretty_name: {title}
tags:
- finance
- brazil
- investment-funds
configs:
{chr(10).join(configs)}
---

# {title}

Brazilian investment fund data as a bronze/silver/gold lake of Parquet files, built daily by a
GitHub Actions pipeline with DuckDB.

{code}Sources: CVM open data portal (Informe Diario de Fundos de Investimento, fund registry) and
the Banco Central do Brasil PTAX API. The data is republished as is; check the terms of the
original sources before reuse.

## Tables

| Config | Files | Content |
|---|---|---|
{table_rows}

Partitioned tables use a hive style `ano_mes=YYYYMM` folder. With DuckDB:

```sql
SELECT * FROM read_parquet('hf://datasets/{repo_id}/gold/daily_flows/*/*.parquet',
                           hive_partitioning = true) LIMIT 10;
```

`_manifest.json` records, for every source partition, the source `Last-Modified`, SHA-256, row
count and the run that wrote it.
"""
