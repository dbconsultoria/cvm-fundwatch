---
title: About the data
---

# About the data

```js
const meta = FileAttachment("data/meta.json").json();
```

## Sources

- **CVM Informe Diário**: daily quota, net equity, subscriptions, redemptions and shareholders
  of every investment fund class (and subclass) registered with CVM.
- **CVM fund registry**: the class of each fund, used as its peer group.
- **BCB PTAX**: the official USD/BRL rate, for returns in dollars.

All of it is public data, downloaded daily by a GitHub Actions pipeline, stored as Parquet on
Hugging Face and summarized here. Nothing in between is a server or a database.

## Read with care

- **Funds of funds** invest in other funds; summing their assets with everything else counts
  the same money twice. The charts show them separately or leave them out.
- **The newest dates are incomplete.** Funds report with a lag of a few days.
- **Anomalies are leads, not verdicts.** A finding means a report looks wrong compared with the
  fund's own history, its peers and its accounting; the source may still be right.
- **Returns** are median returns of the funds of a peer group, not of an index or a portfolio.

## The data behind this page

```js
const repo = meta.repo_id;
```

${repo
  ? html`<p>Dataset: <a href="https://huggingface.co/datasets/${repo}">huggingface.co/datasets/${repo}</a>.
  Every table can be queried there in the dataset viewer and the SQL console, or from DuckDB:</p>
  <pre><code>SELECT * FROM read_parquet('hf://datasets/${repo}/gold/daily_flows/*/*.parquet',
                           hive_partitioning = true)
LIMIT 10;</code></pre>
  <iframe src="https://huggingface.co/datasets/${repo}/embed/viewer/daily_flows/train"
    frameborder="0" width="100%" height="560px" title="Dataset viewer"></iframe>`
  : html`<p class="muted">Built from local data.</p>`}

Methodology, architecture and decisions:
[github.com/dbconsultoria/cvm-fundwatch](https://github.com/dbconsultoria/cvm-fundwatch).
