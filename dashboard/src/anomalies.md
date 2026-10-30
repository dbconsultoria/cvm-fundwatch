---
title: Anomalies
---

# Anomalies in the daily reports

Wrong decimal places, impossible values, equity that does not add up, stale or missing reports.
Market wide moves are not flagged. How it works:
[anomaly detection](https://github.com/dbconsultoria/cvm-fundwatch/blob/main/docs/anomaly_detection.md).

```js
const weekly = FileAttachment("data/anomalies_weekly.csv").csv({typed: true});
const recent = FileAttachment("data/anomalies_recent.csv").csv({typed: true});
const restated = FileAttachment("data/restatements.csv").csv({typed: true});
```

```js
const order = ["high", "medium", "low"];
// Monthly, not weekly: 26 years of weekly bars are thinner than a pixel and vanish.
const bySeverity = d3.rollups(weekly, (v) => d3.sum(v, (d) => d.n), (d) => +d3.utcMonth(d.week), (d) => d.severity)
  .flatMap(([month, rows]) => rows.map(([severity, n]) => ({month: new Date(month), severity, n})));
```

<div class="card">

## Findings per month

```js
display(Plot.plot({
  width,
  height: 300,
  y: {grid: true, label: "findings"},
  color: {domain: order, range: ["#d62728", "#ff9f0e", "#9aa0a6"], legend: true},
  marks: [Plot.rectY(bySeverity, {x: "month", interval: "month", y: "n", fill: "severity", order, inset: 0, tip: true}), Plot.ruleY([0])]
}));
```

</div>

<div class="card">

## Findings by type, last 52 weeks

```js
const lastWeek = d3.max(weekly, (d) => d.week);
const year = weekly.filter((d) => lastWeek && d.week > d3.utcWeek.offset(lastWeek, -52));
display(Plot.plot({
  width,
  marginLeft: 140,
  x: {grid: true, label: "findings"},
  color: {domain: order, range: ["#d62728", "#ff9f0e", "#9aa0a6"], legend: true},
  marks: [Plot.barX(year, Plot.groupY({x: "sum"}, {y: "anomaly_type", x: "n", fill: "severity", order, sort: {y: "-x"}, tip: true}))]
}));
```

</div>

<div class="card">

## High and medium findings, last 90 days

```js
const severity = view(Inputs.checkbox(["high", "medium"], {label: "Severity", value: ["high"]}));
```

```js
const search = view(Inputs.search(recent.filter((d) => severity.includes(d.severity)), {placeholder: "CNPJ, name, type"}));
```

```js
const cnpj = (v) => v?.length === 14 ? `${v.slice(0, 2)}.${v.slice(2, 5)}.${v.slice(5, 8)}/${v.slice(8, 12)}-${v.slice(12)}` : v;
display(Inputs.table(search, {
  columns: ["dt", "fund_id", "class_id", "name", "peer_group", "anomaly_type", "severity", "score"],
  header: {dt: "Date", fund_id: "CNPJ", class_id: "Subclass", name: "Name", peer_group: "Peer group", anomaly_type: "Type", severity: "Severity", score: "Score"},
  format: {fund_id: cnpj, dt: d3.utcFormat("%Y-%m-%d")},
  rows: 20
}));
```

</div>

<div class="card">

## Corrections published by CVM

Reports whose values changed after first publication, by reference month, and how many of them
the detector had flagged before the correction (a weak label for its precision).

```js
const corrections = restated.flatMap((d) => [
  {month: d.month, what: "flagged before", n: d.flagged_before},
  {month: d.month, what: "not flagged", n: d.changed_reports - d.flagged_before}
]);
display(restated.length ? Plot.plot({
  width,
  height: 260,
  y: {grid: true, label: "corrected reports"},
  color: {legend: true},
  marks: [Plot.rectY(corrections, {x: "month", interval: "month", y: "n", fill: "what", tip: true}), Plot.ruleY([0])]
}) : html`<p class="muted">No corrections recorded yet.</p>`);
```

</div>
