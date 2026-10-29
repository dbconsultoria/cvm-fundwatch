---
title: Overview
toc: false
---

# Brazilian investment funds

```js
const meta = FileAttachment("data/meta.json").json();
const aum = FileAttachment("data/aum_monthly.csv").csv({typed: true});
const flows = FileAttachment("data/flows_monthly.csv").csv({typed: true});
const recent = FileAttachment("data/flows_daily_recent.csv").csv({typed: true});
```

```js
const brl = (x) =>
  x == null ? "-" :
  Math.abs(x) >= 1e12 ? `R$ ${(x / 1e12).toFixed(2)} tri` :
  Math.abs(x) >= 1e9 ? `R$ ${(x / 1e9).toFixed(1)} bi` :
  `R$ ${(x / 1e6).toFixed(1)} mi`;
const months = [...new Set(aum.map((d) => d.month))].sort();
const lastMonth = months.at(-1);
const prevMonth = months.at(-2);
const total = (m, fic) => d3.sum(aum.filter((d) => d.month === m && d.is_fic === fic), (d) => d.aum);
const lastFlows = d3.sum(flows.filter((d) => d.month === prevMonth), (d) => d.net_flow);
```

<p class="muted">Daily reports of investment funds registered with CVM, last complete date
${meta.last_complete_date ?? "-"} (data up to ${meta.last_date ?? "-"}).
Built ${meta.built_at}${meta.commit ? html` from commit <code>${meta.commit.slice(0, 8)}</code>` : ""}.</p>

<div class="grid grid-cols-4">
  <div class="card">
    <h2>AUM, excluding funds of funds</h2>
    <span class="big">${brl(total(lastMonth, false))}</span>
  </div>
  <div class="card">
    <h2>AUM held by funds of funds</h2>
    <span class="big">${brl(total(lastMonth, true))}</span>
  </div>
  <div class="card">
    <h2>Change over the previous month</h2>
    <span class="big">${prevMonth ? d3.format("+.1%")(total(lastMonth, false) / total(prevMonth, false) - 1) : "-"}</span>
  </div>
  <div class="card">
    <h2>Net flows in ${prevMonth ?? "-"}</h2>
    <span class="big">${brl(lastFlows)}</span>
  </div>
</div>

<div class="card">

## Assets under management by peer group

Month end values (last complete date of each month), funds of funds excluded to avoid counting
the same money twice.

```js
display(Plot.plot({
  width,
  height: 360,
  y: {grid: true, label: "R$ trillion", transform: (d) => d / 1e12},
  color: {legend: true},
  marks: [
    Plot.areaY(aum.filter((d) => !d.is_fic), {x: "dt", y: "aum", fill: "peer_group", tip: true}),
    Plot.ruleY([0])
  ]
}));
```

</div>

<div class="card">

## Net flows per month

Subscriptions minus redemptions, funds of funds excluded.

```js
display(Plot.plot({
  width,
  height: 320,
  y: {grid: true, label: "R$ billion", transform: (d) => d / 1e9},
  color: {legend: true},
  marks: [
    Plot.rectY(flows, {x: (d) => new Date(d.month + "-01"), interval: "month", y: "net_flow",
                       fill: "peer_group", tip: true}),
    Plot.ruleY([0])
  ]
}));
```

</div>

<div class="card">

## The last 120 days

Funds report with a lag: the newest dates are incomplete (hollow points) and fill in over the
next days.

```js
display(Plot.plot({
  width,
  height: 260,
  y: {grid: true, label: "classes reporting"},
  marks: [
    Plot.lineY(recent, {x: "dt", y: "n_classes", stroke: "var(--theme-foreground-muted)"}),
    Plot.dot(recent, {x: "dt", y: "n_classes", fill: (d) => d.complete ? "currentColor" : "none",
                      stroke: "currentColor", tip: true})
  ]
}));
```

</div>
