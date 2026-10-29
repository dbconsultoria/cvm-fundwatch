---
title: Returns in BRL and USD
---

# Returns in BRL and USD

Median monthly return of the funds of each peer group, in reais and converted to dollars with
the BCB PTAX rate. A fund counts in a month when it has at least 15 daily returns.

```js
const returns = FileAttachment("data/returns_monthly.csv").csv({typed: true});
const ptax = FileAttachment("data/ptax.csv").csv({typed: true});
```

```js
const peers = [...new Set(returns.map((d) => d.peer_group))].sort();
const peer = view(Inputs.select(peers, {label: "Peer group", value: peers.includes("Ações") ? "Ações" : peers[0]}));
```

```js
const series = returns.filter((d) => d.peer_group === peer);
let brl = 1, usd = 1;
const index = series.map((d) => ({
  month: d.month,
  BRL: (brl *= 1 + d.median_ret_brl),
  USD: (usd *= 1 + (d.median_ret_usd ?? 0))
})).flatMap((d) => [{month: d.month, currency: "BRL", value: d.BRL}, {month: d.month, currency: "USD", value: d.USD}]);
```

<div class="card">

## Cumulative median return, ${peer}

Chained median monthly returns, starting at 1.

```js
display(Plot.plot({
  width,
  height: 320,
  y: {grid: true, label: "index"},
  color: {legend: true},
  marks: [Plot.ruleY([1]), Plot.lineY(index, {x: "month", y: "value", stroke: "currency", tip: true})]
}));
```

</div>

<div class="card">

## Monthly median return and interquartile range, ${peer}

```js
display(Plot.plot({
  width,
  height: 300,
  y: {grid: true, label: "return", tickFormat: "+.0%"},
  marks: [
    Plot.ruleY([0]),
    Plot.areaY(series, {x: "month", y1: "p25_ret_brl", y2: "p75_ret_brl", fillOpacity: 0.2}),
    Plot.lineY(series, {x: "month", y: "median_ret_brl", tip: true}),
    Plot.lineY(series, {x: "month", y: "median_ret_usd", strokeDasharray: "4,3"})
  ]
}));
```

Solid: BRL. Dashed: USD. Band: 25th to 75th percentile in BRL.

</div>

<div class="card">

## USD/BRL PTAX

```js
display(Plot.plot({
  width,
  height: 240,
  y: {grid: true, label: "BRL per USD"},
  marks: [Plot.lineY(ptax, {x: "dt", y: "sell", tip: true})]
}));
```

</div>
