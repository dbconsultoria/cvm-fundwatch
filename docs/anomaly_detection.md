# Anomaly detection

Version `v1-rules-robust` (config: `config/anomalies.yml`, code: `src/cvm_lake/anomalies/`).
Rules and three statistical layers with fixed thresholds. Phase 5 adds the evaluation harness
(synthetic injection, restatements as weak labels), Phase 6 the Isolation Forest.

## Goal

Find errors in the daily fund reports (wrong decimal place, impossible values, equity that does
not add up, stale or missing reports) **without flagging market wide moves**, which are real and
affect many funds at once.

## Input

For a scored month M: silver `fund_daily` of M, of the 3 months before it (rolling history) and
of M+1 when it exists (only used to see whether the last days of M revert). Each report gets its
peer group (CVM class, ADR 0017) and ANBIMA class from the registry.

All features are per report, i.e. per (CNPJ, subclass, date), ordered by date. "Previous" means
the previous report of the same fund, not the previous calendar day.

| Feature | Definition |
|---|---|
| `log_ret` | ln(quota_t / quota_{t-1}) |
| `residual` | (equity_t - (equity_{t-1} x quota_t / quota_{t-1} + subscriptions_t - redemptions_t)) / equity_{t-1} |
| `excess_ret` | log_ret minus the median log_ret of the peer group on the same date (at least 10 peers) |
| `z_return`, `z_excess`, `z_residual` | (x - median) / (1.4826 x max(MAD, floor)) over the previous 63 reports of the fund, **excluding the current one** |
| `quota_ratio`, `equity_ratio` | value_t / value_{t-1} |
| `gap_market_days` | market days strictly between the previous report and this one; a market day has at least half of the maximum daily report count |
| `unchanged_reports` | consecutive previous reports with the same quota |
| `market_z` | the largest typical |z_return| (median over funds) of the fund's peer group, ANBIMA class or all funds, on this date or the one before |
| `reverted` | the next report undoes at least 75% of this log return |

The MAD floor depends on the peer group (`mad_floor`): money market funds are so smooth that
their MAD is close to zero and any tiny move would look like an outlier.

## Layers

1. **Rules**: impossible values (equity < 0, quota <= 0 with positive equity, negative flows or
   shareholders), missing quota or equity, duplicate keys in the source file, reporting gaps,
   stale quota, fund closing.
2. **Own history** (`l2`): |z_return| > 8.
3. **Accounting identity** (`l3`): |residual| >= 2% and |z_residual| > 8. Settlement timing makes
   the residual noisy, so the threshold is relative to the fund's own residual history.
4. **Peer context** (`l4`): |z_excess| > 8. The excess return is scaled by the fund's own history
   of excess returns: a fund that always tracks its peers loosely is not an outlier.

Decimal shift: quota or equity ratio within 2.3% of 10^k, k in ±1..±3. The report after a shift
that undoes it is an `echo`.

### Market moves

Layers 2 and 4 are silent when `market_z >= 2.0`: the typical fund of the peer group, of the
ANBIMA class or of the whole market moved by more than two robust standard deviations, on the
date or on the one before. The previous date matters because funds of funds and offshore funds
price with a one day lag and show the move a report later. Layer 3 and the rules still run: a
crash does not break the accounting identity, an error does.

## Anomaly types and severity

| Type | Condition | Severity |
|---|---|---|
| `decimal_shift` | ratio near 10^k, not an echo | high |
| `impossible_value` | first report of an impossible streak | high |
| `return_outlier` | l2 or l4, not a decimal shift, not a distribution | high if two of l2, l3, l4 agree, else medium |
| `equity_residual` | l3, not a structural event | high if two layers agree, else medium |
| `structural_event` | l3 and shareholders x5 or /5 (merger, incorporation), no return outlier | medium |
| `shareholder_spike` | shareholders x5 or /5 and at least 100 more or fewer, no l3 | medium |
| `missing_value` | first report with a null quota or equity | medium |
| `duplicate_report` | key reported more than once in the source file | medium |
| `stale_quota` | 5th consecutive report with an unchanged quota, equity > 0 | medium |
| `reporting_gap` | missing market days between two reports | medium from 5 days, else low |
| `distribution_event` | negative return outlier that the equity follows (residual < 0.2%) and that the next report does not revert: amortization, income distribution, performance fee | low |
| `decimal_shift_echo` | the report after a decimal shift that undoes it | low |
| `fund_closing` | equity goes to zero | low |

Persistent conditions (impossible values, missing values) are reported on the first report of
the streak only. Funds with fewer than 30 prior reports get rules and the decimal shift check
only; the statistical layers need a history.

## Output: `gold/anomalies/ano_mes=YYYYMM/part-0.parquet`

| Column | Content |
|---|---|
| `fund_id` | CNPJ, 14 characters: the fund before Resolution 175, the class after it |
| `class_id` | subclass id, `''` when the class has no subclasses |
| `dt` | reference date of the report |
| `anomaly_type`, `severity`, `score` | see above; score is the |z|, |log10 ratio|, gap length, ... of the type |
| `peer_group` | peer group used |
| `features` | JSON with every feature of the report |
| `model_version` | `model_version` of `config/anomalies.yml` |
| `first_seen_run` | run that first found this (fund, class, date, type); kept across reruns |

## Alerts

New (`first_seen_run` = this run) high severity rows, dated at most 10 days ago, at most 20 per
date by score. In prod, the ETL adds them as a comment to the GitHub Issue
`[prod] High severity anomalies YYYY-MM` (created when missing).

## Observed on real data (v1, before Phase 5)

| Month | Reports | Anomalies | High | Note |
|---|---:|---:|---:|---|
| 2020-01 | 369,894 | 942 | 265 | |
| 2020-02 | 305,860 | 863 | 191 | |
| 2020-03 | 378,066 | 917 | 171 | COVID crash: 0 or 1 high per day from 03-09 to 03-19 |
| 2026-09 | 538,336 | 2,744 | 710 | 91 high on 09-30 (quarter end) |
| 2026-10 | 458,809 | 2,263 | 492 | crash of 10-26 (equity funds -16.9% median): 1 high per day on 10-26 and 10-27 |

Calibration history, so the evaluation stays honest:

1. Layer 4 first used the cross-sectional MAD of the peer group: about 9,700 flags a month,
   mostly long duration fixed income funds inside the heterogeneous "Renda Fixa" group. Now the
   excess return is scaled by the fund's own history.
2. The day after the 2026-10-26 crash produced 864 high flags from lagged valuation funds. The
   market gate now covers the previous date and ANBIMA classes.
3. With `market_z = 3.0` the credit shock of 2020-03-20/23 produced 51 and 77 high flags; 2.0
   removes them without changing calm months.

Because of step 3, **March 2020 is in-sample for v1**. Phase 5 must use other stress periods as
holdout for the claim "market wide moves are not flagged".

## Evaluation (Phase 5)

There are no labels, so the detector is evaluated two ways (ADRs 0031 and 0032).

### Synthetic injection

`src/cvm_lake/anomalies/injection.py` writes errors into a copy of silver before the features
are computed, with a fixed seed, one error per fund:

| Kind | Error | Counts as the right type |
|---|---|---|
| `decimal_shift` | quota or equity x10, x100, x1000 or the inverse, one report | `decimal_shift` |
| `quota_glitch` | quota x(1 +/- 2% to 20%), equity unchanged | `return_outlier`, `equity_residual` |
| `equity_jump` | equity x(1 +/- 5% to 50%), quota unchanged | `equity_residual`, `structural_event` |
| `stale_quota` | quota repeated for 6 reports | `stale_quota` |
| `negative_equity` | equity sign flipped | `impossible_value` |
| `missing_quota` | quota null | `missing_value` |
| `shareholder_spike` | shareholders x10 | `shareholder_spike`, `structural_event` |

The report right after an error is an echo and is not scored. Metrics per variant:

- recall per kind at low, medium and high severity (and the share detected with the right type)
- precision lower bound: flagged reports that are injected errors, over all flagged reports;
  real anomalies in the clean data count as false positives, so the true precision is higher
- clean flags per 10,000 reports: the variant's alarm rate without injection

`config/experiments/v1_baselines.yml` compares, on the same injected errors:

| Variant | Layers |
|---|---|
| `rules_only` | rules and decimal shift |
| `classic_z` | + layer 2 with mean and standard deviation, no floor, no market gate |
| `robust_z` | + layer 2 with median and MAD, floor and market gate |
| `robust_z_residual` | + layer 3 |
| `v1_full` | + layer 4 |

Run it on a local lake that has silver for the scored months, the 3 months before them and the
month after (if published):

```bash
uv run cvm-lake evaluate --lake data/lake --months 202609,202610 \
  --experiment config/experiments/v1_baselines.yml --out experiments/results
```

One JSON and one Markdown file per month land in `experiments/results/`, with the seed, config
hashes, silver source hashes, git commit and package versions. A month takes 5 to 30 minutes on
a laptop (10 detector runs over 4 to 5 months of data), so run it once the full history is in
the prod lake, not on every change.

Suggested months: a calm month (for example 2026-09), March 2020 (in-sample for v1, see the
calibration history above) and holdout stress periods: 2026-10 (crash of 10-26) and earlier
episodes once the history is backfilled.

### CI floors

`tests/test_evaluation.py` runs the harness on a seeded synthetic panel in every CI run and
fails below these recalls:

| Kind | Severity | Floor |
|---|---|---|
| `decimal_shift`, `negative_equity` | high | 0.95 |
| `missing_quota` | medium | 0.95 |
| `quota_glitch`, `equity_jump`, `stale_quota`, `shareholder_spike` | medium | 0.90 |

It also checks decimal shift recall on a crash day (layers 2 and 4 silent), precision 1.0 on
the clean panel, and that the `rules_only` baseline misses quota glitches.

### Restatements as weak labels

Every run compares the new silver of each window month with the previous version and appends
the changes to `gold/restatements`, with `flagged_before` (the anomaly types the old version
had). Once a few months have accumulated:

```sql
-- share of corrected reports that the detector had flagged before the correction
SELECT count(DISTINCT (fund_id, class_id, dt)) FILTER (WHERE flagged_before <> '')
       / count(DISTINCT (fund_id, class_id, dt)) AS flagged_share
FROM read_parquet('hf://datasets/<repo>/gold/restatements/*/*.parquet')
WHERE field IN ('quota', 'equity') AND abs(rel_change) > 0.01;
```
