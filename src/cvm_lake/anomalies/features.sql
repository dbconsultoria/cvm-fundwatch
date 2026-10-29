-- Per report features for anomaly detection (ADR 0028).
--
-- Input temp tables:
--   base(cnpj, subclass_id, dt, quota, equity, subscriptions, redemptions, shareholders,
--        peer_group, peer_source, anbima_class)  numbers as DOUBLE, the scored month plus history months
--   mad_floors(peer_group, mad_floor)  with a row for peer_group = 'default'
-- Parameters (a dollar sign plus a name) are filled from config/anomalies.yml by detector.py.
--
-- Rolling statistics use the previous `window` reports of the same fund and never the current
-- one, so an error cannot hide itself by inflating its own MAD.

CREATE OR REPLACE TEMP TABLE features AS
WITH
market AS (
    SELECT dt, count(*) AS n_reports FROM base GROUP BY dt
),
market_days AS (
    -- cumulative count of market days up to and including dt
    SELECT dt, row_number() OVER (ORDER BY dt) AS md_index
    FROM market
    WHERE n_reports >= $market_day_share * (SELECT max(n_reports) FROM market)
),
seq AS (
    SELECT b.*,
           lag(dt) OVER k AS prev_dt,
           lag(quota) OVER k AS prev_quota,
           lag(equity) OVER k AS prev_equity,
           lag(shareholders) OVER k AS prev_shareholders,
           row_number() OVER k - 1 AS n_prior,
           coalesce(f.mad_floor, d.mad_floor) AS mad_floor
    FROM base b
    LEFT JOIN mad_floors f ON f.peer_group = b.peer_group
    CROSS JOIN (SELECT mad_floor FROM mad_floors WHERE peer_group = 'default') d
    WINDOW k AS (PARTITION BY cnpj, subclass_id ORDER BY dt)
),
derived AS (
    SELECT seq.*,
           CASE WHEN quota > 0 AND prev_quota > 0 THEN ln(quota / prev_quota) END AS log_ret,
           CASE WHEN quota > 0 AND prev_quota > 0 THEN quota / prev_quota END AS quota_ratio,
           CASE WHEN equity > 0 AND prev_equity > 0 THEN equity / prev_equity END AS equity_ratio,
           CASE WHEN prev_equity > 0 AND prev_quota > 0 AND quota IS NOT NULL
                     AND equity IS NOT NULL
                THEN (equity - (prev_equity * quota / prev_quota
                                + coalesce(subscriptions, 0) - coalesce(redemptions, 0)))
                     / prev_equity END AS residual,
           CASE WHEN prev_shareholders > 0 THEN shareholders / prev_shareholders END
               AS shareholder_ratio,
           (quota IS DISTINCT FROM prev_quota)::INTEGER AS quota_changed
    FROM seq
),
gaps AS (
    SELECT d.*,
           -- market days strictly between the previous report and this one
           CASE WHEN d.prev_dt IS NOT NULL THEN
               coalesce(mc.md_index, 0)
               - CASE WHEN mx.dt = d.dt THEN 1 ELSE 0 END
               - coalesce(mp.md_index, 0) END AS gap_market_days
    FROM derived d
    ASOF LEFT JOIN market_days mc ON d.dt >= mc.dt
    ASOF LEFT JOIN market_days mp ON d.prev_dt >= mp.dt
    LEFT JOIN market_days mx ON mx.dt = d.dt
),
streaks AS (
    SELECT *,
           sum(quota_changed) OVER (PARTITION BY cnpj, subclass_id ORDER BY dt
                                    ROWS UNBOUNDED PRECEDING) AS quota_run
    FROM gaps
),
peer AS (
    SELECT peer_group, dt,
           median(log_ret) AS peer_median,
           count(log_ret) AS n_peers
    FROM streaks
    WHERE peer_group <> '$unclassified'
    GROUP BY ALL
),
with_peer AS (
    SELECT s.*, p.peer_median, coalesce(p.n_peers, 0) AS n_peers,
           -- layer 4 input: return in excess of the peer group's median return that day
           CASE WHEN p.n_peers >= $min_peers THEN s.log_ret - p.peer_median END AS excess_ret
    FROM streaks s
    LEFT JOIN peer p USING (peer_group, dt)
),
rolling AS (
    SELECT *,
           row_number() OVER (PARTITION BY cnpj, subclass_id, quota_run ORDER BY dt) - 1
               AS unchanged_reports,
           lead(log_ret) OVER (PARTITION BY cnpj, subclass_id ORDER BY dt) AS next_log_ret,
           median(log_ret) OVER w AS own_median,
           mad(log_ret) OVER w AS own_mad,
           avg(log_ret) OVER w AS own_mean,
           stddev_samp(log_ret) OVER w AS own_std,
           median(excess_ret) OVER w AS excess_median,
           mad(excess_ret) OVER w AS excess_mad,
           median(residual) OVER w AS res_median,
           mad(residual) OVER w AS res_mad
    FROM with_peer
    WINDOW w AS (PARTITION BY cnpj, subclass_id ORDER BY dt
                 ROWS BETWEEN $window PRECEDING AND 1 PRECEDING)
),
scored AS (
    SELECT *,
           -- layer 2: own log return vs own history
           (log_ret - own_median) / (1.4826 * greatest(own_mad, mad_floor)) AS z_return,
           -- classic baseline for experiments: mean and standard deviation, no floor
           (log_ret - own_mean) / nullif(own_std, 0) AS z_return_classic,
           -- layer 4: excess return vs the fund's own history of excess returns, so a fund
           -- that always tracks its peers loosely is not flagged for doing so
           (excess_ret - excess_median) / (1.4826 * greatest(excess_mad, mad_floor))
               AS z_excess,
           -- layer 3: accounting identity residual vs own history
           (residual - res_median) / (1.4826 * greatest(res_mad, $residual_floor))
               AS z_residual
    FROM rolling
),
moves AS (
    -- Typical own-history z on each date, per CVM peer group, per ANBIMA class and over all
    -- funds. A large typical z means the market moved, not the fund.
    SELECT 'peer' AS level, peer_group AS cell, dt, median(z_return) AS z_median
    FROM scored WHERE n_prior >= $min_history GROUP BY ALL HAVING count(*) >= $min_peers
    UNION ALL
    SELECT 'anbima', anbima_class, dt, median(z_return)
    FROM scored WHERE n_prior >= $min_history AND anbima_class IS NOT NULL
    GROUP BY ALL HAVING count(*) >= $min_peers
    UNION ALL
    SELECT 'all', 'all', dt, median(z_return)
    FROM scored WHERE n_prior >= $min_history GROUP BY ALL
),
moves_lagged AS (
    -- the same on the previous date: funds with lagged valuation (funds of funds, offshore
    -- funds) show a market move one report later
    SELECT *, lag(z_median) OVER (PARTITION BY level, cell ORDER BY dt) AS z_median_prev
    FROM moves
)
SELECT s.*,
       greatest(abs(mp.z_median), abs(mp.z_median_prev),
                abs(ma.z_median), abs(ma.z_median_prev),
                abs(mg.z_median), abs(mg.z_median_prev)) AS market_z,
       log10(s.quota_ratio) AS quota_log10,
       log10(s.equity_ratio) AS equity_log10
FROM scored s
LEFT JOIN moves_lagged mp ON mp.level = 'peer' AND mp.cell = s.peer_group AND mp.dt = s.dt
LEFT JOIN moves_lagged ma ON ma.level = 'anbima' AND ma.cell = s.anbima_class AND ma.dt = s.dt
LEFT JOIN moves_lagged mg ON mg.level = 'all' AND mg.dt = s.dt
