"""Daily totals of the last 120 days, with the completeness flag (recent dates are partial)."""

from _reporting import emit_csv

# Holidays: a few funds still report, which would draw as a collapse. Dates with less than
# half of the median class count are dropped, except the newest ones (still filling in).
emit_csv("""
    WITH d AS (
        SELECT dt, any_value(complete) AS complete, sum(n_classes) AS n_classes,
               sum(aum) FILTER (WHERE NOT coalesce(is_fic, false)) AS aum_excl_fic,
               sum(net_flow) FILTER (WHERE NOT coalesce(is_fic, false)) AS net_flow_excl_fic
        FROM aum_flows_daily
        WHERE dt >= (SELECT max(dt) FROM aum_flows_daily) - INTERVAL 120 DAY
        GROUP BY dt
    )
    SELECT * FROM d
    WHERE n_classes >= 0.5 * (SELECT median(n_classes) FROM d)
       OR dt >= (SELECT max(dt) FROM d) - INTERVAL 5 DAY
    ORDER BY dt
""")
