"""Daily totals of the last 120 days, with the completeness flag (recent dates are partial)."""

from _reporting import emit_csv

emit_csv("""
    SELECT dt, any_value(complete) AS complete, sum(n_classes) AS n_classes,
           sum(aum) FILTER (WHERE NOT coalesce(is_fic, false)) AS aum_excl_fic,
           sum(net_flow) FILTER (WHERE NOT coalesce(is_fic, false)) AS net_flow_excl_fic
    FROM aum_flows_daily
    WHERE dt >= (SELECT max(dt) FROM aum_flows_daily) - INTERVAL 120 DAY
    GROUP BY dt
    ORDER BY dt
""")
