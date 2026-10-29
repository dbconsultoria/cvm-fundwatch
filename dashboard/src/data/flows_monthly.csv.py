"""Net flows (subscriptions minus redemptions) per month and peer group, complete dates only."""

from _reporting import emit_csv

emit_csv("""
    SELECT strftime(dt, '%Y-%m') AS month, peer_group,
           sum(subscriptions) AS subscriptions, sum(redemptions) AS redemptions,
           sum(net_flow) AS net_flow
    FROM aum_flows_daily
    WHERE complete AND NOT coalesce(is_fic, false)
    GROUP BY ALL
    ORDER BY month, peer_group
""")
