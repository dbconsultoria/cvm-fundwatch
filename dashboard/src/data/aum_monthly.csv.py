"""AUM per peer group and FIC flag on the last complete date of each month."""

from _reporting import emit_csv

emit_csv("""
    WITH last AS (
        SELECT strftime(dt, '%Y-%m') AS month, max(dt) AS dt
        FROM aum_flows_daily WHERE complete GROUP BY 1
    )
    SELECT a.dt, l.month, a.peer_group, coalesce(a.is_fic, false) AS is_fic,
           sum(a.aum) AS aum, sum(a.n_classes) AS n_classes
    FROM aum_flows_daily a JOIN last l USING (dt)
    GROUP BY ALL
    ORDER BY a.dt, a.peer_group, is_fic
""")
