"""High and medium anomalies of the last 90 days, newest first."""

from _reporting import emit_csv

emit_csv("""
    SELECT dt, fund_id, class_id, name, peer_group, anomaly_type, severity,
           round(score, 2) AS score, first_seen_run
    FROM anomalies_recent
    ORDER BY dt DESC, severity, score DESC
""")
