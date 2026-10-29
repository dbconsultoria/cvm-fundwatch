"""Anomalies per week and severity, and per week and type."""

from _reporting import emit_csv

emit_csv("""
    SELECT date_trunc('week', dt)::DATE AS week, severity, anomaly_type, sum(n) AS n
    FROM anomalies_daily
    GROUP BY ALL
    ORDER BY week, severity, anomaly_type
""")
