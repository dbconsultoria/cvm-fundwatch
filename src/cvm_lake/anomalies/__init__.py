"""Anomaly detection on the daily fund reports (ADR 0028).

Layers: rules (``rules.py``), per fund robust statistics, accounting identity residual and peer
context (``features.sql``), combined into ``gold/anomalies`` by ``detector.py``. Alerts for new
high severity findings are rendered by ``report.py``.
"""
