"""Write small synthetic gold/reporting tables, so CI can build the dashboard without the lake.

Usage: python dashboard/scripts/demo_data.py OUT_DIR   (then REPORTING_DIR=OUT_DIR)
"""

from __future__ import annotations

import math
import random
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cvm_lake.reporting import SCHEMAS, reporting_path

PEERS = {"Renda Fixa": 4.0e12, "Multimercado": 1.5e12, "Ações": 0.7e12, "Cambial": 0.01e12}


def main(out: Path) -> None:
    rng = random.Random(1)
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(640)]
    days = [d for d in days if d.weekday() < 5]
    flows, anomalies, recent, ptax = [], [], [], []
    for n, d in enumerate(days):
        ptax.append({"dt": d, "sell": 5.0 + 0.3 * math.sin(n / 40)})
        for peer, base in PEERS.items():
            for fic in (False, True):
                aum = base * (1 + n / 2000) * (0.3 if fic else 1.0)
                flows.append({
                    "dt": d, "peer_group": peer, "is_fic": fic, "n_classes": 500,
                    "aum": aum, "subscriptions": aum * 0.002, "redemptions": aum * 0.0019,
                    "net_flow": aum * 0.0001, "complete": n < len(days) - 1,
                })  # fmt: skip
        for severity in ("high", "medium", "low"):
            anomalies.append({"dt": d, "anomaly_type": "return_outlier", "severity": severity,
                              "n": rng.randint(5, 40)})  # fmt: skip
        if n > len(days) - 60:
            recent.append({"dt": d, "fund_id": f"{n:014d}", "class_id": "", "name": f"FUNDO {n}",
                           "peer_group": "Multimercado", "anomaly_type": "decimal_shift",
                           "severity": "high", "score": 2.0, "first_seen_run": "demo"})  # fmt: skip
    months = sorted({f"{d:%Y%m}" for d in days})
    returns = [
        {"ano_mes": m, "peer_group": p, "n_funds": 100, "median_ret_brl": 0.01,
         "p25_ret_brl": 0.0, "p75_ret_brl": 0.02, "median_ret_usd": 0.005}
        for m in months for p in PEERS
    ]  # fmt: skip
    restated = [{"ano_mes": m, "detected_run": "demo", "changed_reports": 10, "flagged_before": 3}
                for m in months[-6:]]  # fmt: skip
    tables = {
        "aum_flows_daily": flows,
        "returns_monthly": returns,
        "anomalies_daily": anomalies,
        "anomalies_recent": recent,
        "restatements_monthly": restated,
        "ptax_daily": ptax,
    }
    for name, rows in tables.items():
        path = out / reporting_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMAS[name]), path)
    print(f"demo reporting tables in {out}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
