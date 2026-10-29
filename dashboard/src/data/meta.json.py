"""Build metadata: source repo and commit, date range, latest complete date."""

import json
import sys
from datetime import UTC, datetime

from _reporting import connect, source

con = connect()
first, last, complete = con.execute(
    "SELECT min(dt), max(dt), max(dt) FILTER (WHERE complete) FROM aum_flows_daily"
).fetchall()[0]
json.dump(
    {
        **source(),
        "built_at": datetime.now(UTC).isoformat(timespec="minutes"),
        "first_date": str(first) if first else None,
        "last_date": str(last) if last else None,
        "last_complete_date": str(complete) if complete else None,
    },
    sys.stdout,
)
