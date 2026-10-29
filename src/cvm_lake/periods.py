"""Month handling. A month is always a ``YYYYMM`` string, the same as the partition value."""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

BRT = ZoneInfo("America/Sao_Paulo")
_MONTH = re.compile(r"^(\d{4})(0[1-9]|1[0-2])$")


def today_brt() -> date:
    return datetime.now(BRT).date()


def validate_month(ym: str) -> str:
    if not _MONTH.match(ym):
        raise ValueError(f"invalid month {ym!r}, expected YYYYMM")
    return ym


def shift_month(ym: str, delta: int) -> str:
    validate_month(ym)
    index = int(ym[:4]) * 12 + int(ym[4:]) - 1 + delta
    return f"{index // 12:04d}{index % 12 + 1:02d}"


def month_of(day: date) -> str:
    return f"{day.year:04d}{day.month:02d}"


def month_bounds(ym: str) -> tuple[date, date]:
    validate_month(ym)
    year, month = int(ym[:4]), int(ym[4:])
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def lookback_months(today: date, n: int) -> list[str]:
    """The ``n`` months ending at the month of ``today``, oldest first."""
    if n < 1:
        raise ValueError("lookback must be >= 1")
    current = month_of(today)
    return [shift_month(current, -i) for i in range(n - 1, -1, -1)]


def parse_months(spec: str) -> list[str]:
    """Parse ``202301``, ``202301-202412`` or a comma list of both. Sorted, unique."""
    months: set[str] = set()
    for part in (p.strip() for p in spec.split(",")):
        if not part:
            continue
        if "-" in part:
            start, end = (validate_month(x.strip()) for x in part.split("-", 1))
            if start > end:
                raise ValueError(f"range start after end: {part!r}")
            current = start
            while current <= end:
                months.add(current)
                current = shift_month(current, 1)
        else:
            months.add(validate_month(part))
    if not months:
        raise ValueError(f"no months in {spec!r}")
    return sorted(months)
