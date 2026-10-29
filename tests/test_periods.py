from datetime import date

import pytest

from cvm_lake.periods import lookback_months, month_bounds, parse_months, shift_month


def test_shift_month_crosses_years() -> None:
    assert shift_month("202601", -1) == "202512"
    assert shift_month("202512", 1) == "202601"
    assert shift_month("202610", -13) == "202509"


def test_lookback_is_oldest_first_and_includes_current() -> None:
    assert lookback_months(date(2026, 1, 5), 2) == ["202512", "202601"]
    assert lookback_months(date(2026, 10, 29), 1) == ["202610"]


def test_parse_months_ranges_and_lists() -> None:
    assert parse_months("202311-202402") == ["202311", "202312", "202401", "202402"]
    assert parse_months("202305, 202301,202305") == ["202301", "202305"]


@pytest.mark.parametrize("spec", ["2023-01", "202313", "202402-202401", "", " , "])
def test_parse_months_rejects_bad_input(spec: str) -> None:
    with pytest.raises(ValueError):
        parse_months(spec)


def test_month_bounds_handles_leap_year() -> None:
    assert month_bounds("202402") == (date(2024, 2, 1), date(2024, 2, 29))
