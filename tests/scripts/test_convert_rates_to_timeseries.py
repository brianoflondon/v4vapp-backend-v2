from datetime import datetime, timezone

from scripts import convert_rates_to_timeseries

_normalize_timestamp = convert_rates_to_timeseries._normalize_timestamp


def test_normalize_iso_string():
    s = "2025-12-24T12:00:00Z"
    dt = _normalize_timestamp(s)
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None
    assert dt == datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)


def test_normalize_ms_int():
    ms = 1700000000000
    dt = _normalize_timestamp(ms)
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None


def test_normalize_seconds_int():
    s = 1700000000
    dt = _normalize_timestamp(s)
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None


def test_normalize_naive_datetime():
    naive = datetime(2025, 12, 24, 12, 0, 0)
    dt = _normalize_timestamp(naive)
    assert dt.tzinfo is not None
    assert dt == datetime(2025, 12, 24, 12, 0, 0, tzinfo=timezone.utc)
