from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.config.setup import (
    Config,
    InternalConfig,
    StartupFailure,
    format_time_delta,
    get_in_flight_time,
)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_valid_config_file_and_model_validate(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)
    assert raw_config is not None

    try:
        config = Config.model_validate(raw_config)
        assert config is not None
    except Exception as e:
        print(e)
        assert False


def test_internal_config(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)

    try:
        internal_config = InternalConfig()
    except StartupFailure as e:
        print(e)
    assert internal_config is not None
    assert internal_config.config is not None
    int_config = internal_config.config
    assert int_config.version == raw_config["version"]
    assert len(int_config.lnd_connections) == len(raw_config["lnd_connections"])
    assert (
        int_config.lnd_connections["example"].address
        == raw_config["lnd_connections"]["example"]["address"]
    )
    with pytest.raises(KeyError):
        int_config.lnd_connections["bad_example"]


def test_singleton_config(set_base_config_path: None):
    internal_config = InternalConfig()
    internal_config2 = InternalConfig()
    assert internal_config is internal_config2


def test_bad_internal_config(monkeypatch: pytest.MonkeyPatch):
    test_config_path_bad = Path("tests/data/config-bad")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path_bad
    )
    # detect sys.exit(1) call

    with pytest.raises(StartupFailure):
        InternalConfig()


def test_format_time_delta():
    # Test cases without fractions
    test_cases = [
        (timedelta(days=1, hours=2), "1 days, 2 hours"),
        (timedelta(hours=5, minutes=6, seconds=7), "05:06:07"),
        (timedelta(minutes=8, seconds=9), "00:08:09"),
        (timedelta(seconds=10), "00:00:10"),
        (timedelta(days=0, hours=0, minutes=0, seconds=0), "00:00:00"),
        (timedelta(days=2, hours=0, minutes=0, seconds=0), "2 days, 0 hours"),
        (timedelta(days=0, hours=3, minutes=0, seconds=0), "03:00:00"),
        (timedelta(days=0, hours=0, minutes=4, seconds=0), "00:04:00"),
    ]

    for delta, expected in test_cases:
        assert format_time_delta(delta) == expected

    # Test cases with fractions
    test_cases_with_fractions = [
        (timedelta(hours=1, minutes=2, seconds=3, microseconds=456000), "01:02:03.456"),
        (timedelta(minutes=8, seconds=9, microseconds=123000), "00:08:09.123"),
        (timedelta(seconds=10, microseconds=789000), "00:00:10.789"),
        (
            timedelta(days=0, hours=0, minutes=0, seconds=0, microseconds=0),
            "00:00:00.000",
        ),
    ]

    for delta, expected in test_cases_with_fractions:
        assert format_time_delta(delta, fractions=True) == expected


def test_get_in_flight_time_future_date():
    # Test case where the current time is before the creation date
    future_date = datetime.now(tz=timezone.utc) + timedelta(days=1)
    result = get_in_flight_time(future_date)
    assert result == "00:00:00", f"Expected '00:00:00', but got {result}"


def test_get_in_flight_time_past_date():
    # Test case where the current time is after the creation date
    past_date = datetime.now(tz=timezone.utc) - timedelta(days=1, hours=5, minutes=30)
    result = get_in_flight_time(past_date)
    assert result == "1 days, 5 hours", f"Expected '1 days, 5 hours', but got {result}"


def test_get_in_flight_time_exact_date():
    # Test case where the current time is exactly the creation date
    exact_date = datetime.now(tz=timezone.utc)
    result = get_in_flight_time(exact_date)
    assert result == "00:00:00", f"Expected '00:00:00', but got {result}"
