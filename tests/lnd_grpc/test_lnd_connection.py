from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDStartupError


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH", test_config_logging_path
    )
    yield


def test_local_node_settings(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)
    settings = LNDConnectionSettings(connection_name="example")
    assert settings.address == raw_config["lnd_connections"][0]["address"]


def test_bad_connection_name(set_base_config_path: None):
    with pytest.raises(LNDStartupError):
        LNDConnectionSettings(connection_name="bad_example")
