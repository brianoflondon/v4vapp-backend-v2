from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDStartupError


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


def test_local_node_settings():
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)
    settings = LNDConnectionSettings(connection_name="example")
    assert settings.address == raw_config["lnd_config"]["connections"]["example"]["address"]


def test_bad_connection_name():
    with pytest.raises(LNDStartupError):
        LNDConnectionSettings(connection_name="bad_example")
