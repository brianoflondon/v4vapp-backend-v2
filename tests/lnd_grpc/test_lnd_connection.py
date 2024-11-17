from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDStartupError
from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.BASE_LOGGING_CONFIG_PATH", test_config_logging_path
    )
    yield


def test_local_node_settings(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)

    with pytest.raises(LNDStartupError):
        settings = LNDConnectionSettings()
        assert settings.address == raw_config["lnd_connection"]["address"]

