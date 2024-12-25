from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.config.setup import InternalConfig


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


def test_internal_config(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)

    internal_config = InternalConfig()
    assert internal_config is not None
    assert internal_config.config is not None
    int_config = internal_config.config
    assert int_config.version == raw_config["version"]
    assert len(int_config.lnd_connections) == len(raw_config["lnd_connections"])
    assert int_config.connection("example").name == "example"
    with pytest.raises(ValueError):
        int_config.connection("bad_example")


def test_singleton_config(set_base_config_path: None):
    internal_config = InternalConfig()
    internal_config2 = InternalConfig()
    print(internal_config.config.version)
    print(internal_config2.config.version)
    assert internal_config is internal_config2


def test_bad_internal_config(monkeypatch: pytest.MonkeyPatch):
    test_config_path_bad = Path("tests/data/config-bad")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path_bad
    )
    # detect sys.exit(1) call
    with pytest.raises(SystemExit):
        InternalConfig()
