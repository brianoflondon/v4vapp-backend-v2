from pathlib import Path
import pytest

from v4vapp_backend_v2.hive.hive_config import HiveConfig


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


@pytest.mark.asyncio
async def test_get_settings_from_hive():
    hive_config = HiveConfig("hivehydra")
    assert hive_config is not None
    assert hive_config.data.conv_fee_sats is not None

    hive_config = HiveConfig(server_accname="testnet")
    assert hive_config is not None
    print(hive_config.server_accname)
