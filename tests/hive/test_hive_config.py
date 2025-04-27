import os
from pathlib import Path

import pytest

from v4vapp_backend_v2.hive.hive_config import HiveConfig
from v4vapp_backend_v2.hive.hive_extras import get_hive_client


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


HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "TEST_KEY")
HIVE_POSTING_TEST_KEY = os.environ.get("HIVE_POSTING_TEST_KEY", "TEST_KEY")
HIVE_ACTIVE_TEST_KEY = os.environ.get("HIVE_ACTIVE_TEST_KEY", "TEST_KEY")

hive = get_hive_client(keys=[HIVE_POSTING_TEST_KEY])


@pytest.mark.asyncio
async def test_get_settings_from_hive():
    hive_config = HiveConfig(server_accname="hivehydra", hive=hive)
    assert hive_config is not None
    assert hive_config.data.conv_fee_sats is not None

    hive_config = HiveConfig(server_accname="testnet", hive=hive)
    assert hive_config is not None
    print(hive_config.server_accname)


@pytest.mark.asyncio
async def test_put_settings_from_hive():
    hive_config = HiveConfig(server_accname="hivehydra", hive=hive)
    assert hive_config is not None
    assert hive_config.data.conv_fee_sats is not None

    new_data = hive_config.data

    hive_config = HiveConfig(server_accname=HIVE_ACC_TEST, hive=hive)
    hive_config.put(new_data)
    assert hive_config.data == new_data
