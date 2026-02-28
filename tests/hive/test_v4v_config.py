import asyncio
import os
from decimal import Decimal
from pathlib import Path

import pytest

from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive.v4v_config import V4VConfig


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
# HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "TEST_KEY")
HIVE_POSTING_TEST_KEY = os.environ.get("HIVE_POSTING_TEST_KEY", "TEST_KEY")
# HIVE_ACTIVE_TEST_KEY = os.environ.get("HIVE_ACTIVE_TEST_KEY", "TEST_KEY")


@pytest.mark.asyncio
async def test_get_settings_from_hive():
    hive = get_hive_client()
    hive_config = V4VConfig(server_accname="hivehydra", hive=hive)
    assert hive_config is not None
    assert hive_config.data.conv_fee_sats is not None
    # the new force_custom_json_payment_sats field should exist and default to 500
    assert hasattr(hive_config.data, "force_custom_json_payment_sats")
    assert hive_config.data.force_custom_json_payment_sats == Decimal(500)

    hive_config = V4VConfig(server_accname="testnet", hive=hive)
    assert hive_config is not None
    assert hive_config.data.conv_fee_sats is not None
    # repeat check for second instance
    assert hasattr(hive_config.data, "force_custom_json_payment_sats")
    assert hive_config.data.force_custom_json_payment_sats == Decimal(500)


@pytest.mark.skipif(
    os.environ.get("HIVE_POSTING_TEST_KEY") is None,
    reason="HIVE_POSTING_TEST_KEY environment variable is not set",
)
@pytest.mark.asyncio
async def test_put_settings_into_hive():
    hive = get_hive_client(keys=[HIVE_POSTING_TEST_KEY])
    hive_config = V4VConfig(server_accname=HIVE_ACC_TEST, hive=hive)
    print(
        f"Minimum invoice taken from Hive for {HIVE_ACC_TEST} {hive_config.data.minimum_invoice_payment_sats}"
    )
    # Directly update the settings
    hive_config.data.minimum_invoice_payment_sats += 1
    test_minimum_invoice_payment_sats = hive_config.data.minimum_invoice_payment_sats
    # FORCE them to update Hive
    await hive_config.put(hive_client=hive)
    assert hive_config.data.minimum_invoice_payment_sats == test_minimum_invoice_payment_sats
    await asyncio.sleep(4)
    # Fetch the settings again to check if they were updated
    hive_config.fetch()
    assert hive_config.data.minimum_invoice_payment_sats == test_minimum_invoice_payment_sats
