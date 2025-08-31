from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint
from unittest.mock import AsyncMock, patch

import pytest
from nectar.amount import Amount

from tests.get_last_quote import last_quote
from tests.utils import fake_trx_id, latest_block_num
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.conversion.hive_to_keepsats import conversion_hive_to_keepsats
from v4vapp_backend_v2.conversion.keepsats_to_hive import conversion_keepsats_to_hive
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_transfer import Transfer


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


@pytest.fixture
def mock_hive_to_keepsats_deps():
    with (
        patch(
            "v4vapp_backend_v2.conversion.hive_to_keepsats.send_transfer_custom_json"
        ) as mock_transfer,
        patch.object(LedgerEntry, "save", new_callable=AsyncMock) as mock_save,
        patch.object(TrackedBaseModel, "save", new_callable=AsyncMock) as mock_tracked_save,
        patch.object(TrackedBaseModel, "update_conv", new_callable=AsyncMock) as mock_update_conv,
    ):
        mock_transfer.return_value = {
            "trx_id": fake_trx_id(),
            "block_num": latest_block_num(),
            "trx_num": 1,
            "status": "success",
        }
        mock_save.return_value = None
        mock_tracked_save.return_value = None
        mock_update_conv.return_value = None
        yield {"send_transfer": mock_transfer, "ledger_save": mock_save}


async def test_conversion_hive_to_keepsats_with_change(mock_hive_to_keepsats_deps):
    # Example test case for conversion_hive_to_keepsats
    TrackedBaseModel.last_quote = last_quote()
    pprint(TrackedBaseModel.last_quote)
    original_amount = Amount("12.000 HBD")
    server_account = "devser.v4vapp"
    customer_account = "v4vapp-test"

    msats = 9_111_000

    tracked_op = Transfer(
        from_account=server_account,
        to_account=customer_account,
        amount=original_amount,
        memo="Deposit #sats",
        timestamp=datetime.now(timezone.utc),
        trx_id=fake_trx_id(),
        op_type="transfer",
        block_num=latest_block_num(),
    )

    # Test with valid conversion amount
    await conversion_hive_to_keepsats(
        server_id=server_account,
        cust_id=customer_account,
        tracked_op=tracked_op,
        msats=msats,
        quote=last_quote(),
    )

    assert True
    # Assert the mocks were called
    assert mock_hive_to_keepsats_deps["send_transfer"].called
    assert mock_hive_to_keepsats_deps["ledger_save"].call_count == 3


async def test_conversion_hive_to_keepsats_msats(mock_hive_to_keepsats_deps):
    # Example test case for conversion_hive_to_keepsats
    TrackedBaseModel.last_quote = last_quote()
    convert_amount = Amount("13.456 HIVE")
    server_account = "v4vapp_server"
    customer_account = "customer123"
    msats = 0

    tracked_op = Transfer(
        from_account=server_account,
        to_account=customer_account,
        memo="Deposit #sats",
        amount=convert_amount,
        timestamp=datetime.now(timezone.utc),
        trx_id=fake_trx_id(),
        op_type="transfer",
        block_num=latest_block_num(),
    )

    # Test with valid conversion amount
    await conversion_hive_to_keepsats(
        server_id=server_account,
        cust_id=customer_account,
        tracked_op=tracked_op,
        msats=msats,
        quote=last_quote(),
    )
    assert True
    # Assert the mocks were called
    assert mock_hive_to_keepsats_deps["send_transfer"].called
    assert mock_hive_to_keepsats_deps["ledger_save"].call_count == 3  # 4 ledger entries saved


async def test_keepsats_to_hive(mock_hive_to_keepsats_deps):
    # Example test case for keepsats_to_hive
    pytest.skip("Skipping")
    TrackedBaseModel.last_quote = last_quote()
    convert_amount = Amount("13.456 HIVE")
    server_account = "v4vapp_server"
    customer_account = "customer123"
    msats = 0

    tracked_op = CustomJson(
        op_type="custom_json",
        cj_id="v4vapp_dev_transfer",
        from_account=server_account,
        to_account=customer_account,
        memo="Deposit #sats",
        amount=convert_amount,
        timestamp=datetime.now(timezone.utc),
        trx_id=fake_trx_id(),
        block_num=latest_block_num(),
    )

    # Test with valid conversion amount
    await conversion_keepsats_to_hive(
        server_id=server_account,
        cust_id=customer_account,
        tracked_op=tracked_op,
        msats=msats,
        quote=last_quote(),
    )
    assert True
    # Assert the mocks were called
    assert mock_hive_to_keepsats_deps["send_transfer"].called
    assert mock_hive_to_keepsats_deps["ledger_save"].call_count == 4  # 4 ledger entries saved
