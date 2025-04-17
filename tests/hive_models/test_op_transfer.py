import os
from pathlib import Path
from pprint import pprint

import pytest
from nectar.amount import Amount

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_transfer import Transfer, TransferRaw
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


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
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "")


def test_model_validate_transfer():
    for hive_event in load_hive_events(OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            transfer = TransferRaw.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]


def test_op_transfer_watch_list():
    OpBase.watch_list = ["john", "paul", "george", "ringo"]
    for hive_event in load_hive_events(OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            transfer = TransferRaw.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]
            assert transfer.from_account not in Transfer.watch_list
            assert transfer.to_account not in Transfer.watch_list
            print(transfer.to_account)



def test_model_validate_transfer_enhanced():
    """
    Test the validation of the TransferEnhanced model with enhanced transfer events.

    This test function performs the following steps:
    1. Checks if the HIVE_MEMO_TEST_KEY is available. If not, the test is skipped.
    2. Initializes a Hive client instance using the provided HIVE_MEMO_TEST_KEY.
    3. Loads Hive events of type 'transfer' and iterates through them.
    4. For each transfer event, it validates the event using the TransferEnhanced model.
    5. Asserts that the transaction ID and amount in the validated transfer match the original
    event.
    6. For a specific transaction ID, it checks that the decrypted memo does not match the
    original memo and matches an expected test message.
    7. Prints the notification string of the validated transfer.

    Note:
    - The test is dependent on the presence of the HIVE_MEMO_TEST_KEY.
    - The specific transaction ID "e936d9d3ec5b9c6971c4fe83d65d3fdce7768353" is used to verify the
    decrypted memo.
    """
    if not HIVE_MEMO_TEST_KEY:
        pytest.skip("HIVE_MEMO_TEST_KEY is not available in environment variables")
    hive_inst = get_hive_client(keys=[HIVE_MEMO_TEST_KEY])
    for hive_event in load_hive_events(op_type=OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            hive_event["hive_inst"] = hive_inst
            transfer = Transfer.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]
            if transfer.trx_id == "e936d9d3ec5b9c6971c4fe83d65d3fdce7768353":
                assert transfer.d_memo != hive_event["memo"]
                assert transfer.d_memo == "This is an encrypted test message"
                print(transfer.notification_str)
                print(transfer.logs.log)
                pprint(transfer.logs.log_extra, indent=2, width=120)
            assert transfer.from_account in transfer.notification_str
            assert transfer.to_account in transfer.logs.notification


@pytest.mark.asyncio
async def test_model_dump_transfer_enhanced():
    await Transfer.update_quote()
    for hive_event in load_hive_events(OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            transfer = Transfer.model_validate(hive_event)
            hive_event_model = transfer.model_dump(by_alias=True)
            assert hive_event_model["d_memo"] == transfer.d_memo
            assert hive_event_model["from"] == transfer.from_account
            assert hive_event_model["to"] == transfer.to_account
            assert hive_event_model["memo"] == transfer.memo
            assert transfer.conv.hive == hive_event_model["conv"]["hive"]
            assert transfer.log_str
            assert transfer.notification_str
            assert transfer.conv.conv_from == Amount(hive_event_model["amount"]).symbol.lower()
