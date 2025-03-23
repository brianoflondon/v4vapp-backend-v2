import json
import os
from typing import Dict, Generator

import pytest

from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive_models.op_transfer import Transfer, TransferRaw
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes

files_names: Dict[OpTypes, str] = {
    OpTypes.TRANSFER: "tests/data/hive_models/logs_with_transfer_hive_events.jsonl",
}


def load_hive_events(op_type: OpTypes) -> Generator[Dict, None, None]:
    file_name = files_names[op_type]
    with open(file_name, "r") as f:
        for line in f:
            hive_event = None
            if "hive_event" in line:
                hive_event = json.loads(line)["hive_event"]
                yield hive_event


def test_model_validate_transfer():
    for hive_event in load_hive_events(OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            transfer = TransferRaw.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]


HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "")


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
            assert transfer.log_str
            assert transfer.notification_str
