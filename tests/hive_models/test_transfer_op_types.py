import json
import os
from typing import Dict, Generator

import pytest

from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive_models.op_models import OpTypes
from v4vapp_backend_v2.hive_models.transfer_op_types import Transfer, TransferEnhanced

files_names: Dict[OpTypes, str] = {
    OpTypes.TRANSFER: "tests/data/hive_models/logs_with_transfer_hive_events.jsonl",
}


def load_hive_events(op_type: OpTypes) -> Generator[Dict, None, None]:
    file_name = files_names[op_type]
    with open(file_name, "r") as infile:
        for line in infile:
            hive_event = None
            if "hive_event" in line:
                hive_event = json.loads(line)["hive_event"]
                yield hive_event


def test_model_validate_transfer():
    for hive_event in load_hive_events(OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            transfer = Transfer.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]


HIVE_ACC_TEST = os.environ.get("HIVE_ACC_TEST", "alice")
HIVE_MEMO_TEST_KEY = os.environ.get("HIVE_MEMO_TEST_KEY", "TEST_KEY")


def test_model_validate_transfer_enhanced():
    hive_inst = get_hive_client(keys=[HIVE_MEMO_TEST_KEY])
    for hive_event in load_hive_events(op_type=OpTypes.TRANSFER):
        if hive_event["type"] == "transfer":
            hive_event["hive_inst"] = hive_inst
            transfer = TransferEnhanced.model_validate(hive_event)
            assert transfer.trx_id == hive_event["trx_id"]
            assert transfer.amount.amount == hive_event["amount"]["amount"]
            if transfer.trx_id == "e936d9d3ec5b9c6971c4fe83d65d3fdce7768353":
                assert transfer.d_memo != hive_event["memo"]
                assert transfer.d_memo == "This is an encrypted test message"
