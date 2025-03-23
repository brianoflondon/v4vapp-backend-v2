import json
from pprint import pprint
from typing import Dict, Generator

from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes

files_names: Dict[OpTypes, str] = {
    OpTypes.PRODUCER_REWARD: "tests/data/hive_models/all_ops_log.jsonl",
    OpTypes.ACCOUNT_WITNESS_VOTE: "tests/data/hive_models/all_ops_log.jsonl",
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
