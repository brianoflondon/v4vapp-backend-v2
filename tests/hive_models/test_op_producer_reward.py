import json
from typing import Dict, Generator

from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes

files_names: Dict[OpTypes, str] = {
    OpTypes.PRODUCER_REWARD: "tests/data/hive_models/all_ops_log.jsonl",
}


def load_hive_events(op_type: OpTypes) -> Generator[Dict, None, None]:
    file_name = files_names[op_type]
    with open(file_name, "r") as f:
        for line in f:
            hive_event = None
            if "hive_event" in line:
                hive_event = json.loads(line)["hive_event"]
                yield hive_event


def test_model_validate_producer_reward():
    count = 0
    for hive_event in load_hive_events(OpTypes.PRODUCER_REWARD):
        if hive_event["type"] == "producer_reward":
            count += 1
            producer_reward = ProducerReward.model_validate(hive_event)
            print(f"ID {producer_reward.id}")
            assert producer_reward.trx_id == hive_event["trx_id"]
            assert (
                producer_reward.vesting_shares.amount
                == hive_event["vesting_shares"]["amount"]
            )
            assert (
                producer_reward.vesting_shares.nai
                == hive_event["vesting_shares"]["nai"]
            )
    assert count == 28


def test_model_dump_producer_reward():
    count = 0
    for hive_event in load_hive_events(OpTypes.PRODUCER_REWARD):
        if hive_event["type"] == "producer_reward":
            count += 1
            producer_reward = ProducerReward.model_validate(hive_event)
            hive_event_model = producer_reward.model_dump(by_alias=True)
            assert hive_event_model["trx_id"] == hive_event["trx_id"]
            assert (
                hive_event_model["vesting_shares"]["amount"]
                == hive_event["vesting_shares"]["amount"]
            )
            assert (
                hive_event_model["vesting_shares"]["nai"]
                == hive_event["vesting_shares"]["nai"]
            )
    assert count == 28
