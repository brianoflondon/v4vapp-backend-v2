from pathlib import Path
from pprint import pprint

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


# TODO: #61 replace the set_base_config_path reset_internal_config pair everywhere with this
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
async def test_model_validate_producer_reward():
    """
    Test the validation of the ProducerReward model with hive events of type 'producer_reward'.
    This test function performs the following steps:
    1. Initializes a counter to track the number of 'producer_reward' events.
    2. Iterates through hive events of type 'producer_reward' loaded by the `load_hive_events`
    function.
    3. For each event of type 'producer_reward':
        - Increments the counter.
        - Validates the event using the `ProducerReward.model_validate` method.
        - Asserts that the transaction ID (`trx_id`) matches between the event and the
        validated model.
        - Asserts that the `vesting_shares` amount and NAI match between the event and the
        validated model.
        - If the producer is 'threespeak', fetches witness details using
        `get_hive_witness_details` and asserts that:
            - The witness details are not None.
            - The witness name is 'threespeak'.
        - Prints the producer's name.
    4. Asserts that the total count of 'producer_reward' events is 28.
    """

    count = 0
    for hive_event in load_hive_events(OpTypes.PRODUCER_REWARD):
        if hive_event["type"] == "producer_reward":
            count += 1
            producer_reward = ProducerReward.model_validate(hive_event)
            assert producer_reward.trx_id == hive_event["trx_id"]
            assert producer_reward.vesting_shares.amount == hive_event["vesting_shares"]["amount"]
            assert producer_reward.vesting_shares.nai == hive_event["vesting_shares"]["nai"]
            if producer_reward.producer == "threespeak":
                await producer_reward.get_witness_details()
                assert producer_reward.witness is not None
                assert producer_reward.witness.witness_name == "threespeak"
                pprint(producer_reward.witness, indent=2)
                pprint(producer_reward.model_dump(), indent=2)
                print(producer_reward.notification_str)
                print(producer_reward.log_str)
            print(producer_reward.log_str)
            print(producer_reward.notification_str)
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
            assert hive_event_model["vesting_shares"]["nai"] == hive_event["vesting_shares"]["nai"]
    assert count == 28
