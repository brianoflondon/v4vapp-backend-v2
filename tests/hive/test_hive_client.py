from pathlib import Path

import pytest
from nectar.blockchain import Blockchain

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import get_good_nodes, get_hive_client


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


def test_get_good_nodes():
    good_nodes = get_good_nodes()
    assert good_nodes is not None
    assert len(good_nodes) > 0
    logger.info(f"Good nodes: {good_nodes}")


def test_get_producer_rewards():
    hive_client = get_hive_client(stream_only=True)
    hive_blockchain = Blockchain(hive=hive_client)
    end_block = hive_client.get_dynamic_global_properties().get("head_block_number")
    stream = hive_blockchain.stream(
        start=end_block - 400,
        stop=end_block,
        raw_ops=False,
        only_virtual_ops=True,
        opNames=["producer_reward"],
        # threading=True,
        max_batch_size=50,
    )
    witnesses = []
    witness_counts = {}
    print(f"Producer Rewards Events from {hive_client.rpc.url}:")
    for hive_event in stream:
        print(
            f"Event: {hive_event['timestamp']} {hive_event['block_num']} {hive_event['producer']}"
        )
        witness = hive_event["producer"]
        witnesses.append(hive_event["producer"])
        witness_counts[witness] = witness_counts.get(witness, 0) + 1
    # give count of each time each witness produced a block
    print(f"Producer Rewards Events from {hive_client.rpc.url}:")
    # give the total number of blocks produced
    print(f"Total blocks produced: {len(witnesses)}")
    assert len(witnesses) == 401


if __name__ == "__main__":
    pytest.main([__file__])
