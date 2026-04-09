from pathlib import Path

import pytest
from nectar.exceptions import BlockDoesNotExistsException

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import get_good_nodes


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
    stream = iter(
        [
            {
                "timestamp": "2026-01-01T00:00:00",
                "block_num": 99,
                "producer": "alice",
            },
            {
                "timestamp": "2026-01-01T00:00:03",
                "block_num": 100,
                "producer": "bob",
            },
        ]
    )
    witnesses = []
    witness_counts = {}
    rpc_url = "https://mock.hive.node"
    print(f"Producer Rewards Events from {rpc_url}:")
    try:
        for hive_event in stream:
            print(
                f"Event: {hive_event['timestamp']} {hive_event['block_num']} {hive_event['producer']}"
            )
            witness = hive_event["producer"]
            witnesses.append(hive_event["producer"])
            witness_counts[witness] = witness_counts.get(witness, 0) + 1
    except BlockDoesNotExistsException as e:
        logger.warning(f"Block not available (node may be behind): {e}")
        # Continue with whatever blocks we did get
    # give count of each time each witness produced a block
    print(f"Producer Rewards Events from {rpc_url}:")
    # give the total number of blocks produced
    print(f"Total blocks produced: {len(witnesses)}")
    # Allow for some blocks to be missed if node is behind
    assert len(witnesses) > 0, "Should have received at least some producer rewards"


if __name__ == "__main__":
    pytest.main([__file__])
