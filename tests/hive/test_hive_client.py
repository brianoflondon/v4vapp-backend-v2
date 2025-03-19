from pathlib import Path

import httpx
import pytest
from beem.blockchain import Blockchain  # type: ignore

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import (
    HiveExp,
    get_good_nodes,
    get_hive_block_explorer_link,
    get_hive_client,
)


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_get_good_nodes():
    good_nodes = get_good_nodes()
    assert good_nodes is not None
    assert len(good_nodes) > 0
    logger.info(f"Good nodes: {good_nodes}")


def test_get_hive_block_explorer_link():
    trx_id = "fd321bb9a7ac53ec1a7a04fcca0d0913a089ac2b"
    for block_explorer in HiveExp:
        link = get_hive_block_explorer_link(trx_id, block_explorer)
        try:
            response = httpx.get(link)
            assert response.status_code == 200
        except httpx.HTTPStatusError as e:
            logger.error(f"{block_explorer.name}: {link} - {e}")
        logger.info(f"{block_explorer.name}: {link}")


def test_get_producer_rewards():
    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    end_block = hive_client.get_dynamic_global_properties().get("head_block_number")
    stream = hive_blockchain.stream(
        start=end_block - int(70 * 60 / 3),
        stop=end_block,
        raw_ops=False,
        only_virtual_ops=True,
        opNames=["producer_reward"],
        # threading=True,
        max_batch_size=50,
    )
    witnesses = []
    witness_counts = {}
    for hive_event in stream:
        # print(
        #     f"Event: {hive_event['timestamp']} {hive_event['block_num']} "
        #     f"{hive_event['producer']}"
        # )
        witness = hive_event["producer"]
        witnesses.append(hive_event["producer"])
        witness_counts[witness] = witness_counts.get(witness, 0) + 1
    # give count of each time each witness produced a block

    # give the total number of blocks produced
    print(f"Total blocks produced: {len(witnesses)}")


if __name__ == "__main__":
    pytest.main([__file__])
