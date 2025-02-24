import asyncio
import json
import logging
from pathlib import Path
from timeit import default_timer as timeit

import pytest
from beem.nodelist import NodeList

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_client import HiveClient


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


def test_hive_nodes():
    nodelist = NodeList()
    nodelist.update_nodes()
    nodes = nodelist.get_hive_nodes()
    print(nodes)
    # print(hive.is_hive)


@pytest.mark.asyncio
async def test_hive_client():
    hive_client = HiveClient()
    assert hive_client.blockchain.get_current_block_num()
    print("Current block number:", hive_client.blockchain.get_current_block_num())
    assert hive_client.blockchain.get_current_block()
    print("Hive RPC Url:", hive_client.hive.rpc.url)


@pytest.mark.asyncio
async def test_watch_hive_blockchain():
    OP_NAMES = ["transfer"]
    hive_client = HiveClient()
    start_block = hive_client.blockchain.get_current_block_num() - 10
    end_block = start_block + 5

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            stream = sync_to_async_iterable(
                hive_client.blockchain.stream(start=start_block, opNames=OP_NAMES)
            )
            async for post in stream:
                hive_client.hive.data  # Accessing data (assuming this is intentional)
                print(
                    "Block number:",
                    post.get("block_num") - start_block,
                    hive_client.hive.rpc.url,
                )
                if post.get("block_num") > end_block:
                    break
            # If we reach here without exceptions, the test passes
            assert True
            break  # Exit retry loop on success

        except Exception as e:
            retry_count += 1
            print(f"Caught RPCNodeException: {e}")
            print(f"Retrying ({retry_count}/{max_retries})...")
            if retry_count == max_retries:
                print("Max retries reached. Failing the test.")
                assert False, f"Failed after {max_retries} retries due to RPC errors"
            # Optionally switch nodes or wait before retrying
            hive_client.hive.rpc.next_node()  # Switch to the next node in the list
            await asyncio.sleep(1)  # Small delay before retrying
