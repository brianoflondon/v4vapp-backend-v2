import asyncio
import json
import logging
from pathlib import Path
from timeit import default_timer as timeit

import pytest
from lighthive.client import Client
from lighthive.helpers.event_listener import EventListener

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import (
    sync_to_async,
    sync_to_async_iterable,
)
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


def test_lighthive_client_not_async():
    # Test the lighthive client
    client = Client()
    assert client is not None
    logger.info(f"Client node_list: {client.node_list}")
    props = client.get_dynamic_global_properties()
    head_block_number = props.get("head_block_number")
    # Test the lighthive event listener
    client
    events = EventListener(
        client, start_block=head_block_number - 10, end_block=head_block_number
    )
    for custom_json in events.on(["custom_json", "transfer"]):
        logger.info(f"custom_json: {custom_json}")


@pytest.mark.asyncio
async def test_lighthive_client_async():
    # Test the lighthive client
    client = Client()
    assert client is not None
    logger.info(f"Client node_list: {client.node_list}")

    # Define the custom condition function
    def condition(operation_value):
        if ("to" in operation_value and "v4vapp" in operation_value["to"]) or (
            "from" in operation_value and "v4vapp" in operation_value["from"]
        ):
            return True
        return False

    count = 0
    # Test the lighthive event listener
    events = EventListener(client, start_block=93598595, end_block=93598599 + 10)
    async_events = sync_to_async_iterable(events.on(["transfer"], condition=condition))
    async for transfer in async_events:
        count += 1
        logger.info(f"transfer: {json.dumps(transfer, indent=2)}")
    # Test the lighthive event listener
    assert count == 2


@pytest.mark.asyncio
async def test_find_podpings():
    def condition(operation_value):
        if "id" in operation_value and operation_value["id"].startswith("pp"):
            return True

        return False

    client = Client()
    events = EventListener(client)
    count = 0
    async_events = sync_to_async_iterable(
        events.on(["custom_json"], condition=condition)
    )
    async for custom_json in async_events:
        count += 1
        print(f"custom_json: {json.dumps(custom_json, indent=2)}")
        if count > 2:
            break
    await asyncio.sleep(1)

if __name__ == "__main__":
    pytest.main([__file__])
