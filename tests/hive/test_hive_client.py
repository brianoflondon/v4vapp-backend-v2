import asyncio
import json
import logging
from pathlib import Path
from timeit import default_timer as timeit

import httpx
import pytest

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.helpers.hive_extras import (
    HiveExp,
    get_good_nodes,
    get_hive_block_explorer_link,
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


if __name__ == "__main__":
    pytest.main([__file__])
