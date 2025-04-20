import os
from pathlib import Path

import pytest

from v4vapp_backend_v2.database.db import MongoDBClient


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture()
def set_base_config_path_dev(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("config/")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # Unpatch the monkeypatch
    monkeypatch.undo()


# skip this test if running on github actions
@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_mongodb_client_dev_config(set_base_config_path_dev: None):
    async with MongoDBClient("local_connection") as client:
        assert client.uri is not None
