import os
from pathlib import Path
import pytest
from v4vapp_backend_v2.database.db import MongoDBClient


os.environ["TESTING"] = "True"


@pytest.fixture()
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


# @pytest.fixture(autouse=True)
# def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
#     # Reset the singleton instance before each test
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
#     yield
#     # Reset the singleton instance after each test
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.mark.asyncio
async def test_mongodb_client_local():
    mongo_db = MongoDBClient("mongodb://localhost:37017", "test_db")
    assert mongo_db is not None
    assert mongo_db.uri == "mongodb://localhost:37017"
    await mongo_db.connect()


@pytest.mark.asyncio
async def test_mongodb_client_config(set_base_config_path: None):
    mongo_db = MongoDBClient()
    assert mongo_db.uri is not None
    print(mongo_db.uri)
    await mongo_db.connect()
