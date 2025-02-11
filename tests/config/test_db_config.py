import os
from pathlib import Path
import pytest
from v4vapp_backend_v2.database.db import MongoDBClient
from pymongo.errors import OperationFailure


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


@pytest.mark.asyncio
async def test_mongodb_client_local(set_base_config_path: None):
    """
    Test the MongoDB client connection to a local MongoDB instance.
    This test function initializes a MongoDBClient with a local MongoDB URI and a test database name.
    It asserts that the MongoDBClient instance is created successfully and that the URI is set correctly.
    Finally, it attempts to connect to the MongoDB instance.
    Github Actions has a Mongodb Instance running on localhost:37017
    Args:
        set_base_config_path (None): A fixture to set the base configuration path.
    Raises:
        AssertionError: If the MongoDBClient instance is None or if the URI does not match the expected value.
    """
    async with MongoDBClient("admin") as admin_db:
        ans = await admin_db.client["test_db"].command(
            {"dropDatabase": 1, "comment": "Drop the database during testing"}
        )
        assert ans.get("ok") == 1
        try:
            ans = await admin_db.client["test_db"].command({"dropUser": "test_user"})
            assert ans.get("ok") == 1
        except OperationFailure as e:
            pass
    async with MongoDBClient("test_db") as mongo_db:
        assert mongo_db is not None
        test_collection = await mongo_db.get_collection("startup_collection")
        assert test_collection is not None
        ans = await mongo_db.find_one("startup_collection", {})
        assert ans
        assert ans.get("startup") == "complete"

    # Second connection will encounter a database already exists and
    # user exists
    async with MongoDBClient("test_db") as mongo_db:
        ans = await mongo_db.find_one("startup_collection", {})
        assert ans.get("startup") == "complete"

    async with MongoDBClient("admin") as admin_db:
        ans = await admin_db.client["test_db"].command(
            {"dropDatabase": 1, "comment": "Drop the database during testing"}
        )


@pytest.mark.asyncio
async def test_mongodb_client_config_uri(set_base_config_path: None):
    mongo_db = MongoDBClient()
    assert mongo_db.uri is not None
    await mongo_db.connect()


@pytest.mark.asyncio
async def test_check_create_db(set_base_config_path: None):
    mongo_db = MongoDBClient(db_name="test_db")
    assert mongo_db.uri is not None
    await mongo_db.connect()
    await mongo_db.insert_one("test_collection", {"test": "test"})
    await mongo_db.disconnect()


# skip this test if running on github actions
@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_mongodb_client_dev_config():
    """
    Test the MongoDB client development configuration in the config directory

    This test function performs the following steps:
    1. Creates an instance of `MongoDBClient`.
    2. Asserts that the `uri` attribute of the `MongoDBClient` instance is not None.
    3. Prints the `uri` attribute.
    4. Connects to the MongoDB instance asynchronously.

    Raises:
        AssertionError: If the `uri` attribute is None.
    """
    mongo_db = MongoDBClient()
    assert mongo_db.uri is not None
    await mongo_db.connect()
