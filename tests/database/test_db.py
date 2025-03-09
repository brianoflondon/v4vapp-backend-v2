import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from pymongo.errors import ConnectionFailure, DuplicateKeyError, WriteConcernError

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
    with patch(
        "v4vapp_backend_v2.config.mylogger.NotificationProtocol.send_notification",
        lambda self, message, record, alert_level=1: None,
    ):
        yield
    # Unpatch the monkeypatch
    monkeypatch.undo()


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


async def drop_collection_and_user(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    async with MongoDBClient(conn_name, db_name, db_user) as test_client:
        ans = await test_client.db.drop_collection("startup_collection")
        assert ans.get("ok") == 1
        ans = await test_client.drop_user()
        assert ans.get("ok") == 1
    await drop_database(conn_name=conn_name, db_name=db_name)


async def drop_database(conn_name: str, db_name: str) -> None:
    async with MongoDBClient(conn_name) as admin_client:
        await admin_client.drop_database(db_name)


@pytest.mark.asyncio
async def test_mongodb_client_bad_uri(set_base_config_path: None):
    """
    Test the MongoDBClient connection with a bad URI.

    This test function initializes a MongoDBClient with a bad URI and asserts
    that the connection fails.

    Args:
        set_base_config_path (None): A fixture to set the base configuration path.

    Raises:
        ConnectionFailure: If the connection to the MongoDB instance fails.
    """
    with pytest.raises(ConnectionFailure) as e:
        async with MongoDBClient(
            "conn_bad", serverSelectionTimeoutMS=50, retry=False
        ) as _:
            print("conn bad")
            pass
    assert e


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_mongodb_client_local(set_base_config_path: None):
    """
    Test the MongoDB client connection to a local MongoDB instance.
    This test function initializes a MongoDBClient with a local MongoDB URI and a test
    database name. It asserts that the MongoDBClient instance is created successfully
    and that the URI is set correctly. Finally, it attempts to connect to the MongoDB
    instance. Github Actions has a Mongodb Instance running on localhost:37017
    Args:
        set_base_config_path (None): A fixture to set the base configuration path.
    Raises:
        AssertionError: If the MongoDBClient instance is None or if the URI does not
        match the expected value.
    """
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        assert test_client is not None
        print(test_client.collections)
        cursor = await test_client.db.list_collections()
        collections = []
        async for collection in cursor:
            collections.append(collection)
        assert "startup_collection" in [
            collection["name"] for collection in collections
        ]
        find_1 = await test_client.find_one(
            "startup_collection", {"startup": "complete"}
        )

    # Run a second time to check that the database and user already exist
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        assert test_client is not None
        find_2 = await test_client.find_one(
            "startup_collection", {"startup": "complete"}
        )
    # Second run doesn't change the startup
    assert find_1 == find_2
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_mongodb_multiple_databases(set_base_config_path: None):
    """
    Multiple simultaneous connections to different databases with different users.
    """
    async with MongoDBClient("conn_1", "test_db2", "test_user2") as test2_client:
        assert test2_client.db is not None
        async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
            assert test_client.db is not None
            assert test_client.db_name == "test_db"

    await drop_collection_and_user("conn_1", "test_db2", "test_user2")
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_get_collection(set_base_config_path: None):
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection = await test_client.get_collection("startup_collection")
        ans = await collection.find_one({"startup": "complete"})
        assert ans is not None
    await drop_collection_and_user("conn_1", "test_db2", "test_user2")


@pytest.mark.asyncio
async def test_insert_one_find_one(set_base_config_path: None):
    collection_name = "new_collection"
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        insert_ans = await test_client.insert_one(
            collection_name,
            {collection_name: "test", "timestamp": datetime.now(tz=timezone.utc)},
        )
        assert insert_ans is not None
        find_one_ans = await test_client.find_one(
            collection_name, {collection_name: "test"}
        )
        assert find_one_ans is not None
        find_one_fail_ans = await test_client.find_one(
            collection_name, {collection_name: "fail"}
        )
        assert find_one_fail_ans is None
    await drop_collection_and_user("conn_1", "test_db2", "test_user2")


@pytest.mark.asyncio
async def test_update_one_delete_one(set_base_config_path: None):
    collection_name = "update_delete"
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        insert_ans = await test_client.insert_one(
            collection_name,
            {collection_name: "test", "timestamp": datetime.now(tz=timezone.utc)},
        )
        assert insert_ans is not None
        update_ans = await test_client.update_one(
            collection_name, {collection_name: "test"}, {collection_name: "updated"}
        )
        assert update_ans is not None
        find_one_ans = await test_client.find_one(
            collection_name, {collection_name: "updated"}
        )
        assert find_one_ans is not None
        delete_ans = await test_client.delete_one(
            collection_name, {collection_name: "updated"}
        )
        assert delete_ans is not None
    await drop_collection_and_user("conn_1", "test_db2", "test_user2")


@pytest.mark.asyncio
async def test_fill_database_with_data_index_test(set_base_config_path: None):
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection_name = "index_test"
        index_key = [("timestamp", -1), ("field_1", 1)]
        test_client.db[collection_name].create_index(
            keys=index_key, name="timestamp", unique=True
        )
        for i in range(10):
            data = {
                "field_1": f"test_{i}",
                "field_2": f"test_{i}",
                "field_3": f"test_{i}",
                "field_4": f"test_{i}",
                "field_5": f"test_{i}",
                "timestamp": datetime.now(tz=timezone.utc),
            }
            await test_client.insert_one(collection_name, data)
        try:
            await test_client.insert_one(collection_name, data)
        except DuplicateKeyError as e:
            print(e)
        cursor = test_client.db[collection_name].find({})
        count = 0
        async for _ in cursor:
            count += 1
        assert count == 10
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_check_indexes(set_base_config_path: None):
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        await test_client._check_indexes()
        ans = await test_client.insert_one("test_collection", {"test": "test"})
        assert ans is not None
        with pytest.raises(DuplicateKeyError):
            ans = await test_client.insert_one("test_collection", {"test": "test"})
        await test_client._check_indexes()
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_find(set_base_config_path: None):
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection_name = "index_test"
        index_key = [("timestamp", -1), ("field_1", 1)]
        test_client.db[collection_name].create_index(
            keys=index_key, name="timestamp", unique=True
        )
        insert_items = 10
        for i in range(insert_items):
            data = {
                "field_1": f"test_{i}",
                "field_2": f"test_{i}",
                "field_3": f"test_{i}",
                "field_4": f"test_{i}",
                "field_5": f"test_{i}",
                "timestamp": datetime.now(tz=timezone.utc),
            }
            await test_client.insert_one(collection_name, data)
        cursor = await test_client.find(collection_name, {})
        count = 0
        async for _ in cursor:
            count += 1
        assert count == insert_items
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_interrupted_insert_one(set_base_config_path: None, mocker):
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection_name = "loop_test"

        # Patch the insert_one method to raise WriteConcernError on the 50th iteration
        original_insert_one = test_client.insert_one

        async def patched_insert_one(collection_name, data):
            if data["field_1"] == "test_50":
                raise WriteConcernError(
                    error="interrupted at shutdown",
                    code=11600,
                    details={
                        "errmsg": "interrupted at shutdown",
                        "code": 11600,
                        "codeName": "InterruptedAtShutdown",
                    },
                )
            return await original_insert_one(collection_name, data)

        mocker.patch.object(test_client, "insert_one", side_effect=patched_insert_one)

        for i in range(100):
            data = {
                "field_1": f"test_{i}",
                "field_2": f"test_{i}",
                "field_3": f"test_{i}",
                "field_4": f"test_{i}",
                "field_5": f"test_{i}",
                "timestamp": datetime.now(tz=timezone.utc),
            }
            retries = 0
            while retries < 3:
                try:
                    await test_client.insert_one(collection_name, data)
                    break
                except WriteConcernError as e:
                    print(f"WriteConcernError: {e}")
                    retries += 1
                if retries == 3:
                    mocker.patch.object(
                        test_client, "insert_one", side_effect=original_insert_one
                    )

        cursor = test_client.db[collection_name].find({})
        count = 0
        async for _ in cursor:
            count += 1
        assert count == 99
    await drop_collection_and_user("conn_1", "test_db", "test_user")
