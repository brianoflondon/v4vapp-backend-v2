import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path
from timeit import default_timer as timer
from unittest.mock import patch

import pytest
from pymongo import InsertOne
from pymongo.errors import ConnectionFailure, DuplicateKeyError, WriteConcernError
from pymongo.results import BulkWriteResult

from v4vapp_backend_v2.database.db import MongoDBClient

os.environ["TESTING"] = "True"


@pytest.fixture()
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
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
        async with MongoDBClient("conn_bad", serverSelectionTimeoutMS=50, retry=False) as _:
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
        cursor = await test_client.db.list_collections()
        collections = []
        async for collection in cursor:
            collections.append(collection)
        assert "startup_collection" in [collection["name"] for collection in collections]
        find_1 = await test_client.find_one("startup_collection", {"startup": "complete"})

    # Run a second time to check that the database and user already exist
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        assert test_client is not None
        find_2 = await test_client.find_one("startup_collection", {"startup": "complete"})
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
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_get_timeseries(set_base_config_path: None):
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection = await test_client.get_collection("rates")
        ans = await collection.insert_one(
            {"timestamp": datetime.now(tz=timezone.utc), "rate": "hive_sats", "value": 350}
        )
        assert ans is not None
        ans2 = await collection.find_one({"rate": "hive_sats"})
        assert ans2 is not None
        assert ans2["value"] == 350
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_insert_one_find_one(set_base_config_path: None):
    collection_name = "new_collection"
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        insert_ans = await test_client.insert_one(
            collection_name,
            {collection_name: "test", "timestamp": datetime.now(tz=timezone.utc)},
        )
        assert insert_ans is not None
        find_one_ans = await test_client.find_one(collection_name, {collection_name: "test"})
        assert find_one_ans is not None
        find_one_fail_ans = await test_client.find_one(collection_name, {collection_name: "fail"})
        assert find_one_fail_ans is None
    await drop_collection_and_user("conn_1", "test_db2", "test_user2")


@pytest.mark.asyncio
async def test_update_one_delete_one(set_base_config_path: None):
    """
    Test the functionality of updating, finding, and deleting a document in a MongoDB collection.
    This test performs the following steps:
    1. Inserts a document into the specified collection.
    2. Updates the inserted document with new data.
    3. Verifies the update by finding the updated document.
    4. Updates the document again to unset a specific field.
    5. Verifies the document no longer exists after the unset operation.
    6. Deletes the document from the collection.
    7. Cleans up by dropping the collection and user.
    Args:
        set_base_config_path (None): A fixture or parameter to set the base configuration path.
    Assertions:
        - Ensures the document is successfully inserted.
        - Ensures the document is successfully updated.
        - Ensures the updated document can be found.
        - Ensures the document is no longer found after the unset operation.
        - Ensures the document is successfully deleted.
    """

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
        find_one_ans = await test_client.find_one(collection_name, {collection_name: "updated"})
        assert find_one_ans is not None
        update_ans = await test_client.update_one(
            collection_name=collection_name,
            query={collection_name: "updated"},
            update={"$unset": {collection_name: "test"}},
        )
        assert update_ans is not None
        find_one_ans = await test_client.find_one(collection_name, {collection_name: "updated"})
        assert find_one_ans is None
        delete_ans = await test_client.delete_one(collection_name, {collection_name: "updated"})
        assert delete_ans is not None
    await drop_collection_and_user("conn_1", "test_db2", "test_user2")


@pytest.mark.asyncio
async def test_fill_database_with_data_index_test(set_base_config_path: None):
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        collection_name = "index_test"
        index_key = [("timestamp", -1), ("field_1", 1)]
        test_client.db[collection_name].create_index(keys=index_key, name="timestamp", unique=True)
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
        test_client.db[collection_name].create_index(keys=index_key, name="timestamp", unique=True)
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
        await asyncio.sleep(1)
        ans = await test_client.db.drop_collection(collection_name)
        assert ans.get("ok") == 1
        ans = await test_client.drop_user()
        assert ans.get("ok") == 1
        await test_client.drop_database("test_db")


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
                    mocker.patch.object(test_client, "insert_one", side_effect=original_insert_one)

        cursor = test_client.db[collection_name].find({})
        count = 0
        async for _ in cursor:
            count += 1
        assert count == 99
    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_update_one_repeat(set_base_config_path: None):
    collection_name = "multi_update_one"
    repeat = 3333
    async with MongoDBClient("conn_1", "test_db", "test_user") as test_client:
        start = timer()

        # Prepare bulk insert operations
        bulk_operations = [
            InsertOne({str(n): f"test_{n}", "timestamp": datetime.now(tz=timezone.utc)})
            for n in range(repeat)
        ]

        # Perform bulk write to insert the test data
        bulk_result = await test_client.bulk_write(collection_name, bulk_operations)
        assert bulk_result is not None
        print("Bulk insert time:", timer() - start)
        print("Insert time:", timer() - start)
        tasks = []
        for n in range(repeat):
            tasks.append(
                test_client.update_one_buffer(
                    collection_name, query={str(n): f"test_{n}"}, update={str(n): f"updated {n}"}
                )
            )

        results = await asyncio.gather(*tasks)
        print("Update time:", timer() - start)

        # Initialize totals
        total_nMatched = 0
        total_nModified = 0

        # Iterate through the results and sum up nMatched and nModified
        for result in results:
            if result and result[0] and isinstance(result[0], BulkWriteResult):
                total_nMatched += result[0].matched_count
                total_nModified += result[0].modified_count

        assert total_nMatched == repeat
        assert total_nModified == repeat

    await drop_collection_and_user("conn_1", "test_db", "test_user")
