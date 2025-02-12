import os
from pathlib import Path
import pytest
from v4vapp_backend_v2.database.db import DbErrorCode, MongoDBClient
from pymongo.errors import OperationFailure, ConnectionFailure


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
async def test_admin_db_local_docker_container(set_base_config_path: None):
    """
    Test the connection to the Admin database. Relies on a docker container running
    a MongoDB instance on port 37017 as listed in the `tests/data/config/config.yaml` file.
    This test ensures that a connection to the Admin database can be established
    using the MongoDBClient. It asserts that the connection is not None.
    Args:
        set_base_config_path (None): A fixture to set the base configuration path.
    Returns:
        None
    Side Effects:
        None
    """
    # Test straight connection to the Admin database
    async with MongoDBClient(db_conn="conn_1") as admin_db:
        assert admin_db is not None


@pytest.mark.asyncio
async def test_bad_config_data(set_base_config_path: None):
    """
    Test the MongoDBClient initialization with various bad configuration data.

    This test function checks the following scenarios:
    1. Attempting to initialize MongoDBClient with a non-existent user.
    2. Attempting to initialize MongoDBClient with a non-existent database.
    3. Attempting to initialize MongoDBClient with a user that has no password.

    Args:
        set_base_config_path (None): A fixture to set the base configuration path.

    Raises:
        OperationFailure: Expected exceptions for each bad configuration scenario.
    """
    with pytest.raises(OperationFailure) as e:
        async with MongoDBClient("conn_1", "test_db", "test_no_user") as test_db:
            pass
    assert e.value.code == DbErrorCode.NO_USER
    with pytest.raises(OperationFailure) as e2:
        async with MongoDBClient("conn_1", "test_no_db", "test_user") as test_db:
            pass
    assert e2.value.code == DbErrorCode.NO_DB
    with pytest.raises(OperationFailure) as e3:
        async with MongoDBClient(
            "conn_1", "test_db", "test_user_no_password"
        ) as test_db:
            pass
    assert e3.value.code == DbErrorCode.NO_PASSWORD
    with pytest.raises(OperationFailure) as e4:
        async with MongoDBClient(
            "conn_missing", "test_db", "test_user_no_password"
        ) as test_db:
            pass
    assert e4.value.code == DbErrorCode.NO_CONNECTION


@pytest.mark.asyncio
async def test_mongodb_client_bad_uri(set_base_config_path: None):
    """
    Test the MongoDBClient connection with a bad URI.

    This test function initializes a MongoDBClient with a bad URI and asserts that the connection fails.

    Args:
        set_base_config_path (None): A fixture to set the base configuration path.

    Raises:
        ConnectionFailure: If the connection to the MongoDB instance fails.
    """
    with pytest.raises(ConnectionFailure) as e:
        async with MongoDBClient(
            "conn_bad", serverSelectionTimeoutMS=50
        ) as test_client:
            pass
    assert e


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
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
        find = await test_client.find_one("startup_collection", {"startup": "complete"})
        print(find)
    # async with MongoDBClient("admin") as admin_db:
    #     ans = await admin_db.client["test_db"].command(
    #         {"dropDatabase": 1, "comment": "Drop the database during testing"}
    #     )
    #     assert ans.get("ok") == 1
    #     try:
    #         ans = await admin_db.client["test_db"].command({"dropUser": "test_user"})
    #         assert ans.get("ok") == 1
    #     except OperationFailure as e:
    #         pass
    # async with MongoDBClient("test_db") as mongo_db:
    #     assert mongo_db is not None
    #     test_collection = await mongo_db.get_collection("startup_collection")
    #     assert test_collection is not None
    #     ans = await mongo_db.find_one("startup_collection", {})
    #     assert ans
    #     assert ans.get("startup") == "complete"

    # # Second connection will encounter a database already exists and
    # # user exists
    # async with MongoDBClient("test_db") as mongo_db:
    #     ans = await mongo_db.find_one("startup_collection", {})
    #     assert ans.get("startup") == "complete"

    # async with MongoDBClient("admin") as admin_db:
    #     ans = await admin_db.client["test_db"].command(
    #         {"dropDatabase": 1, "comment": "Drop the database during testing"}
    #     )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_mongodb_client_config_uri(set_base_config_path: None):
    mongo_db = MongoDBClient()
    assert mongo_db.uri is not None
    await mongo_db.connect()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
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
