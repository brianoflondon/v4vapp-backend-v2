import os
from pathlib import Path

import pytest
from pymongo.errors import OperationFailure

from v4vapp_backend_v2.database.db import DbErrorCode, MongoDBClient

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
async def test_admin_db_local_docker_container(set_base_config_path: None):
    """
    Test the connection to the Admin database. Relies on a docker container running
    a MongoDB instance on port 37017 as listed in the `tests/data/config/config.yaml`.
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
    await drop_database("conn_1", "test_db")


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

    # Using conn_2 which doesn't not ahave replica_set set to pyTest becausse that gives
    # a different error code
    with pytest.raises(OperationFailure) as e:
        async with MongoDBClient("conn_2", "test_db", "test_no_user") as _:
            pass
    assert e.value.code == DbErrorCode.NO_USER
    with pytest.raises(OperationFailure) as e2:
        async with MongoDBClient("conn_2", "test_no_db", "test_user") as _:
            pass
    assert e2.value.code == DbErrorCode.NO_DB
    with pytest.raises(OperationFailure) as e3:
        async with MongoDBClient("conn_2", "test_db", "test_user_no_password") as _:
            pass
    assert e3.value.code == DbErrorCode.NO_PASSWORD
    with pytest.raises(OperationFailure) as e4:
        async with MongoDBClient(
            "conn_missing", "test_db", "test_user_no_password"
        ) as _:
            pass
    assert e4.value.code == DbErrorCode.NO_CONNECTION
