import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pytest
from pymongo import AsyncMongoClient, MongoClient

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn

os.environ["TESTING"] = "True"


@pytest.fixture(scope="module")
def module_monkeypatch():
    """MonkeyPatch fixture with module scope."""
    from _pytest.monkeypatch import MonkeyPatch

    monkey_patch = MonkeyPatch()
    yield monkey_patch
    monkey_patch.undo()  # Restore original values after module tests


@pytest.fixture(autouse=True, scope="module")
async def set_base_config_path_combined(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    # Reset DB client completely
    if hasattr(InternalConfig, "db_client"):
        delattr(InternalConfig, "db_client")
    print("InternalConfig initialized:", i_c)
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


async def drop_database(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    db_conn = DBConn(db_conn=conn_name, db_name=db_name, db_user=db_user)
    test_client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(
        db_conn.admin_uri, tz_aware=True
    )
    async with test_client:
        await test_client.drop_database(db_name)


def drop_database_sync(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    db_conn = DBConn(db_conn=conn_name, db_name=db_name, db_user=db_user)
    test_client: MongoClient[Dict[str, Any]] = MongoClient(db_conn.admin_uri, tz_aware=True)
    with test_client:
        test_client.drop_database(db_name)


def test_database_connection_init() -> None:
    db_conn = DBConn(db_conn="conn_bad", db_name="test_db", db_user="test_user")
    assert db_conn.db_conn == "conn_bad"
    assert db_conn.db_name == "test_db"
    assert db_conn.db_user == "test_user"
    assert db_conn.uri is not None
    assert "mongodb://" in db_conn.uri
    assert "admin" in db_conn.admin_uri


def test_default_connection() -> None:
    db_conn = DBConn()
    assert db_conn.db_conn == "conn_1"
    assert db_conn.db_name == "test_db"
    assert db_conn.db_user == "test_user"
    assert db_conn.uri is not None
    assert "mongodb://" in db_conn.uri
    assert "admin" in db_conn.admin_uri


@pytest.mark.asyncio
async def test_admin_database_connection_uri() -> None:
    db_conn = DBConn(db_conn="conn_1", db_name="test_db", db_user="test_user")
    await db_conn.test_connection(admin=True)


@pytest.mark.asyncio
async def test_fail_database_connection_uri() -> None:
    db_conn = DBConn(db_conn="conn_bad", db_name="test_db", db_user="test_user")
    with pytest.raises(ConnectionError):
        await db_conn.test_connection(timeout_seconds=0.01)


@pytest.mark.asyncio
async def test_database_connection_uri() -> None:
    db_conn = DBConn(db_conn="conn_1", db_name="test_db", db_user="test_user")
    await db_conn.setup_database()
    await db_conn.test_connection()
    await db_conn.setup_database()
    await drop_database(conn_name="conn_1", db_name="test_db", db_user="test_user")


timeseries_data = {
    "timestamp": datetime(2023, 10, 1, 0, 0, tzinfo=timezone.utc),
    "hive_usd": 0.2359,
    "sats_hbd": 1182.3551,
    "sats_hive": 278.9176,
    "hbd_usd": 1,
    "hive_hbd": 0.2362,
    "btc_usd": 84576.96,
    "sats_usd": 1182.3551,
}


@pytest.mark.asyncio
async def test_insert_timeseries() -> None:
    db_conn = DBConn(db_conn="conn_1", db_name="test_db", db_user="test_user")
    await db_conn.setup_database()

    client = db_conn.client()
    async with client:
        collection = client[db_conn.db_name]["rates"]
        await collection.insert_one(timeseries_data)

    # Test with a direct access to a collection
    collection2 = db_conn.db()["rates"]
    result = await collection2.find_one(
        {"timestamp": datetime(2023, 10, 1, 0, 0, tzinfo=timezone.utc)}
    )
    assert result is not None
    assert result["hive_usd"] == 0.2359
    await collection2.database.client.close()

    # Use a client from inside the collection
    collection3 = db_conn.db()["rates"]
    async with collection3.database.client:
        result2 = await collection3.find_one({})
        assert result2 is not None
        assert result2["hbd_usd"] == 1

    await drop_database(conn_name="conn_1", db_name="test_db", db_user="test_user")


@pytest.mark.asyncio
async def test_internal_config_database():
    db_conn = DBConn()
    await db_conn.setup_database()

    assert InternalConfig.db_client is not None
    assert InternalConfig.db is not None
    assert InternalConfig.db.name == db_conn.db_name

    db = InternalConfig.db
    collection_names = await db.list_collection_names()
    print(collection_names)

    await drop_database(conn_name="conn_1", db_name="test_db", db_user="test_user")


def test_insert_timeseries_sync() -> None:
    db_conn = DBConn(db_conn="conn_1", db_name="test_db", db_user="test_user")
    db_conn.setup_database_sync()

    client = db_conn.client_sync()
    with client:
        collection = client[db_conn.db_name]["rates"]
        collection.insert_one(timeseries_data)

    # Test with a direct access to a collection
    collection2 = db_conn.db_sync()["rates"]
    result = collection2.find_one({"timestamp": datetime(2023, 10, 1, 0, 0, tzinfo=timezone.utc)})
    assert result is not None
    assert result["hive_usd"] == 0.2359
    collection2.database.client.close()

    # Use a client from inside the collection
    collection3 = db_conn.db_sync()["rates"]
    with collection3.database.client:
        result2 = collection3.find_one({})
        assert result2 is not None
        assert result2["hbd_usd"] == 1

    drop_database_sync(conn_name="conn_1", db_name="test_db", db_user="test_user")
