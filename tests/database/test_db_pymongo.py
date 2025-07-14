import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pytest
from pymongo import AsyncMongoClient

from v4vapp_backend_v2.database.db_pymongo import DBConn

os.environ["TESTING"] = "True"


@pytest.fixture(autouse=True)
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


async def drop_database(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    db_conn = DBConn(db_conn=conn_name, db_name=db_name, db_user=db_user)
    test_client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(
        db_conn.admin_uri, tz_aware=True
    )
    async with test_client:
        await test_client.drop_database(db_name)


def test_database_connection_init() -> None:
    db_conn = DBConn(db_conn="conn_bad", db_name="test_db", db_user="test_user")
    assert db_conn.db_conn == "conn_bad"
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


async def test_insert_timeseries() -> None:
    db_conn = DBConn(db_conn="conn_1", db_name="test_db", db_user="test_user")
    await db_conn.setup_database()

    client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(db_conn.uri, tz_aware=True)
    async with client:
        collection = client[db_conn.db_name]["rates"]
        await collection.insert_one(timeseries_data)

        result = await collection.find_one(
            {"timestamp": datetime(2023, 10, 1, 0, 0, tzinfo=timezone.utc)}
        )
        assert result is not None
        assert result["hive_usd"] == 0.2359

    await drop_database(conn_name="conn_1", db_name="test_db", db_user="test_user")
