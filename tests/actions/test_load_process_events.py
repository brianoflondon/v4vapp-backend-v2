"""
These tests test the filtering of tracked operations and the ledger population
and the generation of balance sheets.

The test data comes from a MongoDB dump of the v4vapp-dev.hive_ops collection.

"""

from pathlib import Path
from typing import Generator, List

import pytest
from bson import json_util

from v4vapp_backend_v2.actions.tracked_all import TrackedAny, tracked_any_filter
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient, get_mongodb_client_defaults


async def drop_collection_and_user(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    async with MongoDBClient(conn_name, db_name, db_user) as test_client:
        if test_client.db is None:
            raise ValueError(f"Database {db_name} does not exist.")
        # Check if the collection exists by listing collection names
        collection_names = await test_client.db.list_collection_names()
        assert isinstance(collection_names, list)
        ans = await test_client.drop_user()
        assert ans.get("ok") == 1
    await drop_database(conn_name=conn_name, db_name=db_name)


async def drop_database(conn_name: str, db_name: str) -> None:
    async with MongoDBClient(conn_name) as admin_client:
        await admin_client.drop_database(db_name)


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    # Mock asyncio.create_task
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


async def fill_rates_db():
    """
    Fill the rates database with quotes.
    This function is a placeholder for filling the rates database.
    It should be implemented to fetch and store quotes in the database.
    """
    # Placeholder for filling the rates database
    mongodb_export_path_rates = "tests/data/hive_models/mongodb/v4vapp-dev.rates.json"
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    with open(mongodb_export_path_rates, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    async with TrackedBaseModel.db_client as db_client:
        await db_client.insert_many("rates", json_data)


def load_tracked_ops_from_mongodb_dump(file_path: str | Path) -> Generator[TrackedAny, None, None]:
    """
    Load tracked operations from a MongoDB collection.

    :param file_path: Path to the JSONL file.
    :return: List of tracked operations.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"File {file_path} does not exist.")
    with open(file_path, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    for tracked_op in json_data:
        try:
            op = tracked_any_filter(tracked_op)
            yield op
        except ValueError as e:
            print(f"Ignoring operation: {e}")
            continue


@pytest.mark.asyncio
async def fill_test_database() -> List[TrackedAny]:
    all_data: List[TrackedAny] = []
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    await fill_rates_db()
    input_path = Path("tests/data/hive_models/mongodb/event_chain_testing")
    for file in input_path.glob("*.json"):
        # Load tracked operations from each JSON file in the directory
        for tracked_op in load_tracked_ops_from_mongodb_dump(file):
            # Process each tracked operation and insert it into the test database
            await tracked_op.save()
            # insert into the all_data list sorted by timestamp for the hive_ops and creation_data for the others
            all_data.append(tracked_op)

    # sort all_data by timestamp
    all_data.sort(key=lambda x: x.timestamp)
    return all_data


async def test_fill_test_database():
    """
    Test the fill_test_database function to ensure it populates the database correctly.
    """
    all_data = await fill_test_database()
    for tracked_op in all_data:
        print(
            f"{tracked_op.timestamp}"
        )
