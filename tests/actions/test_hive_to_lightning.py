import asyncio
import os
from pathlib import Path
from typing import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from bson import json_util

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.actions.hive_to_lightning import process_hive_to_lightning
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db import MongoDBClient, get_mongodb_client_defaults
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase

mongodb_export_path = "tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json"


async def drop_collection_and_user(conn_name: str, db_name: str, db_user: str) -> None:
    # Drop the collection and user
    async with MongoDBClient(conn_name, db_name, db_user) as test_client:
        if test_client.db is not None:
            ans = await test_client.db.drop_collection("startup_collection")
            assert ans.get("ok") == 1
            ans = await test_client.drop_user()
            assert ans.get("ok") == 1
    await drop_database(conn_name=conn_name, db_name=db_name)


async def drop_database(conn_name: str, db_name: str) -> None:
    async with MongoDBClient(conn_name) as admin_client:
        await admin_client.drop_database(db_name)


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("config/")
    test_config_filename = "devhive.config.yaml"
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.DEFAULT_CONFIG_FILENAME", test_config_filename
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    internal_config = InternalConfig(config_filename=test_config_filename)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


def load_hive_events_from_mongodb_dump(file_path: str) -> Generator[OpAny, None, None]:
    """
    Load hive events from a MongoDB collection.

    :param file_path: Path to the JSONL file.
    :return: List of hive events.
    """

    with open(file_path, "r") as f:
        raw_data = f.read()
        json_data = json_util.loads(raw_data)
    for hive_event in json_data:
        op = op_any_or_base(hive_event)
        yield op


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_hive_to_lightning_invoice_expired():
    """
    Test the Hive to Lightning processing.
    """
    TrackedBaseModel.last_quote = last_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()

    # Load hive events from the MongoDB dump
    op_list = list(load_hive_events_from_mongodb_dump(mongodb_export_path))

    # Process each hive event trx_id = "a5f153f96ab572a8260703773d6c530d0dd86e41"

    # create async_mock for repay_hive_to_lightning
    with patch(
        "v4vapp_backend_v2.actions.hive_to_lightning.repay_hive_to_lightning", new_callable=AsyncMock
    ) as mock_repay_hive_to_lightning:

        for op in op_list:
            if not isinstance(op, TransferBase):
                continue
            if op.trx_id == "a5f153f96ab572a8260703773d6c530d0dd86e41":
                print(op.d_memo)
                if op.d_memo.startswith("lnbc"):
                    await process_hive_to_lightning(op=op)

        # Wait for all tasks to complete
        while asyncio.all_tasks():
            if len(asyncio.all_tasks()) == 1:
                break
            await asyncio.sleep(0.1)

        assert mock_repay_hive_to_lightning.call_count == 1


# type: ignore[arg-type]
