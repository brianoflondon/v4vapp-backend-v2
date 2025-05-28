import asyncio
import os
from pathlib import Path
from typing import Generator
from unittest.mock import AsyncMock, patch

import pytest
from bson import json_util

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.hive_to_lightning import (
    HiveToLightningError,
    process_hive_to_lightning,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db import MongoDBClient, get_mongodb_client_defaults
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base

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


def get_op_dict() -> dict[str, OpAny]:
    """
    Get a dictionary of operations from the MongoDB dump.

    :return: Dictionary of operations keyed by trx_id.
    """
    op_list = list(load_hive_events_from_mongodb_dump(mongodb_export_path))
    op_dict = {}
    for op in op_list:
        op_dict[op.trx_id] = op
    return op_dict


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_hive_to_lightning_invoice_expired():
    """
    Test the Hive to Lightning processing but without attempting a refund.
    """
    TrackedBaseModel.last_quote = last_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()

    op_dict = get_op_dict()
    # create async_mock for return_hive_transfer
    with patch(
        "v4vapp_backend_v2.actions.hive_to_lightning.return_hive_transfer",
        new_callable=AsyncMock,
    ) as mock_return_hive_transfer:
        if op := op_dict.get("a5f153f96ab572a8260703773d6c530d0dd86e41"):
            op.reply_id = None
            await process_hive_to_lightning(op)

        # Wait for all tasks to complete
        while asyncio.all_tasks():
            if len(asyncio.all_tasks()) == 1:
                break
            await asyncio.sleep(0.1)

        assert mock_return_hive_transfer.call_count == 1
        assert mock_return_hive_transfer.call_args_list[0][1]["op"] == op


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_hive_to_lightning_pay_refund():
    """
    Test the Hive to Lightning processing but this will make a refund, with nobroadcast set to True.
    """
    TrackedBaseModel.last_quote = last_quote()
    TrackedBaseModel.db_client = get_mongodb_client_defaults()
    LedgerEntry.db_client = TrackedBaseModel.db_client

    op_dict = get_op_dict()
    # This specific transaction is an expired lightning invoice so will trigger a refund.
    correct_reply_id = "222616bceed2f9abbc36ff990cca3cf2eeb80768"

    # How to patch notification bot to avoid sending real messages
    with patch("v4vapp_backend_v2.config.notification_protocol.NotificationBot") as mock_bot:
        bot_instance = mock_bot.return_value
        bot_instance.send_message = AsyncMock()
        if op := op_dict.get("a5f153f96ab572a8260703773d6c530d0dd86e41"):
            # first run with reply ID set to correct_reply_id
            op.reply_id = correct_reply_id
            # This will not raise an exception, but will not broadcast the transaction
            # either, as nobroadcast is set to True.
            with pytest.raises(
                HiveToLightningError,
                match=f"Operation already has a reply transaction: {op.reply_id}",
            ):
                await process_hive_to_lightning(op, nobroadcast=True)

            # Next run with reply_id set to None
            op.reply_id = None
            await process_hive_to_lightning(op, nobroadcast=True)

            await asyncio.sleep(0.2)  # Allow time for the task to complete
            # now reset the reply_id to correct_reply_id
            op.reply_id = correct_reply_id
            await op.save()

            # Wait for all tasks to complete
            while asyncio.all_tasks():
                if len(asyncio.all_tasks()) == 1:
                    break
                await asyncio.sleep(0.1)

        # assert mock_repay_hive_to_lightning.call_count == 1
        # assert mock_repay_hive_to_lightning.call_args_list[0][1]["op"] == op


# type: ignore[arg-type]


trx_json_str = '{"expiration": "2025-05-27T16:10:30", "ref_block_num": 53993, "ref_block_prefix": 4044532601, "operations": [["transfer", {"from": "devser.v4vapp", "to": "v4vapp-test", "amount": "10.000 HIVE", "memo": "Lightning invoice expired - 95911346_a5f153f96ab572a8260703773d6c530d0dd86e41_1_real - Thank you for using v4v.app"}]], "extensions": [], "signatures": ["1f379ef999379c1fc8503a149bf1616f54b3267c5a13e8ae3ec02faa08faa3748e769e3af1ef53554535aae79bbf514bad96c58d90e4190818d6e5114c743540d5"], "trx_id": "d9b771ebf9662a963f42b93c1ce702481f7700c8"}'
