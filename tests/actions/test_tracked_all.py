import json
from pathlib import Path
from typing import Dict, Generator

import pytest

from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.tracked_all import (
    formatted_balance_sheet,
    generate_balance_sheet,
    process_tracked,
    tracked_any,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient


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


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


def load_hive_events(file_path: str) -> Generator[Dict, None, None]:
    """
    Load hive events from a JSONL file.

    :param file_path: Path to the JSONL file.
    :return: List of hive events.
    """
    with open(file_path, "r") as f:
        for line in f:
            if "transfer" in line:
                yield json.loads(line)["transfer"]


async def test_process_tracked():
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    for hive_event in load_hive_events("tests/data/hive_models/ledger_actions_log.jsonl"):
        hive_event["update_conv"] = False
        op_tracked = tracked_any(hive_event)
        assert op_tracked.type == op_tracked.name()
        ledger_entry = await process_tracked(op_tracked)
        if isinstance(ledger_entry, LedgerEntry):
            print(ledger_entry.draw_t_diagram())

    balance_sheet = await generate_balance_sheet()
    fbs= formatted_balance_sheet(balance_sheet)
    print(fbs)

    await drop_collection_and_user("conn_1", "test_db", "test_user")
