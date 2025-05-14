from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pytest
from bson import json_util

from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    balance_sheet_printout,
    generate_balance_sheet_pandas,
    get_account_balance_printout,
    get_ledger_dataframe,
    list_all_accounts,
)
from v4vapp_backend_v2.actions.tracked_all import process_tracked, tracked_any
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base

mongodb_export_path = Path("tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json")


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
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
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


@pytest.mark.asyncio
async def test_fill_ledger_database_from_mongodb_dump() -> None:
    """
    Test loading hive events from a MongoDB dump file.

    :param file_path: Path to the JSONL file.
    """
    file_path = Path("tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json")
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    for op in load_hive_events_from_mongodb_dump(file_path):
        _ = await process_tracked(op)


def test_print_block_numbers_of_events() -> None:
    """
    Print block numbers of events from a JSONL file.

    :param file_path: Path to the JSONL file.
    """
    file_path = Path("tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json")
    block_numbers = []
    for op in load_hive_events_from_mongodb_dump(file_path):
        block_numbers.append(op.block_num)
    print("[")
    for block_number in block_numbers:
        print(f"'{block_number}',")
    print("]")


async def fill_ledger_database_from_log() -> None:
    """
    Fill the ledger database with data from a JSONL file.

    :param file_path: Path to the JSONL file.
    """
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    for hive_event in load_hive_events_from_mongodb_dump(mongodb_export_path):
        hive_event["update_conv"] = False
        op_tracked = tracked_any(hive_event)
        assert op_tracked.type == op_tracked.name()
        _ = await process_tracked(op_tracked)


@pytest.mark.asyncio
async def test_balance_sheet_steps():
    """
    Test balance sheet in steps one by one
    """
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    count = 0
    for op in load_hive_events_from_mongodb_dump(mongodb_export_path):
        hive_event = op.model_dump()
        count += 1
        hive_event["update_conv"] = False
        op_tracked = tracked_any(hive_event)
        print(f"\n\n\nEvent {count=} {op_tracked.d_memo}")
        ledger_entry = await process_tracked(op_tracked)
        print(ledger_entry.print_journal_entry())
        df = await get_ledger_dataframe()
        balance_sheet_pandas = await generate_balance_sheet_pandas(
            df, reporting_date=datetime.now(tz=timezone.utc)
        )
        all_currencies = balance_sheet_all_currencies_printout(balance_sheet_pandas)
        balance_sheet_print = balance_sheet_printout(
            balance_sheet_pandas, datetime.now(tz=timezone.utc)
        )
        if not balance_sheet_pandas["is_balanced"]:
            print(all_currencies)
            print(balance_sheet_print)
            print(f"***********The balance sheet is not balanced. {count}************")
            assert False

        # print(f"The balance sheet is balanced. {count}")
        # if count != 8:
        #     collection = await TrackedBaseModel.db_client.get_collection("ledger")
        #     await collection.delete_many({})

    # await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_process_tracked_and_balance_sheet():
    """
    Test the process of generating a tracked balance sheet and its printouts.
    This test performs the following steps:
    1. Populates the ledger database with data from a MongoDB dump.
    2. Generates a balance sheet in pandas DataFrame format.
    3. Prints the formatted balance sheet as of the current date.
    4. Prints the balance sheet for all currencies.
    5. Cleans up by dropping the test database and user.
    Steps:
    - Calls `test_fill_ledger_database_from_mongodb_dump` to populate the database.
    - Uses `generate_balance_sheet_pandas` to create the balance sheet.
    - Formats the balance sheet using `balance_sheet_printout` and prints it.
    - Prints all currencies using `balance_sheet_all_currencies_printout`.
    - Cleans up resources using `drop_collection_and_user`.
    Note:
    Ensure that the necessary test database and user are set up before running this test.
    """

    await test_fill_ledger_database_from_mongodb_dump()
    as_of_date = datetime.now(tz=timezone.utc)
    balance_sheet_pandas = await generate_balance_sheet_pandas()
    fbs = balance_sheet_printout(balance_sheet_pandas, as_of_date)
    print(fbs)

    all_currencies = balance_sheet_all_currencies_printout(balance_sheet_pandas)
    print(all_currencies)

    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_account_balances():
    await drop_collection_and_user("conn_1", "test_db", "test_user")

    await test_fill_ledger_database_from_mongodb_dump()
    all_accounts = await list_all_accounts()
    for account in all_accounts:
        account_balances = await get_account_balance_printout(
            account=account, full_history=True, as_of_date=datetime.now(tz=timezone.utc)
        )
        print(account_balances)

    await drop_collection_and_user("conn_1", "test_db", "test_user")
