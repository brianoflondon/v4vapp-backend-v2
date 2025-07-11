from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pandas as pd
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
from v4vapp_backend_v2.actions.tracked_all import (
    LedgerEntryException,
    process_tracked,
    tracked_any,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OpBase

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
        hive_event["update_conv"] = False
        op = op_any_or_base(hive_event)
        yield op


@pytest.mark.asyncio
async def test_fill_ledger_database_from_mongodb_dump() -> pd.DataFrame:
    """
    Test loading hive events from a MongoDB dump file.
    """
    file_path = "tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json"
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    count = 0
    processed_count = 0
    for op in load_hive_events_from_mongodb_dump(file_path):
        count += 1
        try:
            ledger_entry = await process_tracked(op)
            if ledger_entry is not None:
                processed_count += 1
                print(f"Inserted ledger entry {count}: {ledger_entry.group_id}")
        except LedgerEntryException as e:
            print(f"Error processing tracked operation: {e}")
            continue
    df = await get_ledger_dataframe()
    assert len(df) == processed_count
    print(f"Total events processed: {count}")
    print(f"Total ledger entries created: {processed_count}")
    print(f"Total ledger entries in database: {len(df)}")
    return df


def test_print_block_numbers_of_events() -> None:
    """
    Print block numbers of events from a JSONL file.

    :param file_path: Path to the JSONL file.
    """
    file_path = "tests/data/hive_models/mongodb/v4vapp-dev.hive_ops.json"
    block_numbers = []
    for op in load_hive_events_from_mongodb_dump(file_path):
        block_numbers.append(op.block_num)
    print("[")
    for block_number in block_numbers:
        print(f"'{block_number}',")
    print("]")


@pytest.mark.asyncio
async def test_balance_sheet_steps():
    """
    Test balance sheet in steps one by one
    This also steps through the process of fetching each Hive Event and processing it.
    It generates a balance sheet in pandas DataFrame format and prints it.
    It also prints the formatted balance sheet as of the current date.
    """
    await OpBase.update_quote()
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    count = 0
    for op in load_hive_events_from_mongodb_dump(mongodb_export_path):
        hive_event = op.model_dump()
        count += 1
        hive_event["update_conv"] = False
        op_tracked = tracked_any(hive_event)
        print(f"\n\n\nEvent {count=} {op_tracked.log_str}")
        try:
            ledger_entry = await process_tracked(op_tracked)
        except LedgerEntryException as e:
            print(f"Expected error processing tracked operation: {e}")
            continue
        print(ledger_entry.print_journal_entry())
        df = await get_ledger_dataframe()
        balance_sheet_pandas = await generate_balance_sheet_pandas(
            df, reporting_date=datetime.now(tz=timezone.utc)
        )
        all_currencies = balance_sheet_all_currencies_printout(balance_sheet_pandas)
        balance_sheet_print = balance_sheet_printout(
            balance_sheet_pandas, as_of_date=datetime.now(tz=timezone.utc)
        )
        if not balance_sheet_pandas["is_balanced"]:
            print(f"***********The balance sheet is not balanced. {count}************")
            print(all_currencies)
            print(balance_sheet_print)
            assert False
    print(balance_sheet_print)
    print(all_currencies)
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
    # Must update quotes before running this test
    await OpBase.update_quote()
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    as_of_date = datetime.now(tz=timezone.utc)
    df = await test_fill_ledger_database_from_mongodb_dump()
    balance_sheet_pandas = await generate_balance_sheet_pandas(df=df, reporting_date=as_of_date)

    all_currencies = balance_sheet_all_currencies_printout(balance_sheet_pandas)
    print(all_currencies)
    fbs = balance_sheet_printout(balance_sheet=balance_sheet_pandas, as_of_date=as_of_date)
    print(fbs)

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
