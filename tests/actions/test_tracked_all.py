import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator

import pytest

from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    balance_sheet_printout,
    generate_balance_sheet_pandas,
    get_account_balance,
    get_ledger_dataframe,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.tracked_all import process_tracked, tracked_any
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


def get_block_numbers_of_events(file_path: str) -> Generator[int, None, None]:
    """
    Get block numbers of events from a JSONL file.

    :param file_path: Path to the JSONL file.
    :return: List of block numbers.
    """
    with open(file_path, "r") as f:
        for line in f:
            if "transfer" in line:
                yield json.loads(line)["transfer"]["block_num"]


def test_print_block_numbers_of_events() -> None:
    """
    Print block numbers of events from a JSONL file.

    :param file_path: Path to the JSONL file.
    """
    block_numbers = get_block_numbers_of_events("tests/data/hive_models/ledger_actions_log.jsonl")
    print("[")
    for block_number in block_numbers:
        print(f"'{block_number}',")
    print("]")


async def fill_ledger_database() -> None:
    """
    Fill the ledger database with data from a JSONL file.

    :param file_path: Path to the JSONL file.
    """
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    for hive_event in load_hive_events("tests/data/hive_models/ledger_actions_log.jsonl"):
        hive_event["update_conv"] = False
        op_tracked = tracked_any(hive_event)
        assert op_tracked.type == op_tracked.name()
        ledger_entry = await process_tracked(op_tracked)
        if isinstance(ledger_entry, LedgerEntry):
            print(ledger_entry.draw_t_diagram())


@pytest.mark.asyncio
async def test_balance_sheet_steps():
    """
    Test balance sheet in steps one by one
    """
    TrackedBaseModel.db_client = MongoDBClient("conn_1", "test_db", "test_user")
    await drop_collection_and_user("conn_1", "test_db", "test_user")
    count = 0
    for hive_event in load_hive_events("tests/data/hive_models/ledger_actions_log.jsonl"):
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
    await fill_ledger_database()
    as_of_date = datetime.now(tz=timezone.utc)
    balance_sheet_pandas = await generate_balance_sheet_pandas()
    fbs = balance_sheet_printout(balance_sheet_pandas, as_of_date)
    print(fbs)

    all_currencies = balance_sheet_all_currencies_printout(balance_sheet_pandas)
    print(all_currencies)

    await drop_collection_and_user("conn_1", "test_db", "test_user")


@pytest.mark.asyncio
async def test_account_balances():
    await fill_ledger_database()
    all_accounts = await list_all_accounts()
    df = await get_ledger_dataframe()
    for account in all_accounts:
        account_balances = get_account_balance(
            df=df, account_name=account.get("name"), sub_account=account.get("sub")
        )
        print(account_balances)

    await drop_collection_and_user("conn_1", "test_db", "test_user")
