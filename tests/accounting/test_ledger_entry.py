from pathlib import Path
from typing import Any, AsyncGenerator

import pytest
from pymongo.errors import DuplicateKeyError

from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base, op_query


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


async def get_all_ops() -> AsyncGenerator[OpAny, Any]:
    async with MongoDBClient(
        db_conn="conn_1",
        db_name="lnd_monitor_v2_voltage",
        db_user="lnd_monitor",
    ) as db_client:
        query = op_query(["fill_recurrent_transfer", "transfer"])
        collection = await db_client.get_collection("hive_ops")
        cursor = collection.find(query).sort("block_num", -1)
        async for doc in cursor:
            op = op_any_or_base(doc)
            yield op


params = {
    "hive_ops": op_any_or_base,
}


@pytest.mark.asyncio
async def test_ledger_entry_transfer():
    # Initialize the database client
    async with MongoDBClient(
        db_conn="conn_1",
        db_name="lnd_monitor_v2_voltage",
        db_user="lnd_monitor",
    ) as db_client:
        # Get the collection
        async for op in get_all_ops():
            if op.from_account == "v4vapp" or op.to_account == "v4vapp":
                ledger_entry = LedgerEntry()
                ledger_entry.customer_deposit(hive_op=op, server_account="v4vapp")
                print(op.log_str)
                try:
                    await db_client.update_one(
                        collection_name="ledger",
                        query={"group_id": ledger_entry.group_id},
                        update=ledger_entry.model_dump(),  # Ensure model_dump() is called correctly
                    )
                except DuplicateKeyError:
                    print("Duplicate key error")


@pytest.mark.asyncio
async def test_ledger_entry():
    client = MongoDBClient(
        db_conn="conn_1",
        db_name="lnd_monitor_v2_voltage",
        db_user="lnd_monitor",
    )
    collection = await client.get_collection("ledger")

    pipeline = [
        {
            "$match": {
                "$or": [
                    {
                        "debit.name": "Customer Deposits Hive",
                        "debit.sub": "v4vapp",
                    },
                    {
                        "credit.name": "Customer Deposits Hive",
                        "credit.sub": "v4vapp",
                    },
                ],
                # "unit": "hive",  # Ensure Hive transactions only
            }
        },
        {"$sort": {"timestamp": 1}},
        {
            "$project": {
                "date": "$timestamp",
                "description": 1,
                "amount": 1,
                "unit": 1,
                "debit": {
                    "$cond": [
                        {
                            "$and": [
                                {"$eq": ["$debit.name", "Customer Deposits Hive"]},
                                {"$eq": ["$debit.sub", "v4vapp"]},
                            ]
                        },
                        "$conv.hbd",
                        0,
                    ]
                },
                "credit": {
                    "$cond": [
                        {
                            "$and": [
                                {"$eq": ["$credit.name", "Customer Deposits Hive"]},
                                {"$eq": ["$credit.sub", "v4vapp"]},
                            ]
                        },
                        "$conv.hbd",
                        0,
                    ]
                },
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"timestamp": 1},
                "output": {
                    "balance": {
                        "$sum": {"$subtract": ["$credit", "$debit"]},
                        "window": {"documents": ["unbounded", "current"]},
                    }
                },
            }
        },
    ]
    # Print ledger table
    print(
        f"| {'Date':<19} | {'Description':<50} | {'Debit':>10} | {'Credit':>10} | {'Balance':>10} | {'Unit':<4} |"
    )
    print(f"|{'-' * 21}|{'-' * 52}|{'-' * 12}|{'-' * 12}|{'-' * 12}|{'-' * 5}|")

    async for entry in collection.aggregate(pipeline):
        date = entry["date"].strftime("%Y-%m-%d %H:%M:%S")
        desc = (
            (entry["description"][:47] + "...")
            if len(entry["description"]) > 47
            else entry["description"]
        )
        debit = f"{entry['debit']:>10.3f}" if entry["debit"] > 0 else f"{'':>10}"
        credit = f"{entry['credit']:>10.3f}" if entry["credit"] > 0 else f"{'':>10}"
        balance = f"{entry['balance']:>10.3f}"
        unit = entry["unit"]
        print(f"| {date:<19} | {desc:<50} | {debit} | {credit} | {balance} | {unit:<4} |")
