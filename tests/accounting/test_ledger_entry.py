from pathlib import Path
from typing import Any, AsyncGenerator

import pytest

from v4vapp_backend_v2.accounting.account_type import AssetAccount, LiabilityAccount
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
                if op.to_account == "v4vapp":
                    ledger_entry = LedgerEntry(
                        group_id=op.group_id,
                        timestamp=op.timestamp,
                        description=op.d_memo,
                        amount=op.amount_decimal,
                        unit=op.unit,
                        conv=op.conv,
                        debit_account=AssetAccount(
                            name="Customer Hive Deposits",
                            sub=op.to_account,
                        ),
                        credit_account=LiabilityAccount(
                            name="Customer Hive Liability",
                            sub=op.from_account,
                        ),
                    )
                elif op.from_account == "v4vapp":
                    ledger_entry = LedgerEntry(
                        group_id=op.group_id,
                        timestamp=op.timestamp,
                        description=op.d_memo,
                        amount=op.amount_decimal,
                        unit=op.unit,
                        conv=op.conv,
                        debit_account=LiabilityAccount(
                            name="Customer Hive Liability",
                            sub=op.to_account,
                        ),
                        credit_account=AssetAccount(
                            name="Customer Hive Deposits",
                            sub=op.from_account,
                        ),
                    )
                print(op.log_str)
                print(ledger_entry)
                await db_client.insert_one(
                    collection_name="ledger",
                    document=ledger_entry.model_dump(),  # Ensure model_dump() is called correctly
                )
