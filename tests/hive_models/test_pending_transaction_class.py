from pathlib import Path
from random import choice, uniform
from uuid import uuid4

import pytest
from nectar.amount import Amount

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive_models.pending_transaction_class import (
    PendingCustomJson,
    PendingTransaction,
)


@pytest.fixture(autouse=True)
async def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


def random_amount() -> Amount:
    value = round(uniform(2, 10), 3)
    symbol = choice(["HIVE", "HBD"])
    return Amount(f"{value:.3f} {symbol}")


async def test_store_pending():
    server_id = InternalConfig().server_id
    for n in range(10):
        amount = random_amount()
        memo = f"Test pending transaction {n}"
        store_pending = await PendingTransaction(
            from_account=server_id,
            to_account="v4vapp-test",
            amount=amount,
            memo=memo,
            nobroadcast=True,
            is_private=False,
            unique_key=f"unique_key_{uuid4()}",
        ).save()
        assert store_pending.id is not None

    pending_hive = await PendingTransaction.list_all_hive()
    pending_hbd = await PendingTransaction.list_all_hbd()
    print("Pending HIVE Transactions:")
    for pending in pending_hive:
        print(pending)

    print("----------------------")

    print("Pending HBD Transactions:")
    for pending in pending_hbd:
        print(pending)


async def test_list_all_pending():
    await test_store_pending()
    all_pending = await PendingTransaction.list_all()
    assert isinstance(all_pending, list)
    sum_hive = Amount("0.000 HIVE")
    sum_hbd = Amount("0.000 HBD")
    for pending in all_pending:
        print(pending)
        assert isinstance(pending, PendingTransaction)
        if pending.amount.symbol == "HIVE":
            sum_hive += pending.amount
        elif pending.amount.symbol == "HBD":
            sum_hbd += pending.amount
        else:
            raise ValueError(
                f"Unexpected currency in pending transaction: {pending.amount.symbol}"
            )
    print(f"Total HIVE: {sum_hive}, Total HBD: {sum_hbd}")

    totals = await PendingTransaction.total_pending()
    assert isinstance(totals, dict)
    assert "HIVE" in totals
    assert "HBD" in totals
    print(f"Total HIVE: {totals['HIVE']}, Total HBD: {totals['HBD']}")
    assert totals["HIVE"] == sum_hive
    assert totals["HBD"] == sum_hbd


async def test_clear_pending_transactions():
    await test_store_pending()
    all_pending = await PendingTransaction.list_all()
    assert len(all_pending) > 0
    for pending in all_pending:
        await pending.delete()
    all_pending_after = await PendingTransaction.list_all()
    assert len(all_pending_after) == 0


# New test: Store PendingCustomJson instances
async def test_store_pending_custom_json():
    for n in range(5):  # Fewer for simplicity
        json_data = {"key": f"value{n}", "number": n}
        memo_like = f"Test custom json {n}"  # Not a real memo, but for context
        pending_custom = PendingCustomJson(
            json_data=json_data,
            send_account="v4vapp-test",
            active=True,
            cj_id="v4vapp_transfer",
            unique_key=f"custom_json_unique_key_{n}_{uuid4()}",
        )
        await pending_custom.save()

    # List and print for verification
    all_custom = await PendingCustomJson.list_all()
    print("PendingCustomJson Transactions:")
    for custom in all_custom:
        print(custom)


# New test: List all PendingCustomJson and verify types
async def test_list_all_pending_custom_json():
    await test_store_pending_custom_json()  # Ensure some data exists
    all_custom = await PendingCustomJson.list_all()
    assert isinstance(all_custom, list)
    for custom in all_custom:
        print(custom)
        assert isinstance(custom, PendingCustomJson)
        assert custom.pending_type == "pending_custom_json"  # Fixed: was custom.type
        assert custom.json_data is not None  # Basic check


# New test: Clear PendingCustomJson instances
async def test_clear_pending_custom_json():
    await test_store_pending_custom_json()
    all_custom = await PendingCustomJson.list_all()
    assert len(all_custom) > 0
    for custom in all_custom:
        await custom.delete()
    all_custom_after = await PendingCustomJson.list_all()
    assert len(all_custom_after) == 0


# New test: Verify mixed types don't interfere (generic behavior)
async def test_mixed_pending_types():
    # Store one of each type
    pending_tx = PendingTransaction(
        from_account=InternalConfig().server_id,
        to_account="v4vapp-test",
        amount=Amount("5.000 HIVE"),
        memo="Mixed test",
        nobroadcast=True,
        is_private=False,
        unique_key=f"mixed_transaction_key_{uuid4()}",
    )
    await pending_tx.save()

    pending_custom = PendingCustomJson(
        json_data={"test": "mixed"},
        send_account="v4vapp-test",
        active=True,
        cj_id="v4vapp_transfer",
        unique_key=f"mixed_custom_json_key_{uuid4()}",
    )
    await pending_custom.save()

    # List each type separately (should only return the correct type due to generics and type filtering)
    tx_list = await PendingTransaction.list_all()
    custom_list = await PendingCustomJson.list_all()

    assert len(tx_list) >= 1
    assert len(custom_list) >= 1
    assert all(isinstance(tx, PendingTransaction) for tx in tx_list)
    assert all(isinstance(custom, PendingCustomJson) for custom in custom_list)

    # Clean up
    for tx in tx_list:
        await tx.delete()
    for custom in custom_list:
        await custom.delete()
