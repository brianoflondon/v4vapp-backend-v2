from pathlib import Path
from random import choice, uniform

import pytest
from nectar.amount import Amount

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction
from v4vapp_backend_v2.process.process_resend_hive import resend_hive_transaction


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
        ).save()


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


