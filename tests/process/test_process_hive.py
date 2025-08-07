import os
from datetime import datetime, timezone

import pytest
from nectar.amount import Amount

from tests.utils import clear_and_reset, close_all_db_connections
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.process.process_hive import process_transfer_op

if os.getenv("GITHUB_ACTIONS") == "true":
    pytest.skip("Skipping tests on GitHub Actions", allow_module_level=True)

"""
This module attempts to test the main monitoring and ledger generating parts of the stack by running them
as background processes and then running tests against them.
It includes fixtures for setup and teardown, as well as tests for various payment scenarios.

This must be run after the three watchers are running, as it relies on the watchers to generate the ledgers.


"""


@pytest.fixture(scope="module", autouse=True)
async def config_file():
    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    await close_all_db_connections()


async def test_just_clear():
    """
    Test to clear the database and reset the environment.
    This test clears the database and resets the environment to ensure a clean state for subsequent tests.
    """
    await clear_and_reset()
    print("Database cleared and reset.")


async def test_process_transfer_op_deposit_hive():
    await clear_and_reset()
    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )

    deposit_amount = Amount("12.00 HBD")
    customer_account = "v4vapp-test"

    tracked_op = Transfer(
        from_account=customer_account,
        to_account=server_account,
        memo="Deposit #sats",
        amount=deposit_amount,
        timestamp=datetime.now(timezone.utc),
        trx_id="fake_trx_id",
        op_type="transfer",
        block_num=123456,
    )

    ledger_entry_deposit = await process_transfer_op(tracked_op, nobroadcast=True)

    assert ledger_entry_deposit, "Failed to create ledger entry for deposit"
