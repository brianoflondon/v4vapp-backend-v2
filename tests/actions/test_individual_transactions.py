import asyncio
import os

import pytest

from tests.actions.test_full_stack import (
    clear_and_reset,
    close_all_db_connections,
    get_lightning_invoice,
    send_hive_customer_to_server,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn

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


async def test_hive_to_lnd_and_lnd_to_hive():
    """
    Test the full stack by sending a payment from Hive to LND and then back to Hive.
    This will test the entire flow of payments through the system.
    """
    await clear_and_reset()

    invoice = await get_lightning_invoice(
        value_sat=10_000, memo="v4vapp.qrc | Your message goes here | #v4vapp"
    )
    trx = await send_hive_customer_to_server(
        send_sats=10_000, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )

    all_ledger_entries = await watch_for_ledger_count(13)

    await asyncio.sleep(1)
    assert len(all_ledger_entries) == 13, "Expected 13 ledger entries"


# Last line of the file
