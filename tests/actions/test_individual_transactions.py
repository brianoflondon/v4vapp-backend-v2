import asyncio
import os
from pprint import pprint

import pytest

from tests.actions.test_full_stack import (
    clear_and_reset,
    close_all_db_connections,
    get_ledger_count,
    get_lightning_invoice,
    send_hive_customer_to_server,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    check_hive_conversion_limits,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    generate_balance_sheet_mongodb,
)
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.text_formatting import text_to_rtf

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
    Integration test for transferring funds between Hive and Lightning Network Daemon (LND).
    This test performs the following steps:
    1. Resets the test environment to a clean state.
    2. Generates a Lightning invoice for 10,000 satoshis with a specific memo.
    3. Sends 10,000 satoshis from a Hive customer to the server using the generated invoice.
    4. Waits for the ledger to record 13 entries, indicating all expected transactions have occurred.
    5. Asserts that exactly 13 ledger entries exist after the operations.
    Ensures the correct flow and ledger recording for Hive-to-LND and LND-to-Hive transactions.
    """
    await clear_and_reset()

    ledger_count = await get_ledger_count()
    limits_before = await check_hive_conversion_limits(hive_accname="v4vapp-test")

    invoice = await get_lightning_invoice(
        value_sat=10_000, memo="v4vapp.qrc | Your message goes here | #v4vapp"
    )
    assert invoice.payment_request, "Invoice payment request is empty"
    trx = await send_hive_customer_to_server(
        send_sats=10_000, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )
    assert trx.get("trx_id"), "Transaction failed to send"
    all_ledger_entries = await watch_for_ledger_count(ledger_count + 13)

    await asyncio.sleep(1)
    assert len(all_ledger_entries) == 13, "Expected 13 ledger entries"
    limits_after = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    limit_increase = limits_after[0].total_sats - limits_before[0].total_sats
    logger.info(f"Limit increase: {limit_increase} sats")
    assert limit_increase > 0, "Total sats should increase after the transaction"


async def test_check_conversion_limits():
    """
    Test to check the conversion limits for a specific customer.
    This test retrieves the conversion limits for the customer 'v4vapp-test'
    and asserts that the limits are greater than 0.
    """

    limits = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    assert limits, "Conversion limits should not be empty"
    pprint(limits)


async def test_hive_to_keepsats():
    ledger_count = await get_ledger_count()
    trx = await send_hive_customer_to_server(
        send_sats=15_000, memo="Deposit #sats #v4vapp", customer="v4vapp-test"
    )

    all_ledger_entries = await watch_for_ledger_count(ledger_count + 6)

    after_count = await get_ledger_count()
    print(
        f"Ledger count after transaction: {after_count} new entries: {after_count - ledger_count}"
    )


async def test_complete_balance_sheet_accounts_ledger():
    balance_sheet = await generate_balance_sheet_mongodb()
    balance_sheet_currencies_str = balance_sheet_all_currencies_printout(balance_sheet)
    complete_printout = f"{balance_sheet_currencies_str}\n"
    all_accounts = await list_all_accounts()
    for account in all_accounts:
        printout, details = await account_balance_printout(
            account=account,
            line_items=True,
        )
        complete_printout += "\n" + printout
    print(complete_printout)
    text_to_rtf(
        input_text=complete_printout,
        output_file="balance_sheet.rtf",
        max_lines_per_page=50,
        font_name="AndaleMono",
        font_size=10,
    )


# Last line of the file
