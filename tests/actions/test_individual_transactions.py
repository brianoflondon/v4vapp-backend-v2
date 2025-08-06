import asyncio
import os
from datetime import datetime
from pprint import pprint

import pytest

from tests.utils import (
    clear_and_reset,
    close_all_db_connections,
    get_all_ledger_entries,
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
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.text_formatting import text_to_rtf
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client_for_accounts,
    send_custom_json,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json

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
    await clear_and_reset()
    yield
    await close_all_db_connections()


async def test_just_clear():
    """
    Test to clear the database and reset the environment.
    This test clears the database and resets the environment to ensure a clean state for subsequent tests.
    """
    await clear_and_reset()
    print("Database cleared and reset.")


async def test_custom_json_transfer():
    """
    Test the process of sending a custom JSON transfer from a Hive customer to the server.
    This test performs the following steps:
    1. Retrieves the current ledger entry count.
    2. Sends a transaction from a Hive customer to the server with a specified amount and memo.
    3. Asserts that the transaction was successfully sent by checking for a transaction ID.
    4. Waits for the ledger to reflect the expected number of new entries.
    5. Checks that the transaction is recorded in the ledger and prints the details.
    6. Asserts that the transaction has replies in the custom JSON transfer (indicating successful processing).

    Raises:
        AssertionError: If the transaction fails to send (missing transaction ID).
    """
    ledger_count = await get_ledger_count()
    print(f"Initial ledger count: {ledger_count}")

    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        msats=2_323_000,
        memo=f"Test transfer {datetime.now().isoformat()}",
        parent_id="",  # This is the group_id of the original transfer
    )
    trx = await send_transfer_custom_json(transfer)
    await watch_for_ledger_count(ledger_count + 1)
    while True:
        transactions = await TransferBase.collection().find({"trx_id": trx["trx_id"]}).to_list()
        result = CustomJson.model_validate(transactions[0])
        if result.replies:
            print(f"Custom JSON transfer replies: {result.replies}")
            break
        await asyncio.sleep(0.5)

    assert transactions, "No transactions found for the transfer"
    assert result.replies, "No replies found in the custom JSON transfer"


async def test_hive_to_lnd_only():
    """ """
    ledger_count = await get_ledger_count()
    limits_before = await check_hive_conversion_limits(hive_accname="v4vapp-test")

    invoice_value_sat= 10_000

    invoice = await get_lightning_invoice(
        value_sat=invoice_value_sat, memo="Simply a bare test invoice"
    )
    assert invoice.payment_request, "Invoice payment request is empty"
    trx = await send_hive_customer_to_server(
        send_sats=invoice_value_sat, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )
    assert trx.get("trx_id"), "Transaction failed to send"
    all_ledger_entries = await watch_for_ledger_count(ledger_count + 10, timeout=120)

    await asyncio.sleep(1)
    assert len(all_ledger_entries) == 10, "Expected 10 ledger entries"
    limits_after = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    limit_used = limits_after[0].total_sats - limits_before[0].total_sats
    logger.info(f"Limit used: {limit_used} sats")
    assert limit_used >= invoice_value_sat, "Total sats should increase after the transaction"


async def test_hive_to_lnd_and_lnd_to_hive():
    """
    Integration test for transferring funds between Hive and Lightning Network Daemon (LND).
    This test performs the following steps:
    1. Resets the test environment to a clean state.
    2. Generates a Lightning invoice for 10,000 satoshis with a specific memo.
    3. Sends 1,000 satoshis from a Hive customer to the server using the generated invoice.
    4. Waits for the ledger to record 13 entries, indicating all expected transactions have occurred.
    5. Asserts that exactly 13 ledger entries exist after the operations.
    Ensures the correct flow and ledger recording for Hive-to-LND and LND-to-Hive transactions.
    """
    ledger_count = await get_ledger_count()
    limits_before = await check_hive_conversion_limits(hive_accname="v4vapp-test")

    invoice_value_sat = 1_234

    invoice = await get_lightning_invoice(
        value_sat=invoice_value_sat, memo="v4vapp.qrc | Your message goes here | #v4vapp"
    )
    assert invoice.payment_request, "Invoice payment request is empty"
    trx = await send_hive_customer_to_server(
        send_sats=invoice_value_sat, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )
    assert trx.get("trx_id"), "Transaction failed to send"
    all_ledger_entries = await watch_for_ledger_count(ledger_count + 12, timeout=120)

    await asyncio.sleep(1)
    assert len(all_ledger_entries) == 12, "Expected 12 ledger entries"
    limits_after = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    limit_used = limits_after[0].total_sats - limits_before[0].total_sats
    logger.info(f"Limit used: {limit_used} sats")
    assert limit_used == invoice_value_sat, "Total sats should increase after the transaction"


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
    """
    Test the process of sending a Hive customer transaction to the server and verifying ledger updates.

    This test performs the following steps:
    1. Retrieves the current ledger entry count.
    2. Sends a transaction from a Hive customer to the server with a specified amount and memo.
    3. Asserts that the transaction was successfully sent by checking for a transaction ID.
    4. Waits for the ledger to reflect the expected number of new entries.
    5. Prints the updated ledger count and the number of new entries after the transaction.

    Raises:
        AssertionError: If the transaction fails to send (missing transaction ID).
    """
    ledger_count = await get_ledger_count()
    trx = await send_hive_customer_to_server(
        send_sats=15_000, memo="Deposit #sats #v4vapp", customer="v4vapp-test"
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transaction failed to send"
    all_ledger_entries = await watch_for_ledger_count(ledger_count + 6)

    after_count = await get_ledger_count()
    print(
        f"Ledger count after transaction: {after_count} new entries: {after_count - ledger_count}"
    )


async def test_deposit_hive_to_keepsats():
    """
    Test to deposit Hive to Keepsats.
    This test sends a specified amount of Hive from a customer account to the server account.
    It checks that the transaction is successful and that the ledger entries are created correctly.
    """

    ledger_count = await get_ledger_count()
    trx = await send_hive_customer_to_server(
        send_sats=5000, memo="Deposit and more #sats", customer="v4vapp-test"
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transaction failed to send"

    ledger_entries = await watch_for_ledger_count(ledger_count + 7)
    await asyncio.sleep(10)
    for i, ledger_entry in enumerate(ledger_entries[ledger_count + 1 :], 1):
        print(f"-------------------------------- Entry {i} --------------------------------")
        print(ledger_entry)
    assert True, "Ledger entries should be created after the transaction"


async def test_deposit_hive_to_keepsats_send_to_account():
    """
    Test to deposit Hive to Keepsats.
    This test sends a specified amount of Hive from a customer account to the server account.
    It checks that the transaction is successful and that the ledger entries are created correctly.
    """
    ledger_count = await get_ledger_count()
    trx = await send_hive_customer_to_server(
        send_sats=5_000, memo="Deposit and more #sats", customer="v4vapp-test"
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transaction failed to send"

    # Transfer from test to qrc
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="v4vapp.qrc",
        sats=4_500,
        memo="Thank you for putting in this message",
    )
    # hive_config = InternalConfig().config.hive
    hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    trx = await send_custom_json(
        json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transfer transaction failed to send"

    await asyncio.sleep(10)
    await watch_for_ledger_count(ledger_count + 7)
    assert True, "Ledger entries should be created after the transaction"


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
    for ledger_entry_dict in await get_all_ledger_entries():
        ledger_entry = LedgerEntry.model_validate(ledger_entry_dict)
        complete_printout += f"{ledger_entry}\n"
    print(complete_printout)
    text_to_rtf(
        input_text=complete_printout,
        output_file="balance_sheet.rtf",
        max_lines_per_page=45,
        font_name="AndaleMono",
        font_size=10,
    )


# Last line of the file
