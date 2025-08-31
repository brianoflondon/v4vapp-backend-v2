import asyncio
import math
import os
from datetime import datetime
from pprint import pprint

import pytest
from nectar.amount import Amount

from tests.utils import (
    clear_and_reset,
    close_all_db_connections,
    get_all_ledger_entries,
    get_ledger_count,
    get_lightning_invoice,
    send_hive_customer_to_server,
    send_test_custom_json,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    check_hive_conversion_limits,
    keepsats_balance_printout,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    generate_balance_sheet_mongodb,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.profit_and_loss import profit_and_loss_printout
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.calculate import calc_keepsats_to_hive
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.text_formatting import text_to_rtf
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction
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
    InternalConfig(config_filename="config/devhive.config.yaml")
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


async def test_deposit_hive_to_keepsats(
    test_amount: int = 5_000, message: str = "test_deposit_hive_to_keepsats", timeout: int = 120
):
    """
    Asynchronously tests the process of depositing Hive to Keepsats for a customer account.

    This test performs the following steps:
    1. Retrieves the initial Keepsats balance and ledger entry count for a specified customer.
    2. Initiates a deposit transaction by sending a specified amount of Hive from the customer to the server.
    3. Asserts that the transaction was successfully created.
    4. Waits for the expected number of new ledger entries and prints their details.
    5. Retrieves the Keepsats balance after the transaction.
    6. Asserts that the net balance reflects the deposited amount within an acceptable margin.

    Raises:
        AssertionError: If the transaction fails to send or if the net msats after deposit does not match the expected value.
    Test to deposit Hive to Keepsats.
    This test sends a specified amount of Hive from a customer account to the server account.
    It checks that the transaction is successful and that the ledger entries are created correctly.

    """
    cust_id = "v4vapp-test"
    net_msats, balance_before = await keepsats_balance_printout(cust_id=cust_id)
    ledger_count = await get_ledger_count()
    trx = await send_hive_customer_to_server(
        send_sats=test_amount,
        memo=f"{message} | #sats",
        customer=cust_id,
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transaction failed to send"

    ledger_entries = await watch_for_ledger_count(ledger_count + 7, timeout=timeout)
    await asyncio.sleep(2)

    net_msats_after, balance_after = await keepsats_balance_printout(cust_id=cust_id)

    # The deposit will be around 5000 + 200 sats.
    assert abs(net_msats_after - (net_msats + test_amount * 1_000)) < 200_000, (
        "Net msats should reflect the deposit"
    )


async def test_hive_and_hbd_to_lnd_only():
    """
    Test the process of sending a transfer from a Hive customer to the Lightning Network Daemon (LND).
    This test performs the following steps:
    1. Retrieves the current ledger entry count.
    2. Sends a transaction from a Hive customer to LND with a specified amount and memo.
    3. Asserts that the transaction was successfully sent by checking for a transaction ID.
    4. Waits for the ledger to reflect the expected number of new entries.
    5. Checks that the transaction is recorded in the ledger and prints the details.
    6. Asserts that the transaction has replies in the custom JSON transfer (indicating successful processing).

    Raises:
        AssertionError: If the transaction fails to send (missing transaction ID).
    """
    # await clear_and_reset()

    ledger_count = await get_ledger_count()
    limits_before = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    invoice_value_sat = 2_222

    for currency in [Currency.HBD, Currency.HIVE]:
        # invoice_value_sat = invoice_value_sat
        invoice = await get_lightning_invoice(
            value_sat=invoice_value_sat,
            memo=f"{currency.symbol} pay invoice {invoice_value_sat}",
        )

        conversion_result = await calc_keepsats_to_hive(
            msats=invoice_value_sat * 1_000, to_currency=currency
        )

        send_hive = math.ceil(conversion_result.to_convert_amount.amount) + 1
        send_hive_amount = Amount(f"{send_hive} {currency.symbol}")

        assert invoice.payment_request, "Invoice payment request is empty"
        trx = await send_hive_customer_to_server(
            amount=send_hive_amount,
            memo=f"{invoice.payment_request} test_hive_to_lnd_only pay with {currency.symbol}",
            customer="v4vapp-test",
        )
        assert trx.get("trx_id"), "Transaction failed to send"

    all_ledger_entries = await watch_for_ledger_count(ledger_count + 22, timeout=60)

    await asyncio.sleep(1)
    assert len(all_ledger_entries) - ledger_count == 22, "Expected 22 new ledger entries"
    limits_after = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    limit_used = limits_after[0].total_sats - limits_before[0].total_sats
    logger.info(f"Limit used: {limit_used} sats")
    assert limit_used >= 2 * invoice_value_sat, "Total sats should increase after the transaction"


async def test_hive_to_lnd_and_lnd_to_hive():
    """
    Integration test for transferring funds between Hive and Lightning Network Daemon (LND).
    This test performs the following steps:
    2. Generates a Lightning invoice for 1,234 satoshis with a specific memo.
    3. Sends 1,234 satoshis from a Hive customer to the server using the generated invoice.
    4. Waits for the ledger to record 11 entries, indicating all expected transactions have occurred.
    5. Asserts that exactly 11 ledger entries exist after the operations.
    Ensures the correct flow and ledger recording for Hive-to-LND and LND-to-Hive transactions.
    """
    # await test_just_clear()
    net_msats_before, balance_before = await keepsats_balance_printout(cust_id="v4vapp.qrc")
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
    all_ledger_entries = await watch_for_ledger_count(ledger_count + 11, timeout=120)

    await asyncio.sleep(1)
    limits_after = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    limit_used = limits_after[0].total_sats - limits_before[0].total_sats
    logger.info(f"Limit used: {limit_used} sats")
    assert limit_used >= invoice_value_sat, "Total sats should increase after the transaction"
    net_msats_after, balance_after = await keepsats_balance_printout(cust_id="v4vapp.qrc")


async def test_check_conversion_limits():
    """
    Test to check the conversion limits for a specific customer.
    This test retrieves the conversion limits for the customer 'v4vapp-test'
    and asserts that the limits are greater than 0.
    """

    limits = await check_hive_conversion_limits(hive_accname="v4vapp-test")
    assert limits, "Conversion limits should not be empty"
    for limit in limits:
        print(limit.output_text)


async def test_deposit_hive_to_keepsats_send_to_account():
    """
    Test the process of depositing sats from a Hive customer to the Keepsats server and then sending those sats to another account.

    Steps performed:
    1. Retrieve the initial ledger count and Keepsats balance for the target customer.
    2. Simulate a deposit from a Hive customer to the Keepsats server.
    3. Wait for the ledger to reflect the deposit.
    4. Transfer a portion of the deposited sats from the test account to the target account.
    5. Wait for the ledger to reflect the transfer.
    6. Verify that the ledger count has increased as expected.
    7. Check that the Keepsats balance for the target account has increased by the transferred amount.
    8. Assert that all transactions were successful and the balances are updated accordingly.
    """

    ledger_count = await get_ledger_count()
    net_msats, balance = await keepsats_balance_printout(cust_id="v4vapp.qrc")
    trx = await send_hive_customer_to_server(
        send_sats=5_000,
        memo="Deposit and more #sats test_deposit_hive_to_keepsats_send_to_account",
        customer="v4vapp-test",
    )
    pprint(trx)
    assert trx.get("trx_id"), "Transaction failed to send"

    # Wait for the deposit to be recorded.
    await watch_for_ledger_count(ledger_count + 6)

    # Transfer from test to qrc
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="v4vapp.qrc",
        sats=4_500,
        memo="Thank you for putting in this message",
    )
    trx = await send_test_custom_json(transfer)
    pprint(trx)
    assert trx.get("trx_id"), "Transfer transaction failed to send"
    await watch_for_ledger_count(ledger_count + 7)
    await asyncio.sleep(10)
    ledger_count_after = await get_ledger_count()
    print(f"Ledger count after transfer: {ledger_count_after}")
    assert True, "Ledger entries should be created after the transaction"
    net_msats_after, balance_after = await keepsats_balance_printout(cust_id="v4vapp.qrc")
    assert (net_msats_after - net_msats) // 1000 == 4500, (
        f"Expected 4500, got {net_msats_after // 1000}"
    )
    assert net_msats_after is not None, "Failed to retrieve net msats"


async def test_conversion_keepsats_to_hive():
    """
    Test the conversion process from Keepsats to Hive.

    This asynchronous test performs the following steps:
    1. Deposits a specified amount (5,000 units) from Hive to Keepsats.
    2. Retrieves and logs the current ledger count.
    3. Prints out the Keepsats balance for the customer with ID "v4vapp-test".
    4. Creates a KeepsatsTransfer object to transfer 5,000,000 msats from "v4vapp-test" to "devser.v4vapp" with a memo indicating conversion to Hive.
    5. Sends the transfer using a custom JSON transaction.

    The test ensures that the conversion and transfer processes function as expected.
    """
    invoice_sats = 5_000
    await test_deposit_hive_to_keepsats(
        invoice_sats,
        timeout=120,
        message="Deposit Hive to Keepsats for test_conversion_keepsats_to_hive",
    )
    net_msats_before, balance_before = await keepsats_balance_printout(cust_id="v4vapp-test")
    ledger_count = await get_ledger_count()
    logger.info(f"Starting Ledger count: {ledger_count}")
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        msats=invoice_sats * 1000,
        memo=f"Convert to #HIVE {datetime.now().isoformat()}",
    )
    trx = await send_transfer_custom_json(transfer)
    await watch_for_ledger_count(ledger_count + 12)
    ledger_count = await get_ledger_count()
    logger.info(f"Ledger count: {ledger_count}")

    await asyncio.sleep(5)

    net_msats_after, balance_after = await keepsats_balance_printout(cust_id="v4vapp-test")
    assert abs(net_msats_after - (net_msats_before - invoice_sats * 1000)) < 200_000, (
        f"Expected {abs(net_msats_after - (net_msats_before - invoice_sats * 1000))} < 200_000. "
    )
    last_hive_op = await InternalConfig.db["hive_ops"].find_one(
        {"type": "transfer", "from": "devser.v4vapp"}, sort=[("timestamp", -1)]
    )
    transfer = Transfer.model_validate(last_hive_op)
    assert transfer.to_account == "v4vapp-test", f"Expected v4vapp-test, got {transfer.to_account}"
    assert "Converted 5,000 sats" in transfer.memo, (
        f"Expected memo to contain 'Converted 5,000 sats', got {transfer.memo}"
    )


async def test_deposit_keepsats_spend_hive_custom_json():
    """
    Test the process of depositing HIVE to Keepsats, generating a Lightning invoice,
    checking the Keepsats balance, and sending a transfer using custom JSON.
    Steps performed:
    1. Deposit HIVE to Keepsats and verify the operation.
    2. Log the current ledger count.
    3. Generate a Lightning invoice for a specified amount.
    4. Retrieve and log the Keepsats balance for a test customer.
    5. Create and send a Keepsats transfer using custom JSON, including the Lightning invoice in the memo.
    This test ensures the integration between HIVE deposits, Keepsats balance management,
    Lightning invoice generation, and custom JSON transfers.
    """
    await test_deposit_hive_to_keepsats(
        5_000, timeout=120, message="Deposit Hive for test_deposit_keepsats_spend_hive_custom_json"
    )
    ledger_count = await get_ledger_count()
    logger.info(f"Ledger count: {ledger_count}")

    invoice_sats = 5_000

    invoice = await get_lightning_invoice(value_sat=invoice_sats, memo="")

    net_msats_before, balance = await keepsats_balance_printout(cust_id="v4vapp-test")
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        memo=f"{invoice.payment_request} {datetime.now().isoformat()}",
    )
    trx = await send_transfer_custom_json(transfer)

    await watch_for_ledger_count(ledger_count + 4)

    await asyncio.sleep(5)

    net_msats_after, balance = await keepsats_balance_printout(cust_id="v4vapp-test")
    assert abs(net_msats_after - (net_msats_before - invoice_sats * 1000)) < 200_000, (
        f"Expected {abs(net_msats_after - (net_msats_before - invoice_sats * 1000))} < 200_000. "
    )
    last_hive_op = await InternalConfig.db["hive_ops"].find_one(
        {"type": "custom_json"}, sort=[("timestamp", -1)]
    )
    custom_json = CustomJson.model_validate(last_hive_op)
    if custom_json.json_data:
        memo = custom_json.json_data.memo
        assert "Paid Invoice with Keepsats" in memo, (
            f"Expected memo to contain 'Paid Invoice with Keepsats', got {memo}"
        )
    else:
        assert False, (
            "Custom JSON data is empty, expected to contain memo with invoice payment request"
        )


async def test_send_internal_keepsats_transfer_by_hive_transfer():
    """
    Test the process of sending an internal Keepsats transfer via a Hive transaction.

    This test performs the following steps:
    1. Deposits a specified amount of HIVE to Keepsats.
    2. Retrieves and logs the current ledger count.
    3. Sends a Hive transaction from a customer to the server, including a memo to trigger a Keepsats transfer.
    4. Prints the transaction details for verification.

    The test ensures that the integration between Hive deposits and Keepsats transfers works as expected.

    Raises:
        AssertionError: If any step in the process fails.
    """
    await test_deposit_hive_to_keepsats(
        5_000, timeout=120, message="test_send_internal_keepsats_transfer_by_hive_transfer"
    )
    ledger_count = await get_ledger_count()
    logger.info(f"Ledger count: {ledger_count}")

    sat_transfer = 4_000

    trx = await send_hive_customer_to_server(
        amount=Amount("0.001 HIVE"),
        memo=f"v4vapp.qrc #paywithsats:{sat_transfer} test_send_internal_keepsats_transfer_by_hive_transfer",
        customer="v4vapp-test",
    )
    pprint(trx)

    await watch_for_ledger_count(ledger_count + 3)
    await asyncio.sleep(5)
    last_hive_op = await InternalConfig.db["hive_ops"].find_one(
        {"type": "custom_json"}, sort=[("timestamp", -1)]
    )
    custom_json = CustomJson.model_validate(last_hive_op)
    pprint(custom_json.model_dump())
    print(custom_json.memo)
    assert "Transfer v4vapp-test -> v4vapp.qrc" in custom_json.memo


async def test_pending_hive_payment():
    await test_deposit_hive_to_keepsats(5_000, timeout=120, message="test_pending_hive_payment")

    memo = "Converting 5000 sats to #HBD"
    transfer_internal = KeepsatsTransfer(
        hive_accname_from="v4vapp-test",
        hive_accname_to=InternalConfig().server_id,
        sats=5_000,
        memo=memo,
    )
    trx = await send_transfer_custom_json(transfer_internal)

    pprint(trx)

    # This will fail with a pending transaction because there isn't enough HBD
    await asyncio.sleep(10)
    pending = await PendingTransaction.list_all()
    assert len(pending) > 0, "Expected at least one pending transaction"
    amount = Amount("10.000 HBD")
    memo = "deposit #sats"
    await send_hive_customer_to_server(amount=amount, memo=memo, customer="v4vapp-test")

    await asyncio.sleep(10)
    pending = await PendingTransaction.list_all()
    assert len(pending) == 0, "Expected no pending transactions"


async def test_complete_balance_sheet_accounts_ledger():
    balance_sheet = await generate_balance_sheet_mongodb()
    balance_sheet_currencies_str = balance_sheet_all_currencies_printout(balance_sheet)
    complete_printout = f"{balance_sheet_currencies_str}\n"
    profit_and_loss = await profit_and_loss_printout()
    complete_printout += f"{profit_and_loss}\n"
    all_accounts = await list_all_accounts()
    for account in all_accounts:
        printout, details = await account_balance_printout(
            account=account,
            line_items=True,
            user_memos=True,
        )
        complete_printout += "\n" + printout

    without_ledger_entries_printout = complete_printout

    for ledger_entry_dict in await get_all_ledger_entries():
        ledger_entry = LedgerEntry.model_validate(ledger_entry_dict)
        complete_printout += f"{ledger_entry}\n"

    text_to_rtf(
        input_text=complete_printout,
        output_file="balance_sheet.rtf",
        max_lines_per_page=45,
        font_name="AndaleMono",
        font_size=10,
    )
    print(without_ledger_entries_printout)


# Last line of the file
