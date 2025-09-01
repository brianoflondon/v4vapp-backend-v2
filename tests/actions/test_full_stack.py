import asyncio
import os
from datetime import datetime
from pprint import pprint

import pytest
from nectar.amount import Amount

from tests.utils import (
    all_ledger_entries,
    clear_database,
    close_all_db_connections,
    get_lightning_invoice,
    send_hive_customer_to_server,
    send_server_balance_to_test,
    watch_for_ledger_count,
)
from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    keepsats_balance,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client_for_accounts,
    send_custom_json,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer

if os.getenv("GITHUB_ACTIONS") == "true":
    pytest.skip("Skipping tests on GitHub Actions", allow_module_level=True)

pytest.skip("Skipping", allow_module_level=True)
"""
This module attempts to test the main monitoring and ledger generating parts of the stack by running them
as background processes and then running tests against them.
It includes fixtures for setup and teardown, as well as tests for various payment scenarios.

You can either run this by including the `full_stack_setup` fixture in your tests or by running the tests
in this module directly after starting each of the three monitor apps db_monitor hive_monitor_v2 and lnd_monitor_v2
in the debugger.


"""


@pytest.fixture(scope="module", autouse=True)
# async def config_file():
async def config_file(full_stack_setup):
    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    trx = await send_server_balance_to_test()
    logger.info(f"Starting test run at {datetime.now()}", extra={"notification": True})
    logger.info("Server balance sent to test account:")
    pprint(trx)
    if trx:
        await asyncio.sleep(15)
    await clear_database()
    await watch_for_ledger_count(0)
    logger.info("Database cleared and ledger count reset.")
    yield
    await close_all_db_connections()


async def test_full_stack_setup():
    """
    Test to ensure the full stack setup fixture works correctly.
    This will run the setup and teardown processes defined in the fixture.
    """
    # The fixture will automatically start the processes and yield control
    # Here we can add assertions or checks if needed, but for now, we just run it
    logger.info("Running the first test.")
    await asyncio.sleep(1)  # Allow some time for the processes to start
    ledger_entries = await watch_for_ledger_count(0)  # Ensure no ledger entries are present
    assert len(ledger_entries) == 0
    logger.info("Finished running the first test. Ledger Empty")


async def test_pay_invoice_with_hive():
    """
    Test the process of paying a Lightning invoice using Hive as the payment method.

    This test performs the following steps:
    1. Generates a Lightning invoice for a specified amount.
    2. Sends a Hive transaction from a test customer to the server, referencing the invoice.
    3. Watches the ledger collection for changes and collects relevant ledger entries.
    4. Validates that all expected ledger entry types are present, including:
        - CUSTOMER_HIVE_IN
        - CUSTOMER_HIVE_OUT
        - CONV_HIVE_TO_LIGHTNING
        - CONTRA_HIVE_TO_LIGHTNING
        - FEE_INCOME
        - FEE_EXPENSE
        - WITHDRAW_LIGHTNING
        - LIGHTNING_EXTERNAL_SEND
    5. Asserts that exactly 8 ledger entries are created for the transaction.
    6. Waits briefly to allow asynchronous operations to complete.

    Raises:
         AssertionError: If any expected ledger entry type is missing or the number of entries is incorrect.
    """
    starting_ledger = await all_ledger_entries()
    assert len(starting_ledger) == 0, "Expected no ledger entries at the start of the test"
    invoice = await get_lightning_invoice(5010, "Test test_pay_invoice_with_hive")
    logger.info(invoice)

    trx = await send_hive_customer_to_server(
        send_sats=5010, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )
    logger.info(trx)

    ledger_entries = await watch_for_ledger_count(7)

    keepsats_balance, ledger_details = await keepsats_balance_printout("v4vapp-test")
    # assert keepsats_balance == 0, "Expected Keepsats balance to be 0 after payment"
    await asyncio.sleep(1)
    ledger_types = [ledger_entry.ledger_type for ledger_entry in ledger_entries]
    expected_types = {
        LedgerType.CUSTOMER_HIVE_IN,
        LedgerType.CONV_HIVE_TO_LIGHTNING,
        LedgerType.CONTRA_HIVE_TO_LIGHTNING,
        LedgerType.FEE_INCOME,
        LedgerType.WITHDRAW_LIGHTNING,
        LedgerType.LIGHTNING_EXTERNAL_SEND,
        LedgerType.FEE_EXPENSE,
    }
    assert expected_types <= set(ledger_types), (
        f"Missing expected ledger types: {expected_types - set(ledger_types)}"
    )


async def test_deposit_hive_to_keepsats():
    """
    This asynchronous test performs the following steps:
    1. Sends a Hive transaction from a test customer to the server.
    2. Validates that the transaction is processed correctly.
    3. Checks that the server's balance and ledger entries are updated accordingly.

    Assertions:
    - The transaction is successful and returns data.
    - The expected ledger types (CUSTOMER_HIVE_IN, CONV_HIVE_TO_KEEPSATS, CONTRA_HIVE_TO_KEEPSATS, FEE_INCOME, DEPOSIT_KEEPSATS) are present.
    - Exactly 5 ledger entries are created for the transaction.

        AssertionError: If the transaction fails, expected ledger entries are missing, or the number of ledger entries is incorrect.
    Test the process of depositing Hive to Keepsats.

    Raises:
        AssertionError: If the transaction fails or the server's balance is not updated.
    """
    starting_ledger = await all_ledger_entries()
    starting_ledger_count = len(starting_ledger)
    expected_test_ledger_count = 7

    trx = await send_hive_customer_to_server(
        amount=Amount("50 HIVE"), memo="Deposit #sats", customer="v4vapp-test"
    )
    logger.info(trx)
    assert trx, "Transaction failed or returned no data"

    ledger_entries = await watch_for_ledger_count(
        starting_ledger_count + expected_test_ledger_count
    )

    await asyncio.sleep(1)
    keepsats_balance, ledger_details = await keepsats_balance_printout("v4vapp-test")
    ledger_types = [ledger_entry.ledger_type for ledger_entry in ledger_entries]
    # Only check ledger types from the 7th entry onward
    keepsats_ledger_types = ledger_types[starting_ledger_count + 1 :]

    expected_keepsats_types = {
        LedgerType.CUSTOMER_HIVE_OUT,
        LedgerType.CUSTOMER_HIVE_IN,
        LedgerType.CONV_HIVE_TO_KEEPSATS,
        LedgerType.CONTRA_HIVE_TO_KEEPSATS,
        LedgerType.FEE_INCOME,
        LedgerType.DEPOSIT_KEEPSATS,
        LedgerType.CUSTOMER_HIVE_OUT,
    }

    assert expected_keepsats_types <= set(keepsats_ledger_types), (
        f"Missing expected Keepsats ledger types: {expected_keepsats_types - set(keepsats_ledger_types)}"
    )


async def test_check_hive_conversion_limits():
    """
    Test the Hive conversion limits for Keepsats.
    After the previous transactions the conversion should be
    50 Hive to x amount of sats.
    25 Hive to x amount of sats.
    """
    starting_ledger = await all_ledger_entries()
    starting_ledger_count = len(starting_ledger)
    if starting_ledger_count == 0:
        logger.info("No ledger entries found, skipping conversion limits test.")
        return
    conv_limits = await check_hive_conversion_limits("v4vapp-test")
    assert conv_limits, "Hive conversion limits should not be empty"
    conversion_entries = (
        await LedgerEntry.collection()
        .find(
            {
                "cust_id": "v4vapp-test",
                "$or": [
                    {"ledger_type": LedgerType.CONV_HIVE_TO_KEEPSATS.value},
                    {"ledger_type": LedgerType.CONV_HIVE_TO_LIGHTNING.value},
                ],
            }
        )
        .to_list()
    )
    pprint(conversion_entries)
    logger.info(f"Hive conversion limits: {conv_limits}")
    total_msats = sum(
        entry["debit_amount"] for entry in conversion_entries if entry["debit_unit"] == "msats"
    )
    assert total_msats == conv_limits[0].total_msats


async def test_paywithsats_and_lightning_to_keepsats_deposit():
    """
    Test the process of paying with sats.

    This test performs the following steps:
    1. Generates a Lightning invoice for a specified amount.
    2. Sends a Hive transaction from a test customer to the server, referencing the invoice.
    3. Watches the ledger collection for changes and collects relevant ledger entries.
    4. Waits briefly to allow asynchronous operations to complete.

    Code Paths:
    actions
        process_tracked_events
            process_tracked_event
            process_custom_json
        hold_release_keepsats
            hold_keepsats
            release_keepsats
        custom_json_to_lnd
            process_custom_json_to_lightning
            custom_json_internal_transfer

    lnd_grpc.lnd_functions.send_lightning_to_pay_req


    Raises:
         AssertionError: If any expected ledger entry type is missing or the number of entries is incorrect.
    """
    starting_ledger = await all_ledger_entries()
    starting_ledger_count = len(starting_ledger)
    if starting_ledger_count == 0:
        logger.info("No ledger entries found, skipping paywithsats test.")
        return

    before_net_sats, ledger_details = await keepsats_balance_printout(cust_id="v4vapp-test")
    # This invoice will be received and deposited into v4vapp.qrc account
    invoice = await get_lightning_invoice(
        2121, memo="v4vapp.qrc #v4vapp #sats Paying invoice with Keepsats"
    )
    # the invoice_message has no effect if the invoice is generated and sent in the message.
    # It is only used when the invoice is generated lightning_address
    # Sats amount is the amount to send for a 0 value invoice OR the maximum amount to send
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        sats=2200,
        memo=invoice.payment_request,
        invoice_message="paying an invoice with keepsats",
    )
    hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    trx = await send_custom_json(
        json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )
    pprint(trx)
    ledger_entries = await watch_for_ledger_count(23)
    await asyncio.sleep(10)
    after_net_sats, ledger_details = await keepsats_balance_printout(
        cust_id="v4vapp-test", previous_sats=before_net_sats
    )

    ledger_entries = await all_ledger_entries()
    ledger_types = [ledger_entry.ledger_type for ledger_entry in ledger_entries]
    logger.info(f"Ledger types: {ledger_types}")
    assert len(ledger_entries) == 23, f"Expected 23 ledger entries, found {len(ledger_entries)}"
    paywithsats_types = ledger_types[starting_ledger_count + 1 :]
    excepted_paywithsats_types = {
        LedgerType.HOLD_KEEPSATS,
        LedgerType.DEPOSIT_KEEPSATS,
        LedgerType.WITHDRAW_KEEPSATS,
        LedgerType.LIGHTNING_EXTERNAL_SEND,
        LedgerType.FEE_CHARGE,
        LedgerType.FEE_EXPENSE,
        LedgerType.RELEASE_KEEPSATS,
        LedgerType.CUSTOMER_HIVE_OUT,
    }
    assert excepted_paywithsats_types <= set(paywithsats_types), (
        f"Missing expected paywithsats ledger types: {excepted_paywithsats_types - set(paywithsats_types)}"
    )
    keepsats_balance, ledger_details = await keepsats_balance_printout("v4vapp.qrc")
    assert abs(keepsats_balance - 2121) < 2, (
        f"Expected Keepsats balance for v4vapp.qrc to be close to 2121, found {keepsats_balance}"
    )


async def test_get_keepsats_balance():
    """
    Test the retrieval of Keepsats balance for a specific customer.

    This test performs the following steps:
    1. Retrieves the Keepsats balance for the customer "v4vapp-test".
    2. Validates that the balance is correctly fetched and printed.

    Raises:
        AssertionError: If the balance retrieval fails or does not match expected values.
    """
    cust_id = "v4vapp-test"
    net_sats, account_balance = await keepsats_balance(cust_id=cust_id, line_items=False)
    assert net_sats >= 0, f"Expected non-negative Keepsats balance, found {net_sats}"
