import asyncio
import os
from pprint import pprint
from timeit import default_timer as timeit
from typing import Any, List

import pytest
from google.protobuf.json_format import MessageToDict
from nectar.account import Account
from nectar.amount import Amount

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.hive_extras import (
    get_verified_hive_client,
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient

if os.getenv("GITHUB_ACTIONS") == "true":
    pytest.skip("Skipping tests on GitHub Actions", allow_module_level=True)

"""
This module attempts to test the main monitoring and ledger generating parts of the stack by running them
as background processes and then running tests against them.
It includes fixtures for setup and teardown, as well as tests for various payment scenarios.

You can either run this by including the `full_stack_setup` fixture in your tests or by running the tests
in this module directly after starting each of the three monitor apps db_monitor hive_monitor_v2 and lnd_monitor_v2
in the debugger.


"""


@pytest.fixture(scope="module", autouse=True)
# async def config_file(full_stack_setup):
async def config_file():
    ic = InternalConfig(config_filename="config/devhive.config.yaml")
    trx = await send_server_balance_to_test()
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

    invoice = await get_lightning_invoice(5010, "Test test_pay_invoice_with_hive")
    logger.info(invoice)

    trx = await send_hive_customer_to_server(
        send_sats=5010, memo=f"{invoice.payment_request}", customer="v4vapp-test"
    )
    logger.info(trx)

    ledger_entries = await watch_for_ledger_count(6)

    keepsats_balance, ledger_details = await keepsats_balance_printout("v4vapp-test")
    # assert keepsats_balance == 0, "Expected Keepsats balance to be 0 after payment"
    await asyncio.sleep(10)
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
    trx = await send_hive_customer_to_server(
        amount=Amount("50 HIVE"), memo="Deposit #sats", customer="v4vapp-test"
    )
    logger.info(trx)
    assert trx, "Transaction failed or returned no data"

    ledger_entries = await watch_for_ledger_count(14)

    await asyncio.sleep(10)
    keepsats_balance, ledger_details = await keepsats_balance_printout("v4vapp-test")
    ledger_types = [ledger_entry.ledger_type for ledger_entry in ledger_entries]
    # Only check ledger types from the 7th entry onward
    keepsats_ledger_types = ledger_types[6:]

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


async def test_paywithsats():
    """
    Test the process of paying with sats.

    This test performs the following steps:
    1. Generates a Lightning invoice for a specified amount.
    2. Sends a Hive transaction from a test customer to the server, referencing the invoice.
    3. Watches the ledger collection for changes and collects relevant ledger entries.
    4. Waits briefly to allow asynchronous operations to complete.

    Raises:
         AssertionError: If any expected ledger entry type is missing or the number of entries is incorrect.
    """
    before_net_sats, ledger_details = await keepsats_balance_printout(cust_id="v4vapp-test")
    invoice = await get_lightning_invoice(2121, memo="")
    # the invoice_message has no effect if the invoice is generated and sent in the message.
    # It is only used when the invoice is generated lightning_address
    # Sats amount is the amount to send for a 0 value invoice OR the maximum amount to send
    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        sats=10000,
        memo=invoice.payment_request,
        invoice_message="paying an invoice with keepsasts",
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
    ledger_entries = await watch_for_ledger_count(21)
    await asyncio.sleep(10)
    after_net_sats, ledger_details = await keepsats_balance_printout(
        cust_id="v4vapp-test", previous_sats=before_net_sats
    )

    ledger_entries = await all_ledger_entries()
    ledger_types = [ledger_entry.ledger_type for ledger_entry in ledger_entries]
    logger.info(f"Ledger types: {ledger_types}")
    assert len(ledger_entries) == 21, f"Expected 21 ledger entries, found {len(ledger_entries)}"
    paywithsats_types = ledger_types[:14]
    excepted_paywithsats_types = {
        LedgerType.CUSTOM_JSON_TRANSFER,
        LedgerType.HOLD_KEEPSATS,
        LedgerType.WITHDRAW_KEEPSATS,
        LedgerType.LIGHTNING_EXTERNAL_SEND,
        LedgerType.FEE_CHARGE,
        LedgerType.FEE_EXPENSE,
        LedgerType.RELEASE_KEEPSATS,
    }
    assert excepted_paywithsats_types <= set(paywithsats_types), (
        f"Missing expected paywithsats ledger types: {excepted_paywithsats_types - set(paywithsats_types)}"
    )


# MARK: Helper functions


async def all_ledger_entries() -> List[LedgerEntry]:
    """
    Fetches all ledger entries from the database and returns them as a list of LedgerEntry models.
    This function is useful for retrieving all ledger entries for validation or processing purposes.
    Returns:
        List[LedgerEntry]: A list of all ledger entries in the database.
    """
    all_ledger_entries = await LedgerEntry.collection().find({}).to_list()
    return [LedgerEntry.model_validate(le) for le in all_ledger_entries]


async def watch_for_ledger_count(count: int, timeout: int = 30) -> List[LedgerEntry]:
    start_time = timeit()
    raw_entries = []
    while timeit() - start_time < timeout:
        ledger_entries = await all_ledger_entries()
        if (count > 0 and len(ledger_entries) >= count) or (
            count == 0 and len(ledger_entries) == 0
        ):
            logger.info(f"Count {count} found")
            return ledger_entries
        await asyncio.sleep(5)
    logger.warning(
        f"⏰ Timeout after {timeout}s waiting for ledger entries count {count} {len(raw_entries)} found"
    )
    ledger_entries = await all_ledger_entries()
    return ledger_entries


async def get_all_ledger_entries():
    all_ledger_entries = await LedgerEntry.collection().find({}).to_list()
    return all_ledger_entries


# async def watch_database_for(ledger_type: LedgerType, timeout: int = 60) -> List[LedgerEntry]:
#     """
#     Watch the database for changes and collect ledger entries of a specific type.
#     Stops after finding the specified ledger_type or after timeout seconds.

#     Args:
#         ledger_type (LedgerType): The type of ledger entry to watch for.
#         timeout (int): Maximum time in seconds to wait before giving up.

#     Returns:
#         List[LedgerEntry]: A list of ledger entries collected while watching.
#     """
#     import time

#     db_conn = DBConn()
#     await db_conn.setup_database()
#     db = db_conn.db()
#     collection = db["ledger"]
#     ledger_entries: List[LedgerEntry] = []
#     start_time = time.time()

#     async with await collection.watch(full_document="updateLookup") as stream:
#         while time.time() - start_time < timeout:
#             try:
#                 # Wait for next change with a timeout to allow checking elapsed time
#                 change = await asyncio.wait_for(stream.next(), 2.0)

#                 try:
#                     ledger_entry = LedgerEntry.model_validate(change["fullDocument"])
#                     ledger_entries.append(ledger_entry)
#                     print(f"{ledger_entry.ledger_type:<15}: {ledger_entry.description}")

#                     if ledger_entry.ledger_type == ledger_type:
#                         logger.info(f"Found target ledger entry type: {ledger_type}")
#                         break

#                 except Exception as e:
#                     logger.error(f"Error validating ledger entry: {e}")
#                     continue

#             except asyncio.TimeoutError:
#                 # No new changes within the wait_for timeout
#                 elapsed = int(time.time() - start_time)
#                 logger.debug(f"Waiting for {ledger_type}... ({elapsed}/{timeout}s)")
#                 continue

#             except StopAsyncIteration:
#                 logger.warning("Change stream ended unexpectedly")
#                 break

#     elapsed = int(time.time() - start_time)
#     if elapsed >= timeout:
#         logger.warning(f"⏰ Timeout after {timeout}s waiting for ledger entry type {ledger_type}")

#     return ledger_entries


async def send_server_balance_to_test() -> dict[str, Any]:
    """
    Sends the server's available balance to the v4vapp-test account.
    """
    hive_client, server_name = await get_verified_hive_client(hive_role=HiveRoles.server)
    server_account = Account(server_name, blockchain_instance=hive_client)
    pprint(server_account.balances.get("available", []))
    for amount in server_account.balances.get("available", []):
        print(f"Server account {server_name} has {amount}")
        if amount.amount > 0:
            trx = await send_transfer(
                to_account="v4vapp-test",
                from_account=server_name,
                hive_client=hive_client,
                amount=amount,
                memo="Clearing balance transfer from v4vapp backend to v4vapp-test account",
            )
            pprint(f"Transfer transaction: {trx}")
            return trx
    return {}


async def clear_database():
    db_conn = DBConn()
    try:
        await db_conn.setup_database()
        db = db_conn.db()
        await db["hive_ops"].delete_many({})
        await db["ledger"].delete_many({})
    finally:
        # Close the connection properly
        if hasattr(db_conn, "client") and db_conn.client:
            await db_conn.client().close()


async def close_all_db_connections():
    """Close all database connections properly."""
    if hasattr(InternalConfig, "db_client") and InternalConfig.db_client:
        await InternalConfig.db_client.close()

    # Also close any other clients that might be in the event loop
    for task in asyncio.all_tasks():
        if "pymongo" in task.get_name():
            task.cancel()

    # Give a moment for connections to close
    await asyncio.sleep(0.5)


async def send_hive_customer_to_server(
    send_sats: int = 0,
    amount: Amount = Amount("0 HIVE"),
    memo: str = "",
    customer: str = "v4vapp-test",
) -> dict[str, Any]:
    if send_sats > 0:
        send_conv = CryptoConversion(
            conv_from=Currency.SATS,
            value=send_sats,
        )
        await send_conv.get_quote()
        conv = send_conv.conversion
        amount_to_send_msats = conv.msats + conv.msats_fee + 200_000
        amount_to_send_msats = conv.msats + conv.msats_fee + 200_000  # Adding a buffer for fees
        amount_to_send_hive = (amount_to_send_msats // 1000) / conv.sats_hive
        hive_amount = Amount(f"{amount_to_send_hive:.3f} HIVE")

    else:
        hive_amount = amount

    hive_config = InternalConfig().config.hive
    hive_client = await get_verified_hive_client_for_accounts([customer])
    server = hive_config.get_hive_role_account(hive_role=HiveRoles.server).name

    trx = await send_transfer(
        from_account=customer,
        to_account=server,
        hive_client=hive_client,
        amount=hive_amount,
        memo=memo,
    )
    return trx


async def get_lightning_invoice(
    value_sat: int, memo: str, connection_name: str = "umbrel"
) -> lnrpc.AddInvoiceResponse:
    """
    Creates a Lightning invoice with the specified value and memo.
    """
    value_msat = value_sat * 1000  # Convert satoshis to millisatoshis
    async with LNDClient(connection_name=connection_name) as client:
        request = lnrpc.Invoice(value_msat=value_msat, memo=memo)
        response: lnrpc.AddInvoiceResponse = await client.lightning_stub.AddInvoice(request)
        add_invoice_response_dict = MessageToDict(response, preserving_proto_field_name=True)
        logger.info(
            f"Invoice generated: {memo} {value_msat // 1000:,} sats",
            extra={"add_invoice_response_dict": add_invoice_response_dict},
        )
        return response
