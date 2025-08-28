import asyncio
from pprint import pprint
from timeit import default_timer as timeit
from typing import Any, Dict, List
from uuid import uuid4

from google.protobuf.json_format import MessageToDict
from nectar.account import Account
from nectar.amount import Amount

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.hive_extras import (
    PendingTransaction,
    get_hive_client,
    get_verified_hive_client,
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
    send_transfer_bulk,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient

# MARK: Hive Helper functions


def fake_trx_id() -> str:
    """
    Generates a fake transaction ID for testing purposes.
    This function is used to create a unique identifier for transactions in tests.
    Returns:
        str: A fake transaction ID.
    """
    return uuid4().hex


def latest_block_num() -> int:
    """
    Retrieves the latest block number from the Hive blockchain.
    This function is useful for testing purposes to ensure that the latest block number is available.
    Returns:
        int: The latest block number.
    """
    hive_client = get_hive_client()
    dynamic_global_properties = hive_client.get_dynamic_global_properties()
    if not dynamic_global_properties:
        raise ValueError("Dynamic global properties not available.")
    return dynamic_global_properties["head_block_number"]


# MARK: DB Helper functions


async def all_ledger_entries() -> List[LedgerEntry]:
    """
    Fetches all ledger entries from the database and returns them as a list of LedgerEntry models.
    This function is useful for retrieving all ledger entries for validation or processing purposes.
    Returns:
        List[LedgerEntry]: A list of all ledger entries in the database.
    """
    ledger_entries_raw = await LedgerEntry.collection().find({}).to_list()
    if not ledger_entries_raw:
        logger.info("No ledger entries found in the database.")
        return []
    ledger_entries = []
    for entry in ledger_entries_raw:
        try:
            ledger_entry = LedgerEntry.model_validate(entry)
            ledger_entries.append(ledger_entry)
        except Exception as e:
            logger.error(
                f"Error validating ledger entry: {entry}. Error: {e}",
                extra={"notification": False, "entry": entry, "error": str(e)},
            )
            continue
    return ledger_entries


async def get_ledger_count() -> int:
    """
    Returns the count of all ledger entries in the database.
    This function is useful for checking the number of ledger entries present.
    Returns:
        int: The count of ledger entries in the database.
    """
    count = await LedgerEntry.collection().count_documents({})
    logger.info(f"Current ledger entry count: {count}")
    return count


async def watch_for_ledger_count(count: int, timeout: int = 30) -> List[LedgerEntry]:
    start_time = timeit()
    raw_entries = []
    while timeit() - start_time < timeout:
        ledger_count = await get_ledger_count()
        if (count > 0 and ledger_count >= count) or (count == 0 and ledger_count == 0):
            logger.info(f"Count {count} found")
            return await all_ledger_entries()
        await asyncio.sleep(5)
    logger.warning(
        f"⏰ Timeout after {timeout}s waiting for ledger entries count {count} {len(raw_entries)} found"
    )
    ledger_entries = await all_ledger_entries()
    return ledger_entries


async def get_all_ledger_entries():
    all_ledger_entries = await LedgerEntry.collection().find({}).to_list()
    return all_ledger_entries


async def send_server_balance_to_test() -> dict[str, Any]:
    """
    Sends the server's available balance to the v4vapp-test account.
    """
    hive_client, server_name = await get_verified_hive_client(hive_role=HiveRoles.server)
    server_account = Account(server_name, blockchain_instance=hive_client)
    pprint(server_account.balances.get("available", []))
    balances = server_account.balances.get("available", [])
    transfer_list = []
    for amount in balances:
        print(f"Server account {server_name} has {amount}")
        if amount.amount > 0:
            hive_transfer = PendingTransaction(
                to_account="v4vapp-test",
                from_account=server_name,
                amount=str(amount),
                memo="Clearing balance transfer from v4vapp backend to v4vapp-test account",
            )
            transfer_list.append(hive_transfer)
    trx = {}
    if transfer_list:
        trx = await send_transfer_bulk(hive_client=hive_client, transfer_list=transfer_list)
    return trx


async def send_test_custom_json(transfer: KeepsatsTransfer) -> Dict[str, Any]:
    hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    trx = await send_custom_json(
        json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )
    return trx


async def clear_and_reset():
    ledger_count = await get_ledger_count()
    trx = await send_server_balance_to_test()
    if trx:
        logger.info(f"Transaction sent: {trx}")
        await watch_for_ledger_count(ledger_count + 1)

    await clear_database()
    await watch_for_ledger_count(0)
    logger.info("Clearing Database.")


async def clear_database():
    db_conn = DBConn()
    try:
        await db_conn.setup_database()
        db = db_conn.db()
        await db["hive_ops"].delete_many({})
        await db["ledger"].delete_many({})
        await db["pending"].delete_many({})
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
    """
    Send a HIVE transfer from a customer account to the server account.
    Depending on the provided arguments, the function either:
    - Converts a given amount of sats (send_sats) to HIVE, including fees and a buffer, and sends that amount.
    - Sends a specified HIVE amount directly.
    Args:
        send_sats (int, optional): Amount in sats to convert and send as HIVE. Defaults to 0.
        amount (Amount, optional): Amount of HIVE to send if send_sats is 0. Defaults to Amount("0 HIVE").
        memo (str, optional): Memo to include with the transfer. Defaults to "".
        customer (str, optional): Hive account name of the customer sending the transfer. Defaults to "v4vapp-test".
    Returns:
        dict[str, Any]: The transaction result dictionary.
    """

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
        return response
