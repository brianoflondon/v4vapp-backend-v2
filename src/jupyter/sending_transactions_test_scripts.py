import asyncio
import os
from pprint import pprint
from typing import Any

from google.protobuf.json_format import MessageToDict
from nectar.account import Account
from nectar.amount import Amount

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    generate_balance_sheet_pandas_from_accounts,
)
from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_dataframe
from v4vapp_backend_v2.actions.hive_to_lightning import get_verified_hive_client
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.hive_extras import SendHiveTransfer, send_transfer, send_transfer_bulk
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient


async def send_server_to_customer() -> dict[str, Any]:
    """
    Sends a transaction from the server to the customer.
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
    """
    Clears the MongoDB database by dropping all collections.
    """
    db_conn = DBConn()
    await db_conn.setup_database()

    await asyncio.sleep(10)  # Wait for database operations to complete

    db = db_conn.db()
    await db["hive_ops"].delete_many({})
    await db["ledger"].delete_many({})


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


async def send_hive_customer_to_server(
    send_sats: int = 0, amount: Amount = Amount("0 HIVE"), memo: str = ""
) -> dict[str, Any]:
    if send_sats > 0:
        send_conv = CryptoConversion(
            conv_from=Currency.SATS,
            value=send_sats,
        )
        await send_conv.get_quote()
        conv = send_conv.conversion
        amount_to_send_msats = conv.msats + conv.msats_fee + 200_0000
        amount_to_send_msats = conv.msats + conv.msats_fee + 200_0000  # Adding a buffer for fees
        amount_to_send_hive = (amount_to_send_msats // 1000) / conv.sats_hive
        hive_amount = Amount(f"{amount_to_send_hive:.3f} HIVE")

    else:
        hive_amount = amount

    hive_config = InternalConfig().config.hive
    hive_client, customer = await get_verified_hive_client(hive_role=HiveRoles.customer)
    server = hive_config.get_hive_role_account(hive_role=HiveRoles.server).name

    trx = await send_transfer(
        from_account=customer,
        to_account=server,
        hive_client=hive_client,
        amount=hive_amount,
        memo=memo,
    )
    return trx


async def graceful_shutdown():
    await asyncio.sleep(3)


async def main():
    # await clear_database()

    # invoice = await get_lightning_invoice(5000, "Test Invoice")
    # pprint(invoice)
    # # Pay invoice with Hive transfer
    # trx = await send_hive_customer_to_server(send_sats=5000, memo=f"{invoice.payment_request}")
    # pprint(trx)

    # # Deposit Hive as Keepsats
    # trx = await send_hive_customer_to_server(amount=Amount("50 HIVE"), memo="Deposit some #sats")
    # pprint(trx)
    # trx = await send_hive_customer_to_server(amount=Amount("25 HIVE"), memo="Deposit and more #sats")
    # pprint(trx)
    # trx = await send_hive_customer_to_server(amount=Amount("25 HIVE"), memo="Deposit yet more #sats")
    # pprint(trx)
    # await asyncio.sleep(10)  # Wait for the transaction to be processed

    hive_config = InternalConfig().config.hive
    hive_client, customer = await get_verified_hive_client(hive_role=HiveRoles.customer)
    server = hive_config.get_hive_role_account(hive_role=HiveRoles.server).name

    # pay with keepsats
    transfer_list = []
    for sats in [1000, 1500, 1234, 2100, 5000]:
        invoice = await get_lightning_invoice(sats, f"Test {sats}")
        hive_transfer = SendHiveTransfer(
            from_account=customer,
            to_account=server,
            amount="0.001 HIVE",
            memo=f"{invoice.payment_request} #paywithsats {sats} sats",
        )
        transfer_list.append(hive_transfer)

    await send_transfer_bulk(
        hive_client=hive_client,
        transfer_list=transfer_list,
    )

    # await send_transfer(
    #     from_account=customer,
    #     to_account=server,
    #     hive_client=hive_client,
    #     amount=Amount("0.001 HIVE"),
    #     memo=f"Payment for invoice {invoice.payment_request}",
    #     transfer_list=transfer_list,
    # )

    # for transfer in transfer_list:
    #     trx = await send_hive_customer_to_server(amount=transfer.amount, memo=transfer.memo)

    # invoice = await get_lightning_invoice(3000, "Test Invoice with Keepsats")
    # trx = await send_hive_customer_to_server(
    #     amount=Amount("0.001 HIVE"), memo=f"{invoice.payment_request} #paywithsats"
    # )

    db_conn = DBConn()
    await db_conn.setup_database()
    ledger_df = await get_ledger_dataframe()
    balance_sheet_dict = await generate_balance_sheet_pandas_from_accounts(df=ledger_df)
    balance_sheet_currencies_str = balance_sheet_all_currencies_printout(balance_sheet_dict)

    print(balance_sheet_currencies_str)

    await graceful_shutdown()


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
