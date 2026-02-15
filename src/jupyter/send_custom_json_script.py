import asyncio
import getpass
import os
from pprint import pprint
from typing import Any

from google.protobuf.json_format import MessageToDict
from nectar.amount import Amount

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.hive_extras import (
    get_hive_client,
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient


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


async def main():
    """
    Main function to run the checks and print results.
    """
    db_conn = DBConn()
    await db_conn.setup_database()
    # Deposit Hive as Keepsats

    # trx = await send_hive_customer_to_server(
    #     amount=Amount("50 HIVE"), memo="Deposit and more #sats", customer="v4vapp-test"
    # )
    # pprint(trx)

    # Send Sats back to this node.
    # invoice = await get_lightning_invoice(
    #     5010, "v4vapp.qrc #v4vapp Sending sats to another account"
    # )
    invoice = await get_lightning_invoice(
        2312, "Sending sats to another account", connection_name="voltage"
    )
    print(invoice.payment_request)
    # the invoice_message has no effect if the invoice is generated and sent in the message.
    # It is only used when the invoice is generated lightning_address
    # Sats amount is the amount to send for a 0 value invoice OR the maximum amount to send
    transfer = KeepsatsTransfer(
        from_account="v4vapp.bol",
        to_account="devser.v4vapp",
        sats=0,
        memo=invoice.payment_request,
        invoice_message="brianoflondon #v4vapp Sending sats to another account",
    )
    # hive_config = InternalConfig().config.hive
    active_key = await asyncio.to_thread(
        getpass.getpass, "Enter the active key for the sending account (v4vapp.bol): "
    )
    hive_client = get_hive_client(keys=[active_key])
    # hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    trx = await send_custom_json(
        json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
        send_account=transfer.from_account,
        active=True,
        id="v4vapp_dev_transfer",
        hive_client=hive_client,
    )
    pprint(trx)

    transfer = KeepsatsTransfer(
        from_account="v4vapp-test",
        to_account="devser.v4vapp",
        sats=4455,
        memo="brianoflondon@walletofsatoshi.com #paywithsats",
        invoice_message="brianoflondon #v4vapp Sending sats to another account",
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

    # # Transfer from test to qrc
    # transfer = KeepsatsTransfer(
    #     to_account="v4vapp-test",
    #     from_account="v4vapp.qrc",
    #     msats=5500113,
    #     memo="back atcha",
    # )
    # # hive_config = InternalConfig().config.hive
    # hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
    # trx = await send_custom_json(
    #     json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
    #     send_account=transfer.from_account,
    #     active=True,
    #     id="v4vapp_dev_transfer",
    #     hive_client=hive_client,
    # )
    # pprint(trx)


if __name__ == "__main__":
    target_dir = "/Users/bol/Documents/dev/v4vapp/v4vapp-backend-v2/"
    os.chdir(target_dir)
    print("Current working directory:", os.getcwd())

    CONFIG = InternalConfig(config_filename="devhive.config.yaml").config

    asyncio.run(main())
