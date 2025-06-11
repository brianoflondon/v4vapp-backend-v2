import asyncio
from base64 import b64encode
from hashlib import sha256
from secrets import token_hex

import httpx
from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.invoices_pb2 as invoicesrpc
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient

config = InternalConfig().config


async def fetch_random_word() -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get("https://random-word-api.herokuapp.com/word")
        words = response.json()
        return words[0]


async def add_invoice(
    value: int, memo: str, connection_name: str = "voltage"
) -> lnrpc.AddInvoiceResponse:
    # https://lightning.engineering/api-docs/api/lnd/router/add-invoice/
    async with LNDClient(connection_name=connection_name) as client:
        request = lnrpc.Invoice(value=value, memo=memo)
        response: lnrpc.AddInvoiceResponse = await client.lightning_stub.AddInvoice(request)
        add_invoice_response_dict = MessageToDict(response, preserving_proto_field_name=True)
        logger.info(
            f"Invoice generated: {memo} {value} sats",
            extra={"add_invoice_response_dict": add_invoice_response_dict},
        )
        return response


def b64_hex_transform(plain_str: str) -> str:
    """Returns the b64 transformed version of a hex string"""
    a_string = bytes.fromhex(plain_str)
    return b64encode(a_string).decode()


async def add_hold_invoice(
    value: int,
    memo: str,
    connection_name: str = "voltage",
    pre_image: str = token_hex(32),
) -> invoicesrpc.AddHoldInvoiceResp:
    # https://lightning.engineering/api-docs/api/lnd/invoices/add-hold-invoice/#rest
    async with LNDClient(connection_name=connection_name) as client:
        logger.info(f"pre_image: {pre_image}")
        payment_hash = sha256(bytes.fromhex(pre_image)).digest()

        request = invoicesrpc.AddHoldInvoiceRequest(
            value=value, memo=memo, expiry=600, cltv_expiry=18, hash=payment_hash
        )
        response: lnrpc.AddHoldInvoiceResp = await client.invoices_stub.AddHoldInvoice(request)
        add_invoice_response_dict = MessageToDict(response, preserving_proto_field_name=True)
        logger.info(
            f"Hold Invoice generated: {memo} {value} sats",
            extra={"add_invoice_response_dict": add_invoice_response_dict},
        )
        return response


async def settle_hold_invoice(
    pre_image: str, connection_name: str = "voltage"
) -> invoicesrpc.SettleInvoiceResp:
    async with LNDClient(connection_name=connection_name) as client:
        request = invoicesrpc.SettleInvoiceRequest(preimage=bytes.fromhex(pre_image))
        response: invoicesrpc.SettleInvoiceResp = await client.invoices_stub.SettleInvoice(request)
        logger.info(f"Hold Invoice settled with preimage: {pre_image}")
        return response


async def send_payment_v2(
    payment_request: str, connection_name: str = "voltage", pre_image: str = None
) -> None:
    # https://lightning.engineering/api-docs/api/lnd/router/send-payment-v2/
    async with LNDClient(connection_name=connection_name) as client:
        if not pre_image:
            request = routerrpc.SendPaymentRequest(
                payment_request=payment_request,
                timeout_seconds=60,
                fee_limit_msat=1000,
                allow_self_payment=True,
                outgoing_chan_ids=[800082725764071425],
            )
        if pre_image:
            payment_hash = sha256(bytes.fromhex(pre_image)).digest()
            request = routerrpc.SendPaymentRequest(
                payment_hash=payment_hash,
                timeout_seconds=60,
                fee_limit_msat=1000,
                allow_self_payment=True,
                outgoing_chan_ids=[800082725764071425],
            )
        async for response in client.router_stub.SendPaymentV2(request):
            payment: lnrpc.Payment = response
            payment_dict = MessageToDict(payment, preserving_proto_field_name=True)
            logger.info(
                f"Status: {lnrpc.Payment.PaymentStatus.Name(payment.status)} - "
                f"Failure {lnrpc.PaymentFailureReason.Name(payment.failure_reason)}",
                extra={
                    "notification": False,
                    "payment": payment_dict,
                },
            )


async def main():
    # Add invoice
    # generate a random word
    connection = "voltage"
    hold_invoice = True

    random_word = await fetch_random_word()
    if not hold_invoice:
        add_invoice_response = await add_invoice(
            value=1000, memo=f"Test invoice {random_word}", connection_name=connection
        )
        logger.info(f"invoice: {add_invoice_response.payment_request}")
        payment_request = add_invoice_response.payment_request
    else:
        pre_image = token_hex(32)
        add_hold_invoice_response = await add_hold_invoice(
            value=1000,
            memo=f"Test invoice {random_word}",
            connection_name=connection,
            pre_image=pre_image,
        )
        logger.info(f"invoice: {add_hold_invoice_response.payment_request}")
        add_hold_invoice_response_dict = MessageToDict(
            add_hold_invoice_response, preserving_proto_field_name=True
        )
        logger.info(
            f"add_hold_invoice_response_dict: {add_hold_invoice_response_dict}",
            extra={"notification": False},
        )

    await asyncio.sleep(10)

    await send_payment_v2(
        payment_request=payment_request,
        connection_name=connection,
        pre_image="e5284a6c7d4a45cae8c33b7a96fbfc72d73416ca2b3caf076f6e06876753a2c4",
    )
    await asyncio.sleep(0.1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        logger.info(f"✅ {__file__} stopped")

    except KeyboardInterrupt:
        logger.warning(f"✅ {__file__} stopped by keyboard", extra={"notification": False})
