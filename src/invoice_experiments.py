import asyncio
import json
from typing import List
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc

import httpx

config = InternalConfig().config


async def fetch_random_word() -> str:
    async with httpx.AsyncClient() as client:
        response = await client.get("https://random-word-api.herokuapp.com/word")
        words = response.json()
        return words[0]


async def add_invoice(
    value: int, memo: str, connection_name: str = "umbrel"
) -> lnrpc.AddInvoiceResponse:
    # https://lightning.engineering/api-docs/api/lnd/router/add-invoice/
    async with LNDClient(connection_name=connection_name) as client:
        request = lnrpc.Invoice(value=value, memo=memo)
        response: lnrpc.AddInvoiceResponse = await client.lightning_stub.AddInvoice(
            request
        )
        add_invoice_response_dict = MessageToDict(
            response, preserving_proto_field_name=True
        )
        logger.info(
            f"Invoice generated: {memo} {value} sats",
            extra={"add_invoice_response_dict": add_invoice_response_dict},
        )
        return response


async def send_payment_v2(
    payment_request: str, connection_name: str = "umbrel"
) -> None:
    # https://lightning.engineering/api-docs/api/lnd/router/send-payment-v2/
    async with LNDClient(connection_name=connection_name) as client:
        request = routerrpc.SendPaymentRequest(
            payment_request=payment_request,
            timeout_seconds=60,
            fee_limit_sat=1,
            allow_self_payment=True,
        )
        async for response in client.router_stub.SendPaymentV2(request):
            payment: lnrpc.Payment = response
            payment_dict = MessageToDict(payment, preserving_proto_field_name=True)
            logger.info(
                f"Status: {lnrpc.Payment.PaymentStatus.Name(payment.status)} - "
                f"Failure {lnrpc.Failure.FailureCode.Name(payment.failure_reason)}",
                extra={
                    "notification": False,
                    "payment": payment_dict,
                },
            )
            # if payment.payment_error:
            #     logger.error(f"payment_error: {response.payment_error}")
            #     return


# async def send_to_route_v2(connection_name: str = "umbrel") -> None:
#     # https://lightning.engineering/api-docs/api/lnd/router/send-to-route-v2/
#     async with LNDClient(connection_name=connection_name) as client:
#         request = routerrpc.SendToRouteRequest(
#             route=[
#                 routerrpc.RouteHint(
#                     hop_hints=[
#                         routerrpc.HopHint(
#                             node_id="02c1e1c6f1b8e4d6c5e5d6e7b3b1c4c7b8e1b1e3


async def main():
    # Add invoice
    # generate a random word
    random_word = await fetch_random_word()
    add_invoice_response = await add_invoice(
        value=1000, memo=f"Test invoice {random_word}"
    )
    logger.info(f"invoice: {add_invoice_response.payment_request}")
    # Send payment
    payment_request = add_invoice_response.payment_request
    await send_payment_v2(payment_request)
    await asyncio.sleep(0.1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        logger.info(f"✅ {__file__} stopped")

    except KeyboardInterrupt:
        logger.warning(
            f"✅ {__file__} stopped by keyboard", extra={"notification": False}
        )
