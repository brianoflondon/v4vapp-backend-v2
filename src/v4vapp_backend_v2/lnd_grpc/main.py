import asyncio

import backoff
import grpc
from grpc.aio import AioRpcError
from pydantic import ValidationError

from v4vapp_backend_v2.config import logger, setup_logging
from v4vapp_backend_v2.lnd_grpc.connect import (
    connect_to_lnd,
    most_recent_invoice,
    subscribe_invoices,
)


@backoff.on_exception(
    lambda: backoff.expo(base=10),
    (ValidationError, AioRpcError, grpc.RpcError),
    max_tries=20,
    logger=logger,
)
async def subscribe_invoices_with_backoff():
    stub = await connect_to_lnd()
    most_recent = await most_recent_invoice(stub)
    while True:
        try:
            async for invoice in subscribe_invoices(add_index=most_recent.add_index):
                if invoice.settled:
                    logger.info(
                        f"✅ Settled invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}"
                    )
                    logger.info(f"{invoice.settle_date}")
                    most_recent = invoice
                else:
                    logger.info(
                        f"✅ Valid invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}"
                    )
                    most_recent = invoice
        except grpc.RpcError as e:
            logger.error(f"Lost connection to server: {e}")
            stub = await connect_to_lnd()  # reconnect to the server


async def main():
    # stub = await connect_to_lnd()
    # response = await stub.WalletBalance(ln.WalletBalanceRequest())
    # print(response)

    # response_inv = await stub.ListInvoices(
    #     ln.ListInvoiceRequest(
    #         pending_only=False, reversed=True, index_offset=0, num_max_invoices=10
    #     )
    # )
    # for inv in response_inv.invoices:
    #     inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
    #     try:
    #         invoice = LNDInvoice.model_validate(inv_dict)
    #         print(f"✅ Valid invoice {invoice.add_index}")
    #     except ValidationError as e:
    #         print(e)
    #         print(f"❌ Invalid invoice {inv.add_index}")

    # response_payment = await stub.ListPayments(
    #     ln.ListPaymentsRequest(reversed=True, index_offset=0, max_payments=1)
    # )

    # for pay in response_payment.payments:
    #     print(pay)
    #     print(MessageToDict(pay, preserving_proto_field_name=True))
    #     print()
    while True:
        try:
            logger.info("🔁 Starting invoice subscription")
            await subscribe_invoices_with_backoff()
        except Exception as e:
            logger.error("❌ Error in invoice subscription")
            logger.error(e)
            await asyncio.sleep(30)


if __name__ == "__main__":

    setup_logging()

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e
