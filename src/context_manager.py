import asyncio
import os
from typing import Any

from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDSubscriptionError
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config

# Create a temporary file
db = MyDB()


async def subscribe_invoices(add_index: int, settle_index: int):
    async with LNDClient() as client:
        request_sub = ln.InvoiceSubscription(
            add_index=add_index, settle_index=settle_index
        )
        try:
            async for inv in client.call_async_generator(
                client.stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
                invoice = LNDInvoice.model_validate(inv_dict)
                yield invoice
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeInvoices"
            )
            return
        except Exception as e:
            logger.error(e)
            raise e


async def main():
    logger.info("Starting LND gRPC client")
    error_codes: set[Any] = set()
    try:
        async with LNDClient() as client:
            balance = await client.call(
                client.stub.WalletBalance,
                ln.WalletBalanceRequest(),
            )
            logger.info(f"Balance: {balance.total_balance} sats")

            add_index = 0
            settle_index = 0

            while True:
                logger.info("Subscribing to invoices")
                logger.info(f"Add index: {add_index} - Settle index: {settle_index}")
                try:
                    async for invoice in subscribe_invoices(add_index, settle_index):

                        if error_codes:
                            logger.info(
                                f"✅ Error codes cleared {error_codes}",
                                extra={
                                    "telegram": True,
                                    "error_code_clear": error_codes,
                                },
                            )
                            error_codes.clear()
                        if invoice.settled:
                            logger.info(
                                f"✅ Settled invoice {invoice.add_index} with memo "
                                f"{invoice.memo} {invoice.value} sats",
                                extra={"telegram": True},
                            )
                            logger.info(f"{invoice.settle_date}")
                            most_recent = invoice
                            settle_index = most_recent.settle_index
                        else:
                            logger.info(
                                f"✅ Valid invoice {invoice.add_index} with memo "
                                f"{invoice.memo} {invoice.value} sats",
                                extra={"telegram": True},
                            )
                            most_recent = invoice
                            db.update_most_recent(invoice)
                            add_index = most_recent.add_index

                except Exception as e:
                    logger.error(e)
                    raise e

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e

    logger.info("❌ LND gRPC client stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e
