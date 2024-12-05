import asyncio
from typing import AsyncGenerator

from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config

# Create a temporary file
db = MyDB()


async def subscribe_invoices(
    add_index: int, settle_index: int
) -> AsyncGenerator[LNDInvoice, None]:
    async with LNDClient() as client:
        request_sub = ln.InvoiceSubscription(
            add_index=add_index, settle_index=settle_index
        )
        try:
            async for inv in client.call_async_generator(
                client.lightning_stub.SubscribeInvoices,
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
            raise e
        except Exception as e:
            logger.error(e)
            raise e


async def subscribe_htlc_events():
    async with LNDClient() as client:
        request_sub = routerrpc.SubscribeHtlcEventsRequest()

        try:
            async for htlc_event in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request_sub,
                call_name="SubscribeHtlcEvents",
            ):
                logger.info(htlc_event)
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            raise e
        except Exception as e:
            logger.error(e)
            raise e


async def main() -> None:
    logger.info("Starting LND gRPC client")
    error_codes: set[str] = set()
    try:
        async with LNDClient() as client:
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            logger.info(f"Balance: {balance.local_balance.sat:,.0f} sats")

            add_index = 0
            settle_index = 0
            await subscribe_htlc_events()
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
                                f"{invoice.memo} {invoice.value:,.0f} sats",
                                extra={"telegram": True},
                            )
                            most_recent = invoice
                            settle_index = most_recent.settle_index
                        else:
                            send_telegram = False if invoice.is_keysend else True
                            logger.info(
                                f"✅ Valid   invoice {invoice.add_index} with memo "
                                f"{invoice.memo} {invoice.value:,.0f} sats",
                                extra={"telegram": send_telegram},
                            )
                            most_recent = invoice
                            db.update_most_recent(invoice)
                            add_index = most_recent.add_index

                except LNDSubscriptionError as e:
                    logger.warning(e)
                    pass

                except Exception as e:
                    logger.error(e)
                    raise e

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped keyboard")
    except LNDFatalError as e:
        logger.error("❌ LND gRPC client stopped fatal error")
        raise e
    except Exception as e:
        logger.error("❌ LND gRPC client stopped error")
        logger.error(e)
        raise e

    logger.info("❌ LND gRPC client stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
        logger.info("✅ LND gRPC client stopped")

    except KeyboardInterrupt:
        logger.warning(
            "✅ LND gRPC client stopped by keyboard", extra={"telegram": False}
        )

    except LNDFatalError as e:
        logger.error(
            "❌ LND gRPC client stopped by fatal error", extra={"telegram": False}
        )
        logger.error(e, extra={"telegram": False})

    except Exception as e:
        logger.error("❌ LND gRPC client stopped by error", extra={"telegram": False})
        logger.error(e, extra={"telegram": False})
        raise e
