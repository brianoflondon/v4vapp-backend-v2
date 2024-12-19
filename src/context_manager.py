import asyncio
import inspect
import json
from datetime import datetime, timezone
from typing import AsyncGenerator, List

from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient, error_to_dict
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.lnd_grpc.lnd_functions import get_channel_name
from v4vapp_backend_v2.models.htlc_event_models import (
    ChannelName,
    HtlcEvent,
    HtlcTrackingList,
)
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config

# Create a temporary file
db = MyDB()

global_tracking = HtlcTrackingList()


def tracking_list_dump():
    if global_tracking.num_events == 0 and global_tracking.num_invoices == 0:
        return
    logger.debug(
        f"Now tracking {global_tracking.num_events} events",
        extra={"events": global_tracking.events},
    )
    for event in global_tracking.events:
        logger.debug(
            f" -> Event {event.htlc_id} {event.event_type}",
        )

    logger.debug(
        f"Now tracking {global_tracking.num_invoices} invoices",
        extra={"invoices": global_tracking.invoices},
    )
    current_time = int(datetime.now(tz=timezone.utc).timestamp())
    for invoice in global_tracking.invoices:
        expires_in = (
            invoice.creation_date.timestamp() + (invoice.expiry or 0) - current_time
        )
        logger.debug(
            f" -> Invoice {invoice.add_index} {invoice.value:,} sats "
            f"expires in {expires_in:.1f}",
        )
        if expires_in < 0:
            global_tracking.remove_invoice(invoice.add_index)


async def tracking_list_dump_loop():
    while True:
        tracking_list_dump()
        await asyncio.sleep(60)


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
                logger.debug(
                    f"Raw invoice data\n{json.dumps(inv_dict, indent=2)}",
                    extra={"invoice_data": inv_dict, "telegram": False},
                )
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


async def subscribe_invoices_loop():
    error_codes: set[str] = set()
    add_index = 0
    settle_index = 0
    while True:
        logger.debug("Subscribing to invoices")
        logger.debug(f"Add index: {add_index} - Settle index: {settle_index}")
        try:
            async for invoice in subscribe_invoices(add_index, settle_index):
                tracking_list_dump()
                add_index = global_tracking.add_invoice(invoice)
                if error_codes:
                    logger.info(
                        f"✅ Error codes cleared {error_codes}",
                        extra={
                            "telegram": True,
                            "error_code_clear": error_codes,
                        },
                    )
                    error_codes.clear()

                send_telegram = False if invoice.is_keysend else True
                invoice.invoice_log(logger.info, send_telegram)
                db.update_most_recent(invoice)

                if invoice.settled:
                    settle_index = invoice.settle_index

                global_tracking.remove_expired_invoices()

        except LNDSubscriptionError as e:
            logger.warning(
                f"Clearing after error {e}",
                extra={"telegram": True, "error_details": error_to_dict(e)},
            )
            pass
        except Exception as e:
            logger.exception(e)
            raise e


async def subscribe_htlc_events() -> AsyncGenerator[HtlcEvent, None]:
    logger.debug(f"Starting {inspect.currentframe().f_code.co_name}")
    async with LNDClient() as client:
        request_sub = routerrpc.SubscribeHtlcEventsRequest()
        try:
            async for htlc in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request_sub,
                call_name="SubscribeHtlcEvents",
            ):
                htlc_data = MessageToDict(htlc, preserving_proto_field_name=True)
                logger.debug(
                    "RAW htlc_event_data object\n" + json.dumps(htlc_data, indent=2),
                    extra={"htlc_data": htlc_data},
                )
                try:
                    htlc_event = HtlcEvent.model_validate(htlc_data)
                except ValidationError as e:
                    logger.warning(
                        "htlc_event_data object\n" + json.dumps(htlc_data, indent=2),
                        extra={"htlc_data": htlc_data},
                    )
                    logger.error(e)
                    continue
                yield htlc_event
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            raise e
        except Exception as e:
            logger.error(f"Unexpected error in {inspect.currentframe().f_code.co_name}")
            logger.error(e)
            raise e


async def subscribe_htlc_events_loop() -> None:
    logger.debug(f"Starting {inspect.currentframe().f_code.co_name}")
    while True:
        logger.debug("Subscribing to HTLC events")
        try:
            async for htlc_event in subscribe_htlc_events():
                tracking_list_dump()
                await asyncio.sleep(0.1)
                htlc_id = global_tracking.add_event(htlc_event)
                complete = global_tracking.complete_group(htlc_id)
                extra = {
                    "telegram": complete,
                    "htlc_event": htlc_event.model_dump(exclude_none=True),
                    "complete": complete,
                }
                log_level = logger.info if complete else logger.debug
                global_tracking.log_event(htlc_id, log_level, extra)
                if complete:
                    logger.debug(f"✅ Complete group, Delete group {htlc_id}")
                    global_tracking.delete_event(htlc_id)

        except LNDSubscriptionError as e:
            logger.warning(
                f"Clearing after error {e}",
                extra={"telegram": True, "error_details": error_to_dict(e)},
            )
            pass
        except Exception as e:
            logger.error(f"Error in {__name__}")
            logger.exception(e)
            raise e


async def main() -> None:
    logger.debug("Starting LND gRPC client")

    try:
        async with LNDClient() as client:
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            logger.info(f"Balance: {balance.local_balance.sat:,.0f} sats")

            channels = await client.call(
                client.lightning_stub.ListChannels,
                ln.ListChannelsRequest(),
            )
            channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
            tasks = []
            for channel in channels_dict.get("channels", []):
                tasks.append(get_channel_name(int(channel["chan_id"])))
                # name =  get_channel_name(int(channel["chan_id"]))
            names_list: List[ChannelName] = await asyncio.gather(*tasks)
            for channel_name in names_list:
                global_tracking.add_name(channel_name)
                logger.info(
                    f"Channel {channel_name.channel_id} -> {channel_name.name}",
                    extra={"channel_name": channel_name.model_dump()},
                )
            logger.info("Starting Tasks")
            tasks = [
                subscribe_invoices_loop(),
                subscribe_htlc_events_loop(),
                tracking_list_dump_loop(),
            ]
            await asyncio.gather(*tasks)

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
