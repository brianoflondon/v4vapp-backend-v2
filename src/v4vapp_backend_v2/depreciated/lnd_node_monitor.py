import asyncio
import inspect
import json
from datetime import datetime, timezone
from typing import Annotated, AsyncGenerator, List, Optional

import typer
from google.protobuf.json_format import MessageToDict
from pydantic import ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.depreciated.htlc_event_models import (
    ChannelName,
    HtlcEvent,
    HtlcTrackingList,
)
from v4vapp_backend_v2.events.async_event import async_publish
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient, error_to_dict
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.lnd_grpc.lnd_functions import get_channel_name
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config
global_tracking = HtlcTrackingList()

app = typer.Typer()


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
    add_index: int, settle_index: int, client: LNDClient
) -> AsyncGenerator[LNDInvoice, None]:
    request_sub = lnrpc.InvoiceSubscription(
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
                extra={"invoice_data": inv_dict, "notification": False},
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


async def subscribe_invoices_loop(client: LNDClient) -> None:
    error_codes: set[str] = set()
    add_index = 0
    settle_index = 0
    while True:
        logger.debug("Subscribing to invoices")
        logger.debug(f"Add index: {add_index} - Settle index: {settle_index}")
        try:
            async for invoice in subscribe_invoices(add_index, settle_index, client):
                tracking_list_dump()
                add_index = global_tracking.add_invoice(invoice)
                if error_codes:
                    logger.info(
                        f"✅ Error codes cleared {error_codes}",
                        extra={
                            "notification": True,
                            "error_code_clear": error_codes,
                        },
                    )
                    error_codes.clear()

                # send_notification = False if invoice.is_keysend else True
                send_notification = (
                    False  # the alerts will come from the received htlc_events
                )
                async_publish(Events.LND_INVOICE, invoice)
                invoice.invoice_log(logger.debug, send_notification)

                if invoice.settled:
                    settle_index = invoice.settle_index

                global_tracking.remove_expired_invoices()

        except LNDSubscriptionError as e:
            logger.warning(
                f"Clearing after error {e}",
                extra={"notification": True, "error_details": error_to_dict(e)},
            )
            pass
        except Exception as e:
            logger.exception(e)
            raise e


async def subscribe_htlc_events(
    client: LNDClient,
) -> AsyncGenerator[HtlcEvent, None]:
    logger.debug(f"Starting {inspect.currentframe().f_code.co_name}")
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


async def subscribe_htlc_events_loop(client: LNDClient) -> None:
    logger.debug(f"Starting {inspect.currentframe().f_code.co_name}")
    icon = client.icon
    while True:
        logger.debug("Subscribing to HTLC events")
        try:
            async for htlc_event in subscribe_htlc_events(client=client):
                tracking_list_dump()
                await asyncio.sleep(0.1)
                htlc_id = global_tracking.add_event(htlc_event)
                # must grab invoice before complete group event which will delete it
                invoice = global_tracking.lookup_invoice_by_htlc_id(htlc_id)
                complete = global_tracking.complete_group(htlc_id)
                extra = {
                    "notification": complete,
                    "complete": complete,
                    "htlc_event": htlc_event.model_dump(exclude_none=True),
                }
                if invoice:
                    extra["invoice"] = invoice.model_dump(exclude_none=True)
                log_level = logger.info if complete else logger.debug
                global_tracking.log_event(htlc_id, log_level, extra=extra, icon=icon)
                if complete:
                    logger.debug(f"✅ Complete group, Delete group {htlc_id}")
                    global_tracking.delete_event(htlc_id)

        except LNDSubscriptionError as e:
            logger.warning(
                f"Clearing after error {e}",
                extra={"notification": True, "error_details": error_to_dict(e)},
            )
            pass
        except Exception as e:
            logger.error(f"Error in {__name__}")
            logger.exception(e)
            raise e


async def fill_channel_list(client: LNDClient) -> None:
    # Get the balance of the node
    balance: lnrpc.ChannelBalanceResponse = await client.call(
        client.lightning_stub.ChannelBalance,
        lnrpc.ChannelBalanceRequest(),
    )
    balance_dict = MessageToDict(balance, preserving_proto_field_name=True)
    # Get the list of channels
    channels = await client.call(
        client.lightning_stub.ListChannels,
        lnrpc.ListChannelsRequest(),
    )
    channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
    tasks = []
    # Get the info about this node
    get_info: lnrpc.GetInfoResponse = await client.node_get_info
    get_info_dict = MessageToDict(get_info, preserving_proto_field_name=True)
    own_pub_key = get_info.identity_pubkey
    logger.info(
        f"{client.icon} " f"Local Balance: {balance.local_balance.sat:,.0f} sats",
        extra={"balance": balance_dict},
    )
    logger.info(
        (f"{client.icon} " f"Own pub key: {own_pub_key}"),
        extra={"get_info": get_info_dict},
    )

    # Get the name of each channel
    for channel in channels_dict.get("channels", []):
        tasks.append(
            get_channel_name(
                channel_id=int(channel["chan_id"]),
                lnd_client=client,
                own_pub_key=own_pub_key,
            )
        )
    names_list: List[ChannelName] = await asyncio.gather(*tasks)
    for channel_name in names_list:
        global_tracking.add_name(channel_name)
        logger.info(
            (
                f"{client.icon} "
                f"Channel {channel_name.channel_id} -> {channel_name.name}"
            ),
            extra={"channel_name": channel_name.model_dump()},
        )


async def run(connection_name: str) -> None:
    async with LNDClient(connection_name=connection_name) as client:
        try:
            await fill_channel_list(client=client)
            logger.info("Starting Tasks")
            tasks = [
                subscribe_invoices_loop(client=client),
                subscribe_htlc_events_loop(client=client),
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


@app.command()
def main(
    node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The node to monitor. If not provided, defaults to the value: "
                f"{config.default_connection}.\n"
                f"Choose from: {config.lnd_connections_names}"
            )
        ),
    ] = config.default_connection
):
    f"""
    Main function to run the node monitor.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor. If not provided,
        defaults to the value specified in config.default_connection.
        Choose from:
        {config.lnd_connections_names}

    Returns:
        None
    """
    icon = config.icon(node)
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: "
        f"{node} {icon}. Version: {config.version}"
    )
    asyncio.run(run(node))


if __name__ == "__main__":
    try:
        app()
        logger.info("✅ LND gRPC client stopped")

    except KeyboardInterrupt:
        logger.warning(
            "✅ LND gRPC client stopped by keyboard", extra={"notification": False}
        )

    except LNDFatalError as e:
        logger.error(
            "❌ LND gRPC client stopped by fatal error", extra={"notification": False}
        )
        logger.error(e, extra={"notification": False})
        raise e
    except Exception as e:
        logger.error(
            "❌ LND gRPC client stopped by error", extra={"notification": False}
        )
        logger.error(e, extra={"notification": False})
        raise e
