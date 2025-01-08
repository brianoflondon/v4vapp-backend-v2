from datetime import datetime, timezone
import typer
import sys
import asyncio
from typing import Optional, Annotated, List
from google.protobuf.json_format import MessageToDict


from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    get_channel_name,
    get_node_alias_from_pay_request,
)
from v4vapp_backend_v2.grpc_models.lnd_events_group import (
    LndChannelName,
    LndEventsGroup,
    EventItem,
)
from v4vapp_backend_v2.lnd_grpc.lnd_errors import (
    LNDConnectionError,
    LNDSubscriptionError,
)
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, format_time_delta, logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient

from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config

app = typer.Typer()


async def track_events(
    event: EventItem,
    client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    """
    Asynchronously retrieves invoices from the LND node and logs them.
    Args:
        client (LNDClient): The LND client to use for the connection.

    Returns:
        None
    """
    event_id = lnd_events_group.append(event)
    if lnd_events_group.complete_group(event=event):
        logger.info(
            f"{client.icon} {lnd_events_group.message(event)}",
        )
        lnd_events_group.remove_group(event)
    pass


async def invoice_report(
    invoice: lnrpc.Invoice, client: LNDClient, lnd_events_group: LndEventsGroup = None
) -> None:
    logger.info(
        f"{client.icon} Invoice: {invoice.add_index:>6} amount: {invoice.value:>10,} sat {invoice.settle_index}",
        extra={"invoice": MessageToDict(invoice, preserving_proto_field_name=True)},
    )


async def payment_report(
    payment: lnrpc.Payment, client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    status = lnrpc.Payment.PaymentStatus.Name(payment.status)
    creation_date = datetime.fromtimestamp(
        payment.creation_time_ns / 1e9, tz=timezone.utc
    )
    pre_image = payment.payment_preimage if payment.payment_preimage else ""
    dest_alias = await get_node_alias_from_pay_request(payment.payment_request, client)
    in_flight_time = format_time_delta(datetime.now(tz=timezone.utc) - creation_date)
    logger.info(
        (
            f"{client.icon} Payment: {payment.payment_index:>6} "
            f"amount: {payment.value_sat:>10,} sat "
            f"dest: {dest_alias} "
            f"pre_image: {pre_image} "
            f"in flight: {in_flight_time} "
            f"{creation_date:%H:%M:%S} status: {status}"
        ),
        extra={"payment": MessageToDict(payment, preserving_proto_field_name=True)},
    )


async def htlc_event_report(
    htlc_event: routerrpc.HtlcEvent, client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    event_type = (
        routerrpc.HtlcEvent.EventType.Name(htlc_event.event_type)
        if htlc_event.event_type
        else None
    )
    htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
    preimage = (
        htlc_event.settle_event.preimage.hex()
        if htlc_event.settle_event.preimage != b""
        else None
    )
    logger.info(
        (f"{client.icon} htlc:    {htlc_id:>6} {event_type} {preimage}"),
        extra={
            "htlc_event": MessageToDict(htlc_event, preserving_proto_field_name=True)
        },
    )


async def invoices_loop(client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    """
    Asynchronously retrieves invoices from the LND node and logs them.
    Args:
        client (LNDClient): The LND client to use for the connection.

    Returns:
        None
    """
    request_sub = lnrpc.InvoiceSubscription(add_index=0, settle_index=0)
    while True:
        try:
            async for invoice in client.call_async_generator(
                client.lightning_stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                invoice: lnrpc.Invoice
                async_publish(Events.LND_INVOICE, invoice, client, lnd_events_group)
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeInvoices"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error(
                "🔴 Connection error in invoices_loop", exc_info=e, stack_info=True
            )
            raise e


async def payments_loop(client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    request = routerrpc.TrackPaymentRequest(no_inflight_updates=False)
    while True:
        try:
            async for payment in client.call_async_generator(
                client.router_stub.TrackPayments,
                request,
                call_name="TrackPayments",
            ):
                payment: lnrpc.Payment
                async_publish(Events.LND_PAYMENT, payment, client, lnd_events_group)
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="TrackPayments"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error(
                "🔴 Connection error in payments_loop", exc_info=e, stack_info=True
            )
            raise e


async def htlc_events_loop(client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    request = routerrpc.SubscribeHtlcEventsRequest()
    while True:
        try:
            async for htlc_event in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request,
                call_name="SubscribeHtlcEvents",
            ):
                htlc_event: routerrpc.HtlcEvent
                async_publish(Events.HTLC_EVENT, htlc_event, client, lnd_events_group)
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error(
                "🔴 Connection error in payments_loop", exc_info=e, stack_info=True
            )
            raise e


async def transactions_loop(client: LNDClient) -> None:
    request_sub = lnrpc.GetTransactionsRequest(
        start_height=0,
        end_height=0,
    )
    logger.info(f"{client.icon} 🔍 Monitoring transactions...")
    while True:
        async for transaction in client.call_async_generator(
            client.lightning_stub.SubscribeTransactions,
            request_sub,
        ):
            transaction: lnrpc.Transaction
            logger.info(transaction)


async def fill_channel_names(
    client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    request = lnrpc.ListChannelsRequest()
    channels = await client.call(
        client.lightning_stub.ListChannels,
        lnrpc.ListChannelsRequest(),
    )
    channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
    # Get the name of each channel
    tasks = []
    for channel in channels_dict.get("channels", []):
        tasks.append(
            get_channel_name(
                channel_id=int(channel["chan_id"]),
                client=client,
            )
        )
    names_list: List[LndChannelName] = await asyncio.gather(*tasks)
    for channel_name in names_list:
        lnd_events_group.append(channel_name)
        logger.info(
            (
                f"{client.icon} "
                f"Channel {channel_name.channel_id} -> {channel_name.name}"
            ),
            extra={"channel_name": channel_name.to_dict()},
        )


async def run(connection_name: str) -> None:
    """
    Main function to run the node monitor.
    Args:
        connection_name (str): The name of the connection to monitor.

    Returns:
        None
    """
    lnd_events_group = LndEventsGroup()
    async with LNDClient(connection_name) as client:
        logger.info(f"{client.icon} 🔍 Monitoring node...")

        if client.get_info:
            logger.info(
                f"{client.icon} Node: {client.get_info.alias} pub_key: {client.get_info.identity_pubkey}"
            )
        await fill_channel_names(client, lnd_events_group)

        async_subscribe(Events.LND_INVOICE, invoice_report)
        async_subscribe(Events.LND_PAYMENT, payment_report)
        async_subscribe(Events.HTLC_EVENT, htlc_event_report)
        async_subscribe(
            [Events.LND_INVOICE, Events.LND_PAYMENT, Events.HTLC_EVENT],
            track_events,
        )
        tasks = [
            invoices_loop(client=client, lnd_events_group=lnd_events_group),
            payments_loop(client=client, lnd_events_group=lnd_events_group),
            htlc_events_loop(client=client, lnd_events_group=lnd_events_group),
        ]
        try:
            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            print("👋 Received signal to stop. Exiting...")
            await client.channel.close()
            INTERNAL_CONFIG.__exit__(None, None, None)


@app.command()
def main(
    node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The node to monitor. If not provided, defaults to the value: "
                f"{CONFIG.default_connection}.\n"
                f"Choose from: {CONFIG.connection_names}"
            )
        ),
    ] = CONFIG.default_connection
):
    f"""
    Main function to run the node monitor.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        {CONFIG.connection_names}

    Returns:
        None
    """
    icon = CONFIG.icon(node)
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: {node} {icon}. Version: {CONFIG.version}"
    )
    asyncio.run(run(node))
    print("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "lnd_monitor_v2"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
