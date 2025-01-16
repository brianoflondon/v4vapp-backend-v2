from datetime import datetime, timedelta, timezone
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
    dest_alias = await check_dest_alias(event, client, lnd_events_group, event_id)
    if lnd_events_group.complete_group(event=event):
        notification = True if type(event) == routerrpc.HtlcEvent else False
        if (
            type(event) == routerrpc.HtlcEvent
            and event.event_type != routerrpc.HtlcEvent.UNKNOWN
        ):
            try:
                htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
                if htlc_id:
                    incoming_invoice = lnd_events_group.lookup_invoice_by_htlc_id(
                        htlc_id
                    )
                if incoming_invoice:
                    amount = incoming_invoice.value if incoming_invoice else 0
                    notification = False if amount < 10 else notification
            except Exception as e:
                logger.exception(e)
                pass
        message_str, ans_dict = lnd_events_group.message(event, dest_alias=dest_alias)
        if " Attempted 0 " not in message_str:
            logger.info(
                f"{client.icon} {message_str}",
                extra={"notification": notification, **ans_dict},
            )
        await remove_event_group(event, client, lnd_events_group)


async def check_dest_alias(
    event: EventItem, client: LNDClient, lnd_events_group: LndEventsGroup, event_id: int
) -> str:
    """
    Asynchronously checks the destination alias for a given event.

    This function checks if the provided event is of type `routerrpc.HtlcEvent`. If so, it retrieves the pre-image
    associated with the event ID from the `lnd_events_group`. If a pre-image is found, it waits for the payment to
    complete, then retrieves the matching payment using the pre-image. If a matching payment is found, it fetches
    the destination alias from the payment request using the provided LND client.

    Args:
        event (EventItem): The event to check.
        client (LNDClient): The LND client to use for fetching node alias.
        lnd_events_group (LndEventsGroup): The group of LND events to query.
        event_id (int): The ID of the event to check.

    Returns:
        str: The destination alias if found, otherwise an empty string.
    """
    if type(event) == routerrpc.HtlcEvent:
        pre_image = lnd_events_group.get_htlc_event_pre_image(event_id)
        if pre_image:
            # Wait for the payment to complete
            await asyncio.sleep(1)
            matching_payment = lnd_events_group.get_payment_by_pre_image(pre_image)
            if matching_payment:
                if matching_payment.payment_request:
                    dest_alias = await get_node_alias_from_pay_request(
                        matching_payment.payment_request, client
                    )
                    return dest_alias
                else:
                    return "Keysend"
    # Keysend payments outgoing do not have a payment request
    if type(event) == lnrpc.Payment:
        if event.payment_request:
            dest_alias = await get_node_alias_from_pay_request(
                event.payment_request, client
            )
            return dest_alias
        else:
            return "Keysend"

    return ""


async def remove_event_group(
    event: EventItem, client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    """
    Asynchronously removes an event from the specified LndEventsGroup after a delay.

    Args:
        event (EventItem): The event to be removed from the group.
        lnd_events_group (LndEventsGroup): The group from which the event will be removed.

    Returns:
        None
    """
    await asyncio.sleep(3)
    lnd_events_group.remove_group(event)


async def invoice_report(
    invoice: lnrpc.Invoice, client: LNDClient, lnd_events_group: LndEventsGroup = None
) -> None:
    expiry_datetime = datetime.fromtimestamp(
        invoice.creation_date + invoice.expiry, tz=timezone.utc
    )
    time_to_expire = expiry_datetime - datetime.now(tz=timezone.utc)
    if time_to_expire.total_seconds() < 0:
        time_to_expire = timedelta(seconds=0)
    time_to_expire_str = format_time_delta(time_to_expire)
    logger.debug(
        (
            f"{client.icon} Invoice: {invoice.add_index:>6} "
            f"amount: {invoice.value:>10,} sat {invoice.settle_index} "
            f"expiry: {time_to_expire_str} "
        ),
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
    logger.debug(
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
    is_complete = lnd_events_group.complete_group(htlc_event)
    is_complete_str = "💎" if is_complete else "🔨"
    logger.debug(
        (
            f"{client.icon} {is_complete_str} htlc:    {htlc_id:>6} {event_type} {preimage}"
        ),
        extra={
            "htlc_event": MessageToDict(htlc_event, preserving_proto_field_name=True),
            "complete": is_complete,
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
        request,
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
        logger.info(
            f"{client.icon} 🔍 Monitoring node... {connection_name}",
            extra={"notification": True},
        )

        if client.get_info:
            logger.info(
                f"{client.icon} Node: {client.get_info.alias} pub_key: {client.get_info.identity_pubkey}"
            )
        await fill_channel_names(client, lnd_events_group)
        # It is important to subscribe to the track_events function before the reporting functions
        # The track_events function will group events and report them when the group is complete
        async_subscribe(
            [Events.LND_INVOICE, Events.LND_PAYMENT, Events.HTLC_EVENT],
            track_events,
        )
        async_subscribe(Events.LND_INVOICE, invoice_report)
        async_subscribe(Events.LND_PAYMENT, payment_report)
        async_subscribe(Events.HTLC_EVENT, htlc_event_report)
        tasks = [
            invoices_loop(client=client, lnd_events_group=lnd_events_group),
            payments_loop(client=client, lnd_events_group=lnd_events_group),
            htlc_events_loop(client=client, lnd_events_group=lnd_events_group),
        ]
        await asyncio.gather(*tasks)
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
