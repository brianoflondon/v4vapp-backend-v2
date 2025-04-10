import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, List, Optional

import typer
from google.protobuf.json_format import MessageToDict
from pymongo.errors import BulkWriteError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db import DATABASE_ICON, MongoDBClient
from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.grpc_models.lnd_events_group import (
    EventItem,
    LndChannelName,
    LndEventsGroup,
)
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, get_in_flight_time
from v4vapp_backend_v2.helpers.pub_key_alias import update_payment_route_with_alias
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDConnectionError, LNDSubscriptionError
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    get_channel_name,
    get_node_alias_from_pay_request,
)
from v4vapp_backend_v2.models.invoice_models import Invoice, ListInvoiceResponse
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse, Payment

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
DATABASE_NAME = "lnd_monitor_v2"

app = typer.Typer()

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def track_events(
    htlc_event: EventItem,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
    **kwargs: Any,
) -> None:
    """
    Asynchronously retrieves invoices from the LND node and logs them.
    Args:
        client (LNDClient): The LND client to use for the connection.

    Returns:
        None
    """
    event_id = lnd_events_group.append(htlc_event)
    dest_alias = await check_dest_alias(htlc_event, lnd_client, lnd_events_group, event_id)
    # message_str, ans_dict = lnd_events_group.message(htlc_event,
    # dest_alias=dest_alias)
    # The delay is necessary to allow the group to complete because sometimes
    # Invoices and Payments are not received in the right order with the HtlcEvents
    if lnd_events_group.complete_group(event=htlc_event):
        notification = True if isinstance(htlc_event, routerrpc.HtlcEvent) else False
        if (
            isinstance(htlc_event, routerrpc.HtlcEvent)
            and htlc_event.event_type != routerrpc.HtlcEvent.UNKNOWN
        ):
            try:
                htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
                if htlc_id:
                    # logger.info(f"Waiting for incoming invoice... {htlc_id}")
                    await asyncio.sleep(0.2)
                    incoming_invoice = lnd_events_group.lookup_invoice_by_htlc_id(htlc_id)
                if incoming_invoice:
                    # logger.info(f"Found incoming invoice... {htlc_id}")
                    amount = int(incoming_invoice.value_msat / 1000)
                    notification = False if amount < 10 else notification
            except Exception as e:
                logger.exception(e)
                pass
        await asyncio.sleep(0.2)
        message_str, ans_dict = lnd_events_group.message(htlc_event, dest_alias=dest_alias)
        if check_for_attempted_forwards(htlc_event, message_str):
            silent = True
            notification = False
        else:
            silent = False
        if not (" Attempted 0 " in message_str or "UNKNOWN 0 " in message_str):
            logger.info(
                f"{lnd_client.icon} {message_str}",
                extra={
                    "notification": notification,
                    "silent": silent,
                    type(htlc_event).__name__: ans_dict,
                },
            )
        asyncio.create_task(remove_event_group(htlc_event, lnd_client, lnd_events_group))


def check_for_attempted_forwards(htlc_event: EventItem, message_str: str) -> bool:
    """
    Checks if the provided event is an attempted forward.

    Args:
        event (EventItem): The event to check.
        message_str (str): The computed message.

    Returns:
        bool: True if the event is an attempted forward, otherwise False.
    """
    if isinstance(htlc_event, routerrpc.HtlcEvent):
        if "Attempted" in message_str:
            return True
    return False


async def check_dest_alias(
    htlc_event: EventItem,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
    event_id: int,
) -> str:
    """
    Asynchronously checks the destination alias for a given event.

    This function checks if the provided event is of type `routerrpc.HtlcEvent`.
    If so, it retrieves the pre-image associated with the event ID from the
    `lnd_events_group`. If a pre-image is found, it waits for the payment to
    complete, then retrieves the matching payment using the pre-image. If a matching
    payment is found, it fetches the destination alias from the payment request
    using the provided LND client.

    Args:
        event (EventItem): The event to check.
        client (LNDClient): The LND client to use for fetching node alias.
        lnd_events_group (LndEventsGroup): The group of LND events to query.
        event_id (int): The ID of the event to check.

    Returns:
        str: The destination alias if found, otherwise an empty string.
    """
    if isinstance(htlc_event, routerrpc.HtlcEvent):
        pre_image = lnd_events_group.get_htlc_event_pre_image(event_id)
        if pre_image:
            # Wait for the payment to complete
            await asyncio.sleep(1)
            matching_payment = lnd_events_group.get_payment_by_pre_image(pre_image)
            if matching_payment:
                if matching_payment.payment_request:
                    dest_alias = await get_node_alias_from_pay_request(
                        matching_payment.payment_request, lnd_client
                    )
                    return dest_alias
                else:
                    return "Keysend"
    # Keysend payments outgoing do not have a payment request
    if isinstance(htlc_event, lnrpc.Payment):
        if htlc_event.payment_request:
            dest_alias = await get_node_alias_from_pay_request(
                htlc_event.payment_request, lnd_client
            )
            return dest_alias
        else:
            return "Keysend"

    return ""


async def remove_event_group(
    htlc_event: EventItem, lnd_client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    """
    Asynchronously removes an event from the specified LndEventsGroup after a delay.

    Args:
        event (EventItem): The event to be removed from the group.
        lnd_events_group (LndEventsGroup): The group from which the event will be
            removed.

    Returns:
        None
    """
    logger.debug(f"Removing event group: {MessageToDict(htlc_event)}")
    await asyncio.sleep(10)
    lnd_events_group.remove_group(htlc_event)


async def db_store_invoice(
    htlc_event: lnrpc.Invoice, lnd_client: LNDClient, *args: Any, **kwargs
) -> None:
    """
    Asynchronously stores an invoice in the MongoDB database.

    Args:
        invoice (lnrpc.Invoice): The invoice to store.

    Returns:
        None
    """
    async with MongoDBClient(
        db_conn="local_connection", db_name=DATABASE_NAME, db_user="lnd_monitor"
    ) as db_client:
        logger.debug(
            f"{lnd_client.icon}{DATABASE_ICON} Storing invoice: {htlc_event.add_index} "
            f"{db_client.hex_id}"
        )
        try:
            invoice_pyd = Invoice(htlc_event)
        except Exception as e:
            logger.warning(e)
            return
        query = {"r_hash": invoice_pyd.r_hash}
        invoice_dict = invoice_pyd.model_dump(exclude_none=True, exclude_unset=True)
        ans = await db_client.update_one("invoices", query, invoice_dict, upsert=True)
        logger.debug(
            f"{lnd_client.icon}{DATABASE_ICON} "
            f"New invoice recorded: {invoice_pyd.add_index:>6} {invoice_pyd.r_hash}",
            extra={"db_ans": ans.raw_result, "invoice": invoice_dict},
        )


async def db_store_payment(
    htlc_event: lnrpc.Payment, lnd_client: LNDClient, *args: Any, **kwargs
) -> None:
    """
    Asynchronously stores a payment in the MongoDB database.

    Args:
        payment (lnrpc.Payment): The payment to store.

    Returns:
        None
    """
    async with MongoDBClient(
        db_conn="local_connection", db_name=DATABASE_NAME, db_user="lnd_monitor"
    ) as db_client:
        try:
            payment_pyd = Payment(htlc_event)
            await update_payment_route_with_alias(
                db_client=db_client,
                lnd_client=lnd_client,
                payment=payment_pyd,
                fill_cache=True,
                col_pub_keys="pub_keys",
            )
            logger.info(
                f"{lnd_client.icon}{DATABASE_ICON} "
                f"Storing payment: {htlc_event.payment_index} "
                f"{db_client.hex_id} {payment_pyd.route_str}"
            )
            query = {"payment_hash": payment_pyd.payment_hash}
            payment_dict = payment_pyd.model_dump(exclude_none=True, exclude_unset=True)
            ans = await db_client.update_one("payments", query, payment_dict, upsert=True)
            logger.info(
                f"{lnd_client.icon}{DATABASE_ICON} "
                f"New payment recorded: {payment_pyd.payment_index:>6} "
                f"{payment_pyd.payment_hash} {payment_pyd.route_str}",
                extra={"db_ans": ans.raw_result, "payment": payment_dict},
            )
        except Exception as e:
            logger.info(e)
            return


async def invoice_report(
    htlc_event: lnrpc.Invoice,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup = None,
) -> None:
    expiry_datetime = datetime.fromtimestamp(
        htlc_event.creation_date + htlc_event.expiry, tz=timezone.utc
    )
    time_to_expire = expiry_datetime - datetime.now(tz=timezone.utc)
    if time_to_expire.total_seconds() < 0:
        time_to_expire = timedelta(seconds=0)
    time_to_expire_str = format_time_delta(time_to_expire)
    invoice_dict = MessageToDict(htlc_event, preserving_proto_field_name=True)
    logger.info(
        (
            f"{lnd_client.icon} Invoice: {htlc_event.add_index:>6} "
            f"amount: {htlc_event.value:>10,} sat {htlc_event.settle_index} "
            f"expiry: {time_to_expire_str} "
            f"{invoice_dict.get('r_hash')}"
        ),
        extra={"invoice": invoice_dict},
    )


async def payment_report(
    htlc_event: lnrpc.Payment,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    status = lnrpc.Payment.PaymentStatus.Name(htlc_event.status)
    creation_date = datetime.fromtimestamp(htlc_event.creation_time_ns / 1e9, tz=timezone.utc)
    pre_image = htlc_event.payment_preimage if htlc_event.payment_preimage else ""
    dest_alias = await get_node_alias_from_pay_request(htlc_event.payment_request, lnd_client)
    in_flight_time = get_in_flight_time(creation_date)
    # in_flight_time = format_time_delta(datetime.now(tz=timezone.utc) - creation_date)
    logger.info(
        (
            f"{lnd_client.icon} Payment: {htlc_event.payment_index:>6} "
            f"amount: {htlc_event.value_sat:>10,} sat "
            f"dest: {dest_alias} "
            f"pre_image: {pre_image} "
            f"in flight: {in_flight_time} "
            f"{creation_date:%H:%M:%S} status: {status} "
            f"{htlc_event.payment_hash}"
        ),
        extra={"payment": MessageToDict(htlc_event, preserving_proto_field_name=True)},
    )


async def htlc_event_report(
    htlc_event: routerrpc.HtlcEvent,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    event_type = (
        routerrpc.HtlcEvent.EventType.Name(htlc_event.event_type)
        if htlc_event.event_type
        else None
    )
    htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
    preimage = (
        htlc_event.settle_event.preimage.hex() if htlc_event.settle_event.preimage != b"" else None
    )
    is_complete = lnd_events_group.complete_group(htlc_event)
    is_complete_str = "💎" if is_complete else "🔨"
    logger.info(
        (f"{lnd_client.icon} {is_complete_str} htlc:    {htlc_id:>6} {event_type} {preimage}"),
        extra={
            "htlc_event": MessageToDict(htlc_event, preserving_proto_field_name=True),
            "complete": is_complete,
        },
    )


async def invoices_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    """
    Asynchronously retrieves invoices from the LND node and logs them.
    Args:
        client (LNDClient): The LND client to use for the connection.

    Returns:
        None
    """
    recent_invoice = await get_most_recent_invoice()
    request_sub = lnrpc.InvoiceSubscription(
        add_index=recent_invoice.add_index, settle_index=recent_invoice.settle_index
    )
    while True:
        try:
            async for lnrpc_invoice in lnd_client.call_async_generator(
                lnd_client.lightning_stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Docker Shutdown")
                lnrpc_invoice: lnrpc.Invoice
                async_publish(
                    event_name=Events.LND_INVOICE,
                    htlc_event=lnrpc_invoice,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error, call_name="SubscribeInvoices"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in invoices_loop", exc_info=e, stack_info=True)
            raise e
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except Exception as e:
            logger.exception(e)
            pass


async def payments_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    request = routerrpc.TrackPaymentRequest(no_inflight_updates=False)
    while True:
        try:
            async for lnrpc_payment in lnd_client.call_async_generator(
                lnd_client.router_stub.TrackPayments,
                request,
                call_name="TrackPayments",
            ):
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Docker Shutdown")
                lnrpc_payment: lnrpc.Payment
                async_publish(
                    event_name=Events.LND_PAYMENT,
                    htlc_event=lnrpc_payment,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error, call_name="TrackPayments"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in payments_loop", exc_info=e, stack_info=True)
            raise e
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except Exception as e:
            logger.exception(e)
            pass


async def htlc_events_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    request = routerrpc.SubscribeHtlcEventsRequest()
    while True:
        try:
            async for htlc_event in lnd_client.call_async_generator(
                lnd_client.router_stub.SubscribeHtlcEvents,
                request,
                call_name="SubscribeHtlcEvents",
            ):
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Docker Shutdown")
                htlc_event: routerrpc.HtlcEvent
                async_publish(
                    event_name=Events.HTLC_EVENT,
                    htlc_event=htlc_event,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in payments_loop", exc_info=e, stack_info=True)
            raise e
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except Exception as e:
            logger.exception(e)
            pass


async def fill_channel_names(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    request = lnrpc.ListChannelsRequest()
    channels = await lnd_client.call(
        lnd_client.lightning_stub.ListChannels,
        request,
    )
    channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
    # Get the name of each channel
    tasks = []
    for channel in channels_dict.get("channels", []):
        tasks.append(
            get_channel_name(
                channel_id=int(channel["chan_id"]),
                lnd_client=lnd_client,
            )
        )
    names_list: List[LndChannelName] = await asyncio.gather(*tasks)
    for channel_name in names_list:
        lnd_events_group.append(channel_name)
        logger.info(
            (f"{lnd_client.icon} Channel {channel_name.channel_id} -> {channel_name.name}"),
            extra={"channel_name": channel_name.to_dict()},
        )


async def read_all_invoices(lnd_client: LNDClient) -> None:
    """
    Reads all invoices from the LND client and inserts them into a MongoDB collection.

    This function continuously fetches invoices from the LND client in batches
    and inserts them into a MongoDB collection.
    It stops fetching when the number of invoices in a batch is less than the maximum
    number of invoices per batch.

    Args:
        lnd_client (LNDClient): The LND client used to fetch invoices.

    Returns:
        None
    """
    try:
        async with MongoDBClient(
            db_conn="local_connection", db_name=DATABASE_NAME, db_user="lnd_monitor"
        ) as db_client:
            index_offset = 0
            num_max_invoices = 1000
            total_invoices = 0
            logger.info(f"{lnd_client.icon} Reading all invoices...")
            while True:
                request = lnrpc.ListInvoiceRequest(
                    pending_only=False,
                    index_offset=index_offset,
                    num_max_invoices=num_max_invoices,
                    reversed=True,
                )
                invoices_raw: lnrpc.ListInvoiceResponse = await lnd_client.call(
                    lnd_client.lightning_stub.ListInvoices,
                    request,
                )
                list_invoices = ListInvoiceResponse(invoices_raw)
                index_offset = list_invoices.first_index_offset
                insert_data = []
                tasks = []
                for invoice in list_invoices.invoices:
                    insert_one = invoice.model_dump(exclude_none=True, exclude_unset=True)
                    insert_data.append(insert_one)
                    query = {"r_hash": invoice.r_hash}
                    tasks.append(
                        db_client.update_one(
                            "invoices", query=query, update=insert_one, upsert=True
                        )
                    )
                try:
                    ans = await asyncio.gather(*tasks)
                    modified = [a.modified_count for a in ans]
                    inserted = [a.did_upsert for a in ans]
                    logger.info(
                        f"{lnd_client.icon} {DATABASE_ICON} "
                        f"Invoices {index_offset}... "
                        f"modified: {sum(modified)} inserted: {sum(inserted)}"
                    )
                    total_invoices += len(list_invoices.invoices)
                except BulkWriteError as e:
                    logger.debug(e.details)
                    pass
                if len(list_invoices.invoices) < num_max_invoices:
                    logger.info(
                        f"{lnd_client.icon} {DATABASE_ICON} "
                        f"Finished reading {total_invoices} invoices..."
                    )
                    break
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
        return


async def read_all_payments(lnd_client: LNDClient) -> None:
    """
    Reads all payments from the LND client and inserts them into a MongoDB collection.

    This function continuously fetches payments from the LND client in batches and
    inserts them into a MongoDB collection.
    It stops fetching when the number of payments in a batch is less than the
    maximum number of payments per batch.

    Args:
        lnd_client (LNDClient): The LND client used to fetch payments.

    Returns:
        None
    """
    try:
        async with MongoDBClient(
            db_conn="local_connection", db_name=DATABASE_NAME, db_user="lnd_monitor"
        ) as db_client:
            index_offset = 0
            num_max_payments = 1000
            total_payments = 0
            logger.info(f"{lnd_client.icon} Reading all payments...")
            while True:
                request = lnrpc.ListPaymentsRequest(
                    include_incomplete=True,
                    index_offset=index_offset,
                    max_payments=num_max_payments,
                    reversed=True,
                )
                payments_raw: lnrpc.ListPaymentsResponse = await lnd_client.call(
                    lnd_client.lightning_stub.ListPayments,
                    request,
                )
                list_payments = ListPaymentsResponse(payments_raw)
                index_offset = payments_raw.first_index_offset
                insert_data = []
                tasks = []
                for payment in list_payments.payments:
                    await update_payment_route_with_alias(
                        db_client=db_client,
                        lnd_client=lnd_client,
                        payment=payment,
                        fill_cache=True,
                        col_pub_keys="pub_keys",
                    )
                    insert_one = payment.model_dump(exclude_none=True, exclude_unset=True)
                    insert_data.append(insert_one)
                    query = {"payment_hash": payment.payment_hash}
                    tasks.append(
                        db_client.update_one(
                            "payments", query=query, update=insert_one, upsert=True
                        )
                    )
                try:
                    ans = await asyncio.gather(*tasks)
                    modified = [a.modified_count for a in ans]
                    inserted = [a.did_upsert for a in ans]
                    logger.info(
                        f"{lnd_client.icon} {DATABASE_ICON} "
                        f"Payments {index_offset}... "
                        f"modified: {sum(modified)} inserted: {sum(inserted)}"
                    )
                    total_payments += len(list_payments.payments)
                except BulkWriteError as e:
                    logger.debug(e.details)
                    pass
                except Exception as e:
                    logger.exception(str(e), extra={"error": e})
                if len(list_payments.payments) < num_max_payments:
                    logger.info(
                        f"{lnd_client.icon} {DATABASE_ICON} "
                        f"Finished reading {total_payments} payments..."
                    )
                    break
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
        return


async def get_most_recent_invoice() -> Invoice:
    async with MongoDBClient(
        db_conn="local_connection", db_name=DATABASE_NAME, db_user="lnd_monitor"
    ) as db_client:
        query = {}
        sort = [("creation_date", -1)]
        collection = db_client.db.get_collection("invoices")
        cursor = collection.find(query)
        cursor.sort(sort)
        async for ans in cursor:
            invoice = Invoice(**ans)
            break
        logger.info(
            f"{DATABASE_ICON} Most recent invoice: {invoice.add_index} {invoice.settle_index}"
        )
        return invoice


async def main_async_start(connection_name: str) -> None:
    """
    Main function to run the node monitor.
    Args:
        connection_name (str): The name of the connection to monitor.

    Returns:
        None
    """
    try:
        # Get the current event loop
        loop = asyncio.get_event_loop()

        # Register signal handlers for SIGTERM and SIGINT
        loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
        loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

        global DATABASE_NAME
        DATABASE_NAME = f"lnd_monitor_v2_{connection_name}"
        lnd_events_group = LndEventsGroup()
        async with LNDClient(connection_name) as lnd_client:
            logger.info(
                f"{lnd_client.icon} 🔍 Monitoring node... {connection_name} {DATABASE_NAME}",
                extra={"notification": True},
            )
            if lnd_client.get_info:
                logger.info(
                    f"{lnd_client.icon} Node: {lnd_client.get_info.alias} "
                    f"pub_key: {lnd_client.get_info.identity_pubkey}"
                )
            await fill_channel_names(lnd_client, lnd_events_group)
            # It is important to subscribe to the track_events function
            # before the reporting functions The track_events function will
            # group events and report them when the group is complete
            async_subscribe(
                [
                    Events.LND_INVOICE,
                    Events.LND_PAYMENT,
                    Events.HTLC_EVENT,
                ],
                track_events,
            )
            # raise Exception("Test error in lnd_monitor_v2.py")
            async_subscribe(Events.LND_INVOICE, db_store_invoice)
            async_subscribe(Events.LND_PAYMENT, db_store_payment)
            async_subscribe(Events.LND_INVOICE, invoice_report)
            async_subscribe(Events.LND_PAYMENT, payment_report)
            async_subscribe(Events.HTLC_EVENT, htlc_event_report)
            tasks = [
                read_all_invoices(lnd_client),
                read_all_payments(lnd_client),
                invoices_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                payments_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                htlc_events_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                check_for_shutdown(),
            ]
            await asyncio.gather(*tasks)

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("👋 Received signal to stop. Exiting...")
        if hasattr(lnd_client, "channel") and lnd_client.channel:
            await lnd_client.channel.close()

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(
            f"{lnd_client.icon} Irregular shutdown in LND Monitor {e}",
            extra={"error": e},
        )
        if hasattr(lnd_client, "channel") and lnd_client.channel:
            await lnd_client.channel.close()
        await asyncio.sleep(0.2)
        raise e

    finally:
        # Cancel all tasks except the current one
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


    logger.info(
        f"{lnd_client.icon} ✅ LND gRPC client shutting down. "
        f"Monitoring node: {connection_name}. Version: {CONFIG.version}",
        extra={"notification": True},
    )
    await asyncio.sleep(0.2)


async def check_for_shutdown():
    """
    Check for shutdown signal and wait for it to be set.
    """
    await shutdown_event.wait()
    logger.info("Shutdown signal received. Cleaning up...")
    await asyncio.sleep(0.2)
    # Perform any necessary cleanup here
    # await check_notifications()
    raise asyncio.CancelledError("Docker Shutdown")



@app.command()
def main(
    lnd_node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The LND node to monitor. If not provided, defaults to the value: "
                f"{CONFIG.default_lnd_connection}.\n"
                f"Choose from: {CONFIG.lnd_connections_names}"
            )
        ),
    ] = CONFIG.default_lnd_connection,
    database: Annotated[
        str,
        typer.Argument(help=(f"The database to monitor.Choose from: {CONFIG.dbs_names}")),
    ] = CONFIG.default_db_name,
):
    f"""
    Main function to run the node monitor.
    Args:
        lnd_node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        {CONFIG.lnd_connections_names}

    Returns:
        None
    """
    icon = CONFIG.lnd_connections[lnd_node].icon
    logger.info(
        f"{icon} ✅ LND gRPC client started. "
        f"Monitoring node: {lnd_node} {icon}. Version: {CONFIG.version}"
    )
    asyncio.run(main_async_start(lnd_node))
    logger.info("👋 Goodbye!")
    # INTERNAL_CONFIG.shutdown()


if __name__ == "__main__":
    try:
        logger.name = "lnd_monitor_v2"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        print(e)
        sys.exit(1)
