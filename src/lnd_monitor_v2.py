import asyncio
import contextlib
import os
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Dict, List

import typer
from colorama import Fore, Style
from google.protobuf.json_format import MessageToDict
from grpc.aio import AioRpcError  # type: ignore
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from status.status_api import StatusAPI, StatusAPIException
from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DATABASE_ICON, DBConn
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
from v4vapp_backend_v2.models.lnd_balance_models import NodeBalances
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse, Payment
from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent

ICON = "⚡"

app = typer.Typer()

# Define a global flag to track shutdown and startup completion
startup_complete_event = asyncio.Event()
shutdown_event = asyncio.Event()


@dataclass
class StatusObject:
    """
    Used to store status information for the StatusAPI health check.
    """


STATUS_OBJ = StatusObject()


async def health_check() -> Dict[str, Any]:
    """
    Asynchronous health check function that verifies the status of critical background tasks.
    Used with the `StatusAPI` to provide health monitoring API endpoint especially for docker
    containers.

    This function checks if the 'all_ops_loop' and 'store_rates' tasks are currently running
    among all asyncio tasks. It also formats the time difference in STATUS_OBJ. If any tasks
    are not running, it raises a StatusAPIException with details. Otherwise, it returns the
    STATUS_OBJ dictionary.

    Returns:
        Dict[str, Any]: The dictionary representation of STATUS_OBJ containing status information.

    Raises:
        StatusAPIException: If one or more critical tasks are not running, with a message
            listing the issues and extra data from STATUS_OBJ.
    """

    exceptions = []
    check_for_tasks = ["invoices_loop", "payments_loop", "htlc_events_loop", "channel_events_loop"]
    if not startup_complete_event.is_set():
        logger.info(f"{ICON} LND Monitor Startup not complete", extra={"notification": False})
        return STATUS_OBJ.__dict__
    for task in check_for_tasks:
        if not any(t.get_name() == task and not t.done() for t in asyncio.all_tasks()):
            exceptions.append(f"{task} task is not running")
            logger.warning(
                f"{ICON} {task} task is not running",
                extra={"notification": True, "error_code": "hive_monitor_task_failure"},
            )
            sys.exit(1)

    if exceptions:
        raise StatusAPIException(", ".join(exceptions), extra=STATUS_OBJ.__dict__)
    logger.debug(
        f"{ICON} Health check passed",
        extra={"notification": False, "error_code_clear": "hive_monitor_task_failure"},
    )
    return STATUS_OBJ.__dict__


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
    try:
        htlc_event_dict = MessageToDict(htlc_event, preserving_proto_field_name=True)
    except Exception:
        htlc_event_dict = {}
    invoice_dict = {}
    if lnd_events_group.complete_group(event=htlc_event):
        incoming_invoice = None
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
                    settled = incoming_invoice.settled
                    notification = False if amount < 10 or not settled else notification
                    invoice_dict = MessageToDict(
                        incoming_invoice, preserving_proto_field_name=True
                    )
            except Exception as e:
                logger.exception(e)
                pass
        await asyncio.sleep(0.2)
        message_str, ans_dict = lnd_events_group.message(htlc_event, dest_alias=dest_alias)
        forward_success = False
        if check_for_attempted_forwards(htlc_event, message_str):
            silent = True
            notification = False
            forward_success = False
        else:
            forward_success = True
            silent = False
        if not (" Attempted 0 " in message_str or "UNKNOWN 0 " in message_str):
            ans_dict["htlc_event_dict"] = htlc_event_dict
            ans_dict["forward_success"] = forward_success
            logger.info(
                f"{lnd_client.icon} {message_str}",
                extra={
                    "notification": notification,
                    "silent": silent,
                    type(htlc_event).__name__: ans_dict,
                    "incoming_invoice": invoice_dict if incoming_invoice else None,
                },
            )
            if ans_dict.get("message_type") == "FORWARD" and forward_success:
                try:
                    forward_event = TrackedForwardEvent.model_validate(ans_dict)
                    asyncio.create_task(db_store_htlc_event(forward_event=forward_event))
                except Exception as e:
                    logger.warning(
                        f"Could not save HTLC event: {e}", extra={"notification": False}
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
                    return await fetch_dest_alias_from_request(
                        matching_payment.payment_request, lnd_client
                    )
                else:
                    return "Keysend"
    # Keysend payments outgoing do not have a payment request
    if isinstance(htlc_event, lnrpc.Payment):
        if htlc_event.payment_request:
            return await fetch_dest_alias_from_request(htlc_event.payment_request, lnd_client)
        else:
            return "Keysend"

    return ""


async def fetch_dest_alias_from_request(payment_request: str, lnd_client: LNDClient) -> str:
    """
    Safely fetch node alias from a payment request string. Returns 'Unknown' on failure.

    Args:
        payment_request: The BOLT-11 payment request string.
        lnd_client: The LND client instance used for RPC.

    Returns:
        The resolved node alias as a string, or 'Unknown' if lookup failed.
    """
    try:
        dest_alias = await get_node_alias_from_pay_request(payment_request, lnd_client)
        return dest_alias
    except LNDConnectionError as e:
        logger.warning(
            f"{getattr(lnd_client, 'icon', '')} Could not fetch dest alias (connection): {e}",
            extra={"notification": False},
        )
        return "Unknown"
    except Exception as e:
        logger.warning(
            f"{getattr(lnd_client, 'icon', '')} Could not fetch dest alias: {e}",
            extra={"notification": False},
        )
        return "Unknown"


async def remove_event_group(
    htlc_event: EventItem, lnd_client: LNDClient, lnd_events_group: LndEventsGroup
) -> None:
    """
    Asynchronously removes an event from the specified LndEventsGroup after a delay.
    """
    # Exit early on shutdown; otherwise delay up to 10s before cleanup
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=10)
        return
    except asyncio.TimeoutError:
        pass
    lnd_events_group.remove_group(htlc_event)


async def db_store_invoice(
    htlc_event: lnrpc.Invoice,
    lnd_client: LNDClient,
    *args: Any,
    **kwargs,
) -> None:
    """
    Asynchronously stores an invoice in the MongoDB database.

    Args:
        invoice (lnrpc.Invoice): The invoice to store.

    Returns:
        None
    """
    try:
        invoice_pyd = Invoice(htlc_event)
        await invoice_pyd.update_conv()
        ans = await invoice_pyd.save()
        logger.info(
            f"{lnd_client.icon}{DATABASE_ICON} "
            f"New invoice recorded: {invoice_pyd.add_index:>6} {invoice_pyd.r_hash}",
            extra={"db_ans": ans.raw_result, **invoice_pyd.log_extra},
        )
    except Exception as e:
        logger.warning(e)
        return


async def db_store_payment(
    htlc_event: lnrpc.Payment,
    lnd_client: LNDClient,
    *args: Any,
    **kwargs,
) -> None:
    """
    Asynchronously stores a payment in the MongoDB database.

    Args:
        payment (lnrpc.Payment): The payment to store.

    Returns:
        None
    """
    try:
        payment_pyd = Payment(htlc_event)
        await update_payment_route_with_alias(
            lnd_client=lnd_client,
            payment=payment_pyd,
            fill_cache=True,
            col_pub_keys="pub_keys",
        )
        await payment_pyd.update_conv()
        ans = await payment_pyd.save()
        logger.info(
            f"{lnd_client.icon}{DATABASE_ICON} "
            f"Storing payment: {htlc_event.payment_index} "
            f"{payment_pyd.route_str}",
            extra={"db_ans": ans.raw_result, **payment_pyd.log_extra},
        )

    except Exception as e:
        logger.info(e)
        return


async def db_store_htlc_event(
    forward_event: TrackedForwardEvent,
) -> None:
    """
    Asynchronously stores an HTLC event in the MongoDB database.

    Args:
        htlc_event_ans (Dict[str, Any]): The HTLC event data to store as returned to the logger.
    Returns:
        None
    """
    await forward_event.save()


async def node_balance_report(
    lnd_client: LNDClient,
) -> None:
    """
    Asynchronously fetches and logs the current node balances.

    Args:
        lnd_client (LNDClient): The LND client instance used for RPC.

    Returns:
        None
    """
    try:
        balances = NodeBalances()
        await balances.fetch_balances(lnd_client=lnd_client)
        if balances:
            logger.info(f"{lnd_client.icon} {balances.log_str}", extra={**balances.log_extra})
            await balances.save()

    except Exception as e:
        logger.warning(
            f"{lnd_client.icon} Could not fetch or log node balance: {e}",
            extra={"notification": False},
        )


async def invoice_report(
    htlc_event: lnrpc.Invoice,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup | None = None,
) -> None:
    asyncio.create_task(node_balance_report(lnd_client=lnd_client))
    expiry_datetime = datetime.fromtimestamp(
        htlc_event.creation_date + htlc_event.expiry, tz=timezone.utc
    )
    time_to_expire = expiry_datetime - datetime.now(tz=timezone.utc)
    if time_to_expire.total_seconds() < 0:
        time_to_expire = timedelta(seconds=0)
    time_to_expire_str = format_time_delta(time_to_expire)
    invoice_dict = MessageToDict(htlc_event, preserving_proto_field_name=True)
    notification = True if invoice_dict.get("state") == "SETTLED" else False
    logger.info(
        (
            f"{lnd_client.icon} Invoice: {htlc_event.add_index:>6} "
            f"amount: {htlc_event.value:>10,} sat {htlc_event.settle_index} "
            f"expiry: {time_to_expire_str} "
            f"{invoice_dict.get('r_hash')}"
        ),
        extra={"notification": notification, "invoice": invoice_dict},
    )


async def payment_report(
    htlc_event: lnrpc.Payment,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    status = lnrpc.Payment.PaymentStatus.Name(htlc_event.status)
    creation_date = datetime.fromtimestamp(htlc_event.creation_time_ns / 1e9, tz=timezone.utc)
    pre_image = htlc_event.payment_preimage if htlc_event.payment_preimage else ""
    asyncio.create_task(node_balance_report(lnd_client=lnd_client))
    try:
        dest_alias = await get_node_alias_from_pay_request(htlc_event.payment_request, lnd_client)
    except LNDConnectionError as e:
        logger.warning(
            f"{lnd_client.icon} Could not fetch dest alias (connection): {e}",
            extra={"notification": False},
        )
        dest_alias = "Unknown"
    except ValueError as e:
        logger.warning(
            f"{lnd_client.icon} Could not fetch or save node balance: {e}",
            extra={"notification": False},
        )
        dest_alias = "Unknown"
    except Exception as e:
        logger.warning(
            f"{lnd_client.icon} Could not fetch dest alias: {e}",
            extra={"notification": False},
        )
        dest_alias = "Unknown"
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
    """Log a human-readable report for a single HTLC event.

    This asynchronous helper formats and logs key information about an incoming
    or outgoing HTLC (Hashed Time-Locked Contract) event observed from LND.
    It derives a textual event type, selects the relevant HTLC id (incoming or
    outgoing), extracts the settle preimage when present, and determines whether
    the event completes a logical HTLC group via the provided LndEventsGroup.
    A short emoji indicates completion (💎) or non-completion (🔨). The full
    event is serialized into a dictionary and included in structured logging
    metadata under the "htlc_event" key, and the completion boolean is included
    under "complete".

    Args:
        htlc_event (routerrpc.HtlcEvent): The raw HTLC event message from LND.
        lnd_client (LNDClient): Client wrapper used for contextual info (e.g. icon).
        lnd_events_group (LndEventsGroup): Helper used to determine whether the
            HTLC event completes a group of related events.

    Returns:
        None

    Side effects:
        Emits an INFO-level log entry containing a concise human-readable message
        and structured metadata for downstream processing or debugging.
    """
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


async def invoices_loop(
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    """
    Asynchronously retrieves invoices from the LND node and logs them.
    Args:
        client (LNDClient): The LND client to use for the connection.

    Returns:
        None
    """

    recent_invoice = await get_most_recent_invoice()
    if recent_invoice:
        add_index = recent_invoice.add_index
        settle_index = recent_invoice.settle_index
    else:
        add_index = 0
        settle_index = 0
        # Don’t block shutdown for 10s if the DB is empty
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=10)
            return
        except asyncio.TimeoutError:
            pass

    request_sub = lnrpc.InvoiceSubscription(
        add_index=int(add_index) if add_index is not None else 0,
        settle_index=int(settle_index) if settle_index is not None else 0,
    )
    while True:
        try:
            async for lnrpc_invoice in lnd_client.call_async_generator(
                lnd_client.lightning_stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                if shutdown_event.is_set():
                    return
                await TrackedBaseModel.update_quote()
                lnrpc_invoice: lnrpc.Invoice
                async_publish(
                    event_name=Events.LND_INVOICE,
                    htlc_event=lnrpc_invoice,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error
                if hasattr(e, "original_error") and isinstance(e.original_error, AioRpcError)
                else None,
                call_name="SubscribeInvoices",
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
                    return
                lnrpc_payment: lnrpc.Payment
                await TrackedBaseModel.update_quote()
                async_publish(
                    event_name=Events.LND_PAYMENT,
                    htlc_event=lnrpc_payment,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error
                if hasattr(e, "original_error") and isinstance(e.original_error, AioRpcError)
                else None,
                call_name="TrackPayments",
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
                    return
                htlc_event: routerrpc.HtlcEvent
                async_publish(
                    event_name=Events.HTLC_EVENT,
                    htlc_event=htlc_event,
                    lnd_client=lnd_client,
                    lnd_events_group=lnd_events_group,
                )
        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error
                if hasattr(e, "original_error") and isinstance(e.original_error, AioRpcError)
                else None,
                call_name="SubscribeHtlcEvents",
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


async def get_channel_display_name(
    chan_id: int | None,
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> str:
    """
    Get the display name for a channel ID, with fallback to direct lookup if not cached.

    Args:
        chan_id: The channel ID to look up
        lnd_client: The LND client for direct lookups
        lnd_events_group: The events group containing cached channel names

    Returns:
        The channel name or 'Unknown' if not found
    """
    if not chan_id:
        return "Unknown"

    # First try to get from cache
    cached_name = lnd_events_group.channel_names.get(int(chan_id))
    if cached_name and isinstance(cached_name, str):
        return cached_name

    # If not in cache, try direct lookup
    try:
        channel_name_obj = await get_channel_name(
            channel_id=int(chan_id),
            lnd_client=lnd_client,
        )
        return channel_name_obj.name if channel_name_obj else "Unknown"
    except LNDConnectionError as e:
        rpc_err = e.args[1] if len(e.args) > 1 else None
        details = ""
        try:
            if rpc_err and hasattr(rpc_err, "details"):
                details = rpc_err.details()
            else:
                details = getattr(rpc_err, "_details", "") or str(rpc_err)
        except Exception:
            details = str(rpc_err)
        if "edge not found" in str(details).lower():
            logger.warning(f"{lnd_client.icon} get_channel_name: channel {chan_id} not found")
            return "Unknown"
        logger.exception(e)
        return "Unknown"
    except Exception:
        return "Unknown"


async def channel_events_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    """Subscribe to channel events from LND"""
    request = lnrpc.ChannelEventSubscription()

    while True:
        try:
            async for channel_event in lnd_client.call_async_generator(
                lnd_client.lightning_stub.SubscribeChannelEvents,
                request,
                call_name="SubscribeChannelEvents",
            ):
                # channel_event is of type lnrpc.ChannelEventUpdate
                # It has different types including:
                # - OPEN_CHANNEL
                # - CLOSED_CHANNEL
                # - ACTIVE_CHANNEL
                # - INACTIVE_CHANNEL
                # - PENDING_OPEN_CHANNEL

                decoded_event = MessageToDict(channel_event, preserving_proto_field_name=True)
                logger.info("Channel event received", extra={"channel_event": decoded_event})

                # Process the different event types
                if "open_channel" in decoded_event:
                    channel = decoded_event["open_channel"]
                    await fill_channel_names(lnd_client, lnd_events_group)
                    chan_id = channel.get("chan_id", 0)
                    channel_name = await get_channel_display_name(
                        chan_id, lnd_client, lnd_events_group
                    )
                    logger.info(
                        f"{lnd_client.icon} Channel opened: {chan_id} {channel_name}",
                        extra={
                            "notification": True,
                        },
                    )

                elif "closed_channel" in decoded_event:
                    channel = decoded_event["closed_channel"]
                    chan_id = channel.get("chan_id", 0)
                    channel_name = await get_channel_display_name(
                        chan_id, lnd_client, lnd_events_group
                    )
                    logger.info(
                        f"{lnd_client.icon} Channel closed: {chan_id} {channel_name}",
                        extra={"notification": True},
                    )
                    await fill_channel_names(lnd_client, lnd_events_group)

                elif "active_channel" in decoded_event:
                    channel = decoded_event["active_channel"]
                    # Active channel events might not have chan_id, so we need to handle this
                    chan_id = channel.get("chan_id")
                    if chan_id:
                        channel_name = await get_channel_display_name(
                            chan_id, lnd_client, lnd_events_group
                        )
                        logger.info(
                            f"{lnd_client.icon} Channel active: {chan_id} {channel_name}",
                            extra={"notification": True},
                        )
                    else:
                        # Handle case where chan_id is not available
                        funding_txid = channel.get("funding_txid_bytes", "Unknown")
                        logger.info(
                            f"{lnd_client.icon} Channel active: funding_txid={funding_txid}",
                            extra={"notification": False},
                        )
                    await fill_channel_names(lnd_client, lnd_events_group)

                elif "inactive_channel" in decoded_event:
                    channel = decoded_event["inactive_channel"]
                    chan_id = channel.get("chan_id", 0)
                    funding_txid = channel.get("funding_txid_bytes", "Unknown")
                    channel_name = await get_channel_display_name(
                        chan_id, lnd_client, lnd_events_group
                    )
                    logger.info(
                        f"{lnd_client.icon} Channel inactive: {chan_id} {channel_name} {funding_txid}",
                        extra={"notification": False},
                    )
                    await fill_channel_names(lnd_client, lnd_events_group)

                elif "pending_open_channel" in decoded_event:
                    channel = decoded_event["pending_open_channel"]
                    chan_id = channel.get("chan_id", 0)
                    channel_name = await get_channel_display_name(
                        chan_id, lnd_client, lnd_events_group
                    )
                    logger.info(
                        f"{lnd_client.icon} Pending channel open: {chan_id} {channel_name}",
                        extra={"notification": True},
                    )
                    await fill_channel_names(lnd_client, lnd_events_group)

        except LNDSubscriptionError as e:
            await lnd_client.check_connection(
                original_error=e.original_error
                if hasattr(e, "original_error") and isinstance(e.original_error, AioRpcError)
                else None,
                call_name="SubscribeHtlcEvents",
            )
            pass
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in channel_events_loop", exc_info=e, stack_info=True)
            raise e
        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except Exception as e:
            logger.exception(e)
            pass


async def fill_channel_names(
    lnd_client: LNDClient,
    lnd_events_group: LndEventsGroup,
) -> None:
    """
    Asynchronously fills the channel names for a given LND client and appends them to the provided LndEventsGroup.

    This function retrieves the list of channels from the LND client, fetches the name of each channel asynchronously,
    and appends the resulting channel names to the provided `lnd_events_group`. It also logs the channel names.

    Args:
        lnd_client (LNDClient): The LND client instance used to interact with the Lightning Network Daemon.
        lnd_events_group (LndEventsGroup): The group to which the channel names will be appended.

    Returns:
        None: This function does not return a value. It performs asynchronous operations and updates the provided group.
    """
    try:
        request = lnrpc.ListChannelsRequest()
        channels = await lnd_client.call(
            lnd_client.lightning_stub.ListChannels,
            request,
        )
        channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
        if len(channels_dict.get("channels", [])) == len(lnd_events_group.channel_names):
            logger.debug("No new channels to fill")
            return
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

    except LNDConnectionError:
        logger.error("🔴 Connection error in fill_channel_names", extra={"notification": False})
        await asyncio.sleep(59)
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
        return
    except Exception as e:
        logger.exception(e, extra={"notification": False})
        await asyncio.sleep(10)


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
        index_offset = 0
        num_max_invoices = 1000
        total_invoices = 0
        logger.info(f"{lnd_client.icon} Reading all invoices...")
        while True:
            if shutdown_event.is_set():
                return
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
            bulk_updates = []
            for invoice in list_invoices.invoices:
                insert_one = invoice.model_dump(
                    exclude_none=True, exclude_unset=True, exclude={"conv"}
                )
                query = {"r_hash": invoice.r_hash}
                read_invoice = await Invoice.collection().find_one(
                    filter=query,
                )
                if read_invoice:
                    continue
                    # this match is only necessary if running for the first time or filling an empty database
                    try:
                        db_invoice = Invoice(**read_invoice)
                        if db_invoice == invoice:
                            continue
                    except Exception as e:
                        logger.warning(e, extra={"notification": False, "invoice": read_invoice})
                        pass
                bulk_updates.append(
                    {
                        "filter": query,
                        "update": {"$set": insert_one},
                        "upsert": True,
                    }
                )
            try:
                if bulk_updates:
                    result = await Invoice.collection().bulk_write(
                        requests=[
                            UpdateOne(update["filter"], update["update"], upsert=update["upsert"])
                            for update in bulk_updates
                        ],
                    )
                    modified = result.modified_count
                    inserted = result.inserted_count
                else:
                    modified = 0
                    inserted = 0
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Invoices {index_offset}... "
                    f"modified: {modified} inserted: {inserted}"
                )
                total_invoices += len(list_invoices.invoices)
            except BulkWriteError as e:
                logger.debug(e.details)
                pass
            except Exception as e:
                logger.exception(str(e), extra={"error": e})
                break
            if len(list_invoices.invoices) < num_max_invoices:
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Finished reading {total_invoices} invoices..."
                )
                break
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
        raise e


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
        index_offset = 0
        num_max_payments = 1000
        total_payments = 0
        logger.info(f"{lnd_client.icon} Reading all payments...")
        while True:
            if shutdown_event.is_set():
                return
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
            bulk_updates = []
            for payment in list_payments.payments:
                query = {"payment_hash": payment.payment_hash}
                read_payment = await Payment.collection().find_one(
                    filter=query,
                )
                if read_payment and read_payment.get("route_str"):
                    continue
                    try:
                        db_payment = Payment.model_validate(read_payment)
                        if db_payment == payment:
                            continue
                    except Exception as e:
                        logger.warning(e, extra={"notification": False, "payment": read_payment})
                        pass
                await update_payment_route_with_alias(
                    lnd_client=lnd_client,
                    payment=payment,
                    fill_cache=True,
                    col_pub_keys="pub_keys",
                )
                insert_one = payment.model_dump(
                    exclude_none=True, exclude_unset=True, exclude={"conv", "conv_fee"}
                )
                bulk_updates.append(
                    {
                        "filter": query,
                        "update": {"$set": insert_one},
                        "upsert": True,
                    }
                )
            try:
                if bulk_updates:
                    result = await Payment.collection().bulk_write(
                        requests=[
                            UpdateOne(update["filter"], update["update"], upsert=update["upsert"])
                            for update in bulk_updates
                        ],
                    )
                    modified = result.modified_count
                    inserted = result.inserted_count
                else:
                    modified = 0
                    inserted = 0
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Payments {index_offset}... "
                    f"modified: {modified} inserted: {inserted}"
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
        raise e
    except Exception as e:
        logger.exception(e, extra={"error": e})
        return


async def get_most_recent_invoice() -> Invoice | None:
    """
    Fetches the most recent invoice from the MongoDB collection.

    This asynchronous function retrieves the most recent invoice document
    from the "invoices" collection in the MongoDB database. The invoices
    are sorted by the "creation_date" field in descending order to ensure
    the latest invoice is selected.

    Returns:
        Invoice: An instance of the `Invoice` class representing the most
        recent invoice.

    Raises:
        Exception: If there is an issue with database connectivity or data
        parsing.

    Logs:
        Logs the `add_index` and `settle_index` of the most recent invoice
        for debugging and monitoring purposes.
    """
    query = {}
    sort = [("add_index", -1)]
    collection = Invoice.collection()
    cursor = collection.find(query)
    cursor.sort(sort)
    invoice = None
    try:
        async for ans in cursor:
            invoice = Invoice(**ans)
            break
        if not invoice:
            logger.warning("No invoices found, empty database")
            return None
        logger.info(
            f"{DATABASE_ICON} Most recent invoice: {invoice.add_index} {invoice.settle_index}"
        )
        if invoice:
            return invoice
    except Exception as e:
        logger.warning(f"No invoices found, empty database {e}")
    return None


async def get_most_recent_payment() -> Payment | None:
    """
    Fetches the most recent payment from the MongoDB collection.

    This asynchronous function retrieves the most recent payment document
    from the "payments" collection in the MongoDB database. The payments
    are sorted by the "creation_date" field in descending order to ensure
    the latest payment is selected.

    Returns:
        Payment: An instance of the `Payment` class representing the most
        recent payment.

    Raises:
        Exception: If there is an issue with database connectivity or data
        parsing.
    """
    query = {}
    sort = [("creation_date", -1)]
    collection = Payment.collection()
    cursor = collection.find(query)
    cursor.sort(sort)
    payment = None
    try:
        async for ans in cursor:
            payment = Payment(**ans)
            break
        if not payment:
            logger.warning("No payments found, empty database")
            return None
        logger.info(
            f"{DATABASE_ICON} Most recent payment: {payment.payment_index} {payment.creation_date}"
        )
        if payment:
            return payment
    except Exception as e:
        logger.warning(f"No payments found, empty database {e}")
    return None


async def synchronize_db(
    lnd_client: LNDClient,
    delay: int = 10,
) -> None:
    """
    Synchronizes the database with the LND client.

    This function retrieves all invoices from the LND client and stores them
    in the specified MongoDB collection. It also handles any exceptions that
    may occur during the process.

    Args:
        lnd_client (LNDClient): The LND client instance used to interact with
            the Lightning Network Daemon.
        delay (int): The delay in seconds before starting the synchronization
            process. Default is 10 seconds.

    Returns:
        None: This function does not return a value. It performs asynchronous
            operations and updates the database after waiting for 10 seconds.
    """
    sync_tasks = [
        read_all_invoices(lnd_client),
        read_all_payments(lnd_client),
    ]
    # Allow early exit during initial delay
    try:
        await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
        return
    except asyncio.TimeoutError:
        pass
    await asyncio.gather(*sync_tasks)


async def main_async_start(connection_name: str) -> None:
    """
    Main function to run the node monitor.
    Args:
        connection_name (str): The name of the connection to monitor.

    Returns:
        None
    """
    process_name = os.path.splitext(os.path.basename(__file__))[0]
    health_check_port = os.environ.get("HEALTH_CHECK_PORT", "6001")
    status_api = StatusAPI(
        port=int(health_check_port),
        health_check_func=health_check,
        shutdown_event=shutdown_event,
        process_name=process_name,
        version=__version__,
    )  # Use a port from config if needed

    lnd_client = None
    running_tasks: list[asyncio.Task] = []
    try:
        db_conn = DBConn()
        await db_conn.setup_database()
        await TrackedBaseModel.update_quote()
        # Get the current event loop
        loop = asyncio.get_event_loop()

        # Register signal handlers for SIGTERM and SIGINT
        loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
        loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

        lnd_events_group = LndEventsGroup()
        async with LNDClient(connection_name) as lnd_client:
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

            running_tasks = [
                asyncio.create_task(
                    invoices_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="invoices_loop",
                ),
                asyncio.create_task(
                    payments_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="payments_loop",
                ),
                asyncio.create_task(status_api.start(), name="status_api"),
            ]

            # If we haven't synced for a long time, do the sync first to avoid massive DB load
            pause_for_sync = await pause_for_database_sync()
            if pause_for_sync:
                # Run sync now (blocking) before starting the rest
                await synchronize_db(lnd_client, delay=0)
            else:
                # Schedule sync as a cancellable task
                running_tasks.append(
                    asyncio.create_task(
                        synchronize_db(lnd_client, delay=10),
                        name="synchronize_db",
                    )
                )
            running_tasks += [
                asyncio.create_task(
                    htlc_events_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="htlc_events_loop",
                ),
                asyncio.create_task(
                    channel_events_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="channel_events_loop",
                ),
            ]
            startup_complete_event.set()
            lnd_node = InternalConfig().config.lnd_config.default
            icon = InternalConfig().config.lnd_config.connections[lnd_node].icon
            logger.info(
                f"{icon}{Fore.WHITE}✅ LND gRPC client started. "
                f"Monitoring node: {lnd_node} {icon}. Version: {__version__} on {InternalConfig().local_machine_name}{Style.RESET_ALL}",
                extra={"notification": True},
            )
            # Wait for shutdown signal, then cancel streams immediately
            await shutdown_event.wait()
            for t in running_tasks:
                t.cancel()
            # Don’t hang forever; bound the wait
            try:
                await asyncio.wait_for(
                    asyncio.gather(*running_tasks, return_exceptions=True), timeout=5
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out waiting for stream tasks to cancel; continuing shutdown."
                )
            # REMOVE premature shutdown here (it prevents goodbye notification)
            # InternalConfig().shutdown()

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("👋 Received signal to stop. Exiting...")
        if lnd_client and hasattr(lnd_client, "channel") and lnd_client.channel:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(lnd_client.channel.close(), timeout=3)

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        if lnd_client and hasattr(lnd_client, "channel") and lnd_client.channel:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(lnd_client.channel.close(), timeout=3)
            await asyncio.sleep(0.2)
            raise e

    finally:
        # Ensure channel is closed with a timeout
        if lnd_client and hasattr(lnd_client, "channel") and lnd_client.channel:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(lnd_client.channel.close(), timeout=3)
        icon = hasattr(lnd_client, "icon") and lnd_client.icon if lnd_client else ""
        logger.info(
            f"{icon} ✅ LND gRPC client shutting down. "
            f"Monitoring node: {connection_name}. Version: {__version__} on {InternalConfig().local_machine_name}",
            extra={"notification": True},
        )
        # Let notifications flush before tearing down logging/redis
        await asyncio.sleep(1)
        InternalConfig().shutdown()


async def pause_for_database_sync() -> bool:
    recent_invoice = await get_most_recent_invoice()
    recent_payment = await get_most_recent_payment()
    if (
        recent_invoice
        and recent_payment
        and recent_invoice.creation_date
        and recent_payment.creation_date
    ):
        invoice_time_delta = datetime.now(tz=timezone.utc) - recent_invoice.creation_date
        payment_time_delta = datetime.now(tz=timezone.utc) - recent_payment.creation_date
        if invoice_time_delta > timedelta(days=1) and payment_time_delta > timedelta(days=1):
            logger.info(
                f"Database sync needed Invoice: {recent_invoice.creation_date} {invoice_time_delta}"
            )
            logger.info(
                f"Database sync needed Payment: {recent_payment.creation_date} {payment_time_delta}"
            )
            return True
    return False


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
    config_filename: Annotated[
        str,
        typer.Option(
            "-c",
            "--config",
            "--config-filename",
            help="The name of the config file (in a folder called ./config)",
            show_default=True,
        ),
    ] = DEFAULT_CONFIG_FILENAME,
):
    """
    Main function to run the node monitor.
    Args:
        config_filename (str): The name of the config file (in a folder called ./config).


    Returns:
        None
    """
    CONFIG = InternalConfig(config_filename=config_filename).config
    lnd_node = CONFIG.lnd_config.default
    icon = CONFIG.lnd_config.connections[lnd_node].icon
    if not lnd_node:
        logger.error("No LND node found in the config file.")
        sys.exit(1)
    logger.name = f"lnd_monitor_{lnd_node}"
    logger.info(
        f"{icon}{Fore.WHITE}✅ LND gRPC client started. "
        f"Monitoring node: {lnd_node} {icon}. Version: {__version__} on {InternalConfig().local_machine_name}{Style.RESET_ALL}",
        extra={"notification": False},
    )
    asyncio.run(main_async_start(lnd_node))
    logger.info("👋 Goodbye!")


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        print(e)
        sys.exit(1)
