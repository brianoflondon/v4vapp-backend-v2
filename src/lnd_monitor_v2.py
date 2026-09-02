import asyncio
import contextlib
import os
import signal
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

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
from v4vapp_backend_v2.config.setup import (
    DEFAULT_CONFIG_FILENAME,
    InternalConfig,
    StartupFailure,
    logger,
)
from v4vapp_backend_v2.database.db_pymongo import DATABASE_ICON, DBConn
from v4vapp_backend_v2.database.db_tools import delete_expired_unsettled_invoices
from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.grpc_models.lnd_events_group import (
    EventItem,
    LndChannelName,
    LndEventsGroup,
)
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, get_in_flight_time
from v4vapp_backend_v2.helpers.pub_key_alias import (
    decode_payment_request_and_attach,
    update_payment_route_with_alias,
)
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDConnectionError, LNDSubscriptionError
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    get_channel_name,
    get_node_alias_from_pay_request,
)
from v4vapp_backend_v2.models.invoice_models import Invoice, InvoiceState, ListInvoiceResponse
from v4vapp_backend_v2.models.lnd_balance_models import NodeBalances
from v4vapp_backend_v2.models.payment_models import ListPaymentsResponse, Payment, PaymentStatus
from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent

ICON = "⚡"

NOTIFICATION_QUITE_MODE = (
    True  # Set to True to disable notifications if db_monitor will provide these
)

app = typer.Typer()


@dataclass(frozen=True)
class LndIndexFloors:
    """Exclusive floors for LND sequence indexes (process only values strictly greater)."""

    add_index: int = 0
    settle_index: int = 0
    payment_index: int = 0

    @property
    def has_any(self) -> bool:
        return self.add_index > 0 or self.settle_index > 0 or self.payment_index > 0


def get_lnd_index_floors(connection_name: str | None = None) -> LndIndexFloors:
    """
    Load optional index floors from the LND connection config.

    Missing config fields behave as 0 (no floor).

    **Important:** ``add_index`` and ``settle_index`` are independent LND sequences.
    Many invoices are created (high add_index) while fewer settle (low settle_index).
    Never default the settle floor to the add floor — that skips live SETTLED events
    (OPEN invoices are stored, then never update when paid).
    """
    lnd_config = InternalConfig().config.lnd_config
    conn = lnd_config.connection_config(connection_name)
    if conn is None:
        return LndIndexFloors()

    add_floor = int(conn.start_add_index or 0)
    # Explicit only — do not copy start_add_index onto settle (different sequence).
    settle_floor = int(conn.start_settle_index or 0)
    payment_floor = int(conn.start_payment_index or 0)
    return LndIndexFloors(
        add_index=add_floor,
        settle_index=settle_floor,
        payment_index=payment_floor,
    )


def apply_invoice_index_floors(
    add_index: int | None,
    settle_index: int | None,
    floors: LndIndexFloors,
) -> tuple[int, int]:
    """Raise subscription cursors so they are never below configured floors."""
    add = int(add_index or 0)
    settle = int(settle_index or 0)
    return max(add, floors.add_index), max(settle, floors.settle_index)


def should_ignore_invoice_index(add_index: int | None, floors: LndIndexFloors) -> bool:
    """True when this invoice's add_index is at or below the configured floor."""
    if floors.add_index <= 0:
        return False
    return int(add_index or 0) <= floors.add_index


def should_ignore_payment_index(payment_index: int | None, floors: LndIndexFloors) -> bool:
    """True when this payment's payment_index is at or below the configured floor."""
    if floors.payment_index <= 0:
        return False
    return int(payment_index or 0) <= floors.payment_index


# Define a global flag to track shutdown and startup completion
startup_complete_event = asyncio.Event()
shutdown_event = asyncio.Event()
# Set by the watchdog so main() can sys.exit(1). sys.exit inside an asyncio
# Task does not terminate the process, so Docker never restarts the container.
_failed_restart_requested = False

# Live TrackPayments / SubscribeInvoices can hang without the task dying.
# After this many watchdog polls with LND ahead of Mongo, exit 1 so Docker
# restarts the container.
SUBSCRIPTION_LAG_POLLS_BEFORE_EXIT = 3
# Newest-first page used to find a SETTLED invoice by r_hash (not max add_index).
INVOICE_LAG_SCAN = 32
# Keep the watchdog moving if ListPayments / ListInvoices itself hangs.
LAG_RPC_TIMEOUT_SECONDS = 10
# Same retention as expired_invoices_maintenance_loop / db_tools prune.
TIMEDELTA_RETAIN_AFTER_EXPIRY = timedelta(days=1)

_active_lnd_client: LNDClient | None = None


@dataclass
class StatusObject:
    """
    Used to store status information for the StatusAPI health check.
    """


STATUS_OBJ = StatusObject()


def _task_still_running(name: str) -> bool:
    return any(t.get_name() == name and not t.done() for t in asyncio.all_tasks())


def _request_failed_restart() -> None:
    """Ask main() to exit 1 after cleanup. Safe to call from an asyncio task."""
    global _failed_restart_requested
    _failed_restart_requested = True
    shutdown_event.set()


async def _reconnect_after_stream_drop(
    lnd_client: LNDClient,
    *,
    call_name: str,
    original_error: AioRpcError | None = None,
    cancelled: BaseException | None = None,
) -> None:
    """Rebuild the shared LND channel after a dropped subscription.

    Closing the channel cancels sibling streams with CancelledError. That is
    not a shutdown — resubscribe unless ``shutdown_event`` is already set.
    """
    if shutdown_event.is_set():
        if cancelled is not None:
            raise cancelled
        return
    if cancelled is not None:
        logger.warning(
            f"{lnd_client.icon} {call_name} cancelled (likely LND channel rebuild); "
            "resubscribing after reconnect",
            extra={"notification": False},
        )
    await lnd_client.check_connection(original_error=original_error, call_name=call_name)


def _index_floors_for_client(lnd_client: LNDClient) -> LndIndexFloors:
    """Load configured start_*_index floors; missing/broken config is no floor."""
    try:
        conn_name = getattr(getattr(lnd_client, "connection", None), "name", None)
        return get_lnd_index_floors(conn_name if isinstance(conn_name, str) else None)
    except Exception:
        return LndIndexFloors()


def _invoice_expiry_date(invoice: Any) -> datetime | None:
    expiry_date = getattr(invoice, "expiry_date", None)
    if expiry_date is None:
        creation = getattr(invoice, "creation_date", None)
        expiry_secs = getattr(invoice, "expiry", None)
        if creation is not None and expiry_secs:
            expiry_date = creation + timedelta(seconds=int(expiry_secs))
    if expiry_date is None:
        return None
    if expiry_date.tzinfo is None:
        return expiry_date.replace(tzinfo=UTC)
    return expiry_date


def _lnd_invoice_is_prunable(invoice: Any, now: datetime | None = None) -> bool:
    """True when Mongo is allowed to have deleted this unpaid, expired invoice.

    Matches ``expired_unsettled_invoice_query``: settle_index == 0, not settled,
    expiry_date older than TIMEDELTA_RETAIN_AFTER_EXPIRY.
    """
    if invoice is None:
        return False
    if getattr(invoice, "state", None) == InvoiceState.SETTLED:
        return False
    if bool(getattr(invoice, "settled", False)):
        return False
    if int(getattr(invoice, "settle_index", 0) or 0) != 0:
        return False
    expiry_date = _invoice_expiry_date(invoice)
    if expiry_date is None:
        return False
    as_of = now or datetime.now(tz=UTC)
    return expiry_date < as_of - TIMEDELTA_RETAIN_AFTER_EXPIRY


_PAYMENT_LAG_INCOMPLETE_STATUSES = frozenset(
    {PaymentStatus.IN_FLIGHT, PaymentStatus.INITIATED, PaymentStatus.UNKNOWN}
)


def _payment_status_is_incomplete(status: Any) -> bool:
    """True when LND has assigned an index but the payment is not yet final."""
    if status is None:
        return False
    if isinstance(status, PaymentStatus):
        return status in _PAYMENT_LAG_INCOMPLETE_STATUSES
    return str(status).upper() in {s.value for s in _PAYMENT_LAG_INCOMPLETE_STATUSES}


async def _payment_subscription_lag_message(lnd_client: LNDClient) -> str | None:
    """
    Detect a hung TrackPayments stream: LND has a newer payment_index than Mongo.

    Quiet nodes (no new payments) are healthy. Only lag is a failure.
    Payments at or below ``start_payment_index`` are never stored, so the
    configured floor is treated as Mongo's baseline on a fresh / cut-over DB.
    Incomplete LND tips (IN_FLIGHT / INITIATED) are ignored: ListPayments
    assigns an index before TrackPayments persists the document, so treating
    that gap as hung restarts the monitor mid-payment.
    Returns None if the check cannot run (RPC/DB error).
    """
    try:
        request = lnrpc.ListPaymentsRequest(
            include_incomplete=True,
            index_offset=0,
            max_payments=1,
            reversed=True,
        )
        payments_raw = await asyncio.wait_for(
            lnd_client.call(
                lnd_client.lightning_stub.ListPayments,
                request,
            ),
            timeout=LAG_RPC_TIMEOUT_SECONDS,
        )
        list_payments = ListPaymentsResponse(payments_raw)
        newest = list_payments.payments[0] if list_payments.payments else None
        if newest is None:
            return None
        if _payment_status_is_incomplete(getattr(newest, "status", None)):
            return None
        lnd_index = int(newest.payment_index or 0)
        mongo_index = 0
        cursor = Payment.collection().find({}).sort([("payment_index", -1)]).limit(1)
        async for ans in cursor:
            mongo_index = int(ans.get("payment_index") or 0)
            break
        floors = _index_floors_for_client(lnd_client)
    except Exception:  # do not fail health on a transient RPC/DB blip
        return None
    # Exclusive floor: payment_index <= start_payment_index is never in Mongo.
    baseline = max(mongo_index, floors.payment_index)
    if lnd_index > baseline:
        return (
            f"payments_loop hung (LND payment_index {lnd_index} > baseline {baseline} "
            f"[Mongo {mongo_index}, start_payment_index {floors.payment_index}])"
        )
    return None


def _mongo_invoice_is_settled(doc: dict[str, Any] | None) -> bool:
    if not doc:
        return False
    return doc.get("state") == InvoiceState.SETTLED or bool(doc.get("settled"))


async def _invoice_subscription_lag_message(lnd_client: LNDClient) -> str | None:
    """
    Detect a hung SubscribeInvoices stream by LND's newest invoice r_hash.

    Do not compare max add_index: expired OPEN invoices are pruned from Mongo
    and add_index / settle_index are independent sequences.

    - Newest invoice (reversed ListInvoices[0]): r_hash must exist in Mongo,
      unless add_index is at/below ``start_add_index`` or the invoice is old
      enough to have been pruned (expired + unsettled past retention).
    - Any SETTLED invoice in that page above the index floors: same r_hash
      must be SETTLED in Mongo.
    """
    try:
        request = lnrpc.ListInvoiceRequest(
            pending_only=False,
            index_offset=0,
            num_max_invoices=INVOICE_LAG_SCAN,
            reversed=True,
        )
        invoices_raw = await asyncio.wait_for(
            lnd_client.call(
                lnd_client.lightning_stub.ListInvoices,
                request,
            ),
            timeout=LAG_RPC_TIMEOUT_SECONDS,
        )
        listed = ListInvoiceResponse(invoices_raw)
        invoices = listed.invoices
        if not invoices:
            return None
        floors = _index_floors_for_client(lnd_client)

        newest = invoices[0]
        if not should_ignore_invoice_index(newest.add_index, floors):
            mongo = await Invoice.collection().find_one({"r_hash": newest.r_hash})
            if mongo is None:
                # Quiet node: newest LND invoice can be a pruned expired OPEN.
                if not _lnd_invoice_is_prunable(newest):
                    short_hash = (newest.r_hash or "")[:12]
                    return (
                        f"invoices_loop hung (LND newest r_hash {short_hash}… "
                        f"add_index {newest.add_index} missing from Mongo)"
                    )
            elif newest.state == InvoiceState.SETTLED and not _mongo_invoice_is_settled(mongo):
                short_hash = (newest.r_hash or "")[:12]
                return (
                    f"invoices_loop hung (LND newest r_hash {short_hash}… "
                    f"SETTLED on LND, not SETTLED in Mongo)"
                )

        for inv in invoices:
            if inv.state != InvoiceState.SETTLED:
                continue
            # Never stored (add floor) or settle events we intentionally skip.
            if should_ignore_invoice_index(inv.add_index, floors):
                continue
            if floors.settle_index > 0 and int(inv.settle_index or 0) <= floors.settle_index:
                continue
            mongo = await Invoice.collection().find_one({"r_hash": inv.r_hash})
            if mongo is None or not _mongo_invoice_is_settled(mongo):
                short_hash = (inv.r_hash or "")[:12]
                return (
                    f"invoices_loop hung (LND settled r_hash {short_hash}… "
                    f"missing or still OPEN in Mongo)"
                )
    except Exception:  # do not fail health on a transient RPC/DB blip
        return None
    return None


async def _subscription_lag_messages(lnd_client: LNDClient) -> list[str]:
    results = await asyncio.gather(
        _payment_subscription_lag_message(lnd_client),
        _invoice_subscription_lag_message(lnd_client),
    )
    return [msg for msg in results if msg]


async def health_check() -> dict[str, Any]:
    """
    Asynchronous health check function that verifies the status of critical background tasks.
    Used with the `StatusAPI` to provide health monitoring API endpoint especially for docker
    containers.

    This function checks if the 'all_ops_loop' and 'store_rates' tasks are currently running
    among all asyncio tasks. It also formats the time difference in STATUS_OBJ. If any tasks
    are not running, it raises a StatusAPIException with details. Otherwise, it returns the
    STATUS_OBJ dictionary.

    Returns:
        dict[str, Any]: The dictionary representation of STATUS_OBJ containing status information.

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
        if not _task_still_running(task):
            exceptions.append(f"{task} task is not running")
            logger.warning(
                f"{ICON} {task} task is not running",
                extra={"notification": True, "error_code": "hive_monitor_task_failure"},
            )

    # Hung gRPC streams stay as running tasks; compare LND tip vs Mongo.
    if _active_lnd_client and not _task_still_running("synchronize_db"):
        exceptions.extend(await _subscription_lag_messages(_active_lnd_client))

    if exceptions:
        raise StatusAPIException(", ".join(exceptions), extra=STATUS_OBJ.__dict__)
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
                    invoice_dict = MessageToDict(
                        incoming_invoice, preserving_proto_field_name=True
                    )
                    settled = invoice_dict.get("state") == InvoiceState.SETTLED
                    notification = False if amount < 10 or not settled else notification
            except Exception as e:
                logger.exception(e)
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
            if NOTIFICATION_QUITE_MODE:
                notification = False
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
                    forward_event.node_name = lnd_client.connection.name
                    asyncio.create_task(
                        db_store_htlc_event(forward_event=forward_event, lnd_client=lnd_client)
                    )
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
    except TimeoutError:
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
        floors = get_lnd_index_floors(lnd_client.connection.name)
        # add_index is on the protobuf event; filter before building the full model
        raw_add_index = getattr(htlc_event, "add_index", 0)
        if should_ignore_invoice_index(raw_add_index, floors):
            logger.debug(
                f"{lnd_client.icon} Skipping invoice add_index={raw_add_index} "
                f"(<= floor {floors.add_index})",
                extra={"notification": False},
            )
            return

        invoice_pyd = Invoice(htlc_event)
        invoice_pyd.node_name = lnd_client.connection.name
        await invoice_pyd.update_conv()
        ans = await invoice_pyd.save()
        state = invoice_pyd.state.value if invoice_pyd.state else "OPEN"
        logger.info(
            f"{lnd_client.icon}{DATABASE_ICON} "
            f"Invoice {state}: add={invoice_pyd.add_index:>7} settle={invoice_pyd.settle_index:>7} "
            f"{invoice_pyd.value:,.0f} sats {invoice_pyd.r_hash}",
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
        floors = get_lnd_index_floors(lnd_client.connection.name)
        raw_payment_index = getattr(htlc_event, "payment_index", 0)
        if should_ignore_payment_index(raw_payment_index, floors):
            logger.debug(
                f"{lnd_client.icon} Skipping payment payment_index={raw_payment_index} "
                f"(<= floor {floors.payment_index})",
                extra={"notification": False},
            )
            return

        payment_pyd = Payment(htlc_event)
        payment_pyd.node_name = lnd_client.connection.name
        # Attach invoice description (decoded from payment_request) if available
        await decode_payment_request_and_attach(lnd_client=lnd_client, payment=payment_pyd)
        await update_payment_route_with_alias(
            lnd_client=lnd_client,
            payment=payment_pyd,
            fill_cache=True,
            col_pub_keys="pub_keys",
        )
        await payment_pyd.update_conv()

        ans = await payment_pyd.save()
        logger.info(
            f"{lnd_client.icon}{DATABASE_ICON} {htlc_event.payment_index} "
            f"Storing payment: "
            f"{payment_pyd.route_str} {payment_pyd.short_id}",
            extra={"db_ans": ans.raw_result, **payment_pyd.log_extra},
        )

    except Exception as e:
        logger.info(e)
        return


async def db_store_htlc_event(
    forward_event: TrackedForwardEvent,
    lnd_client: LNDClient | None = None,
) -> None:
    """
    Asynchronously stores an HTLC event in the MongoDB database.

    Args:
        forward_event: The HTLC forward event to store.
        lnd_client: Optional LND client used to stamp node_name when not already set.
    Returns:
        None
    """
    if lnd_client is not None and (
        not forward_event.node_name or forward_event.node_name == "unset"
    ):
        forward_event.node_name = lnd_client.connection.name
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
    expiry_datetime = datetime.fromtimestamp(htlc_event.creation_date + htlc_event.expiry, tz=UTC)
    time_to_expire = expiry_datetime - datetime.now(tz=UTC)
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
    creation_date = datetime.fromtimestamp(htlc_event.creation_time_ns / 1e9, tz=UTC)
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
    logger.info(f"{lnd_client.icon} invoices_loop task started")

    floors = get_lnd_index_floors(lnd_client.connection.name)
    try:
        add_index, settle_index = await asyncio.wait_for(
            get_invoice_subscription_cursors(), timeout=30
        )
    except TimeoutError:
        logger.warning(
            "Timed out querying DB for invoice cursors in invoices_loop (30s). "
            "Starting subscription from index floors or 0.",
            extra={"notification": True},
        )
        add_index, settle_index = 0, 0

    if add_index == 0 and settle_index == 0 and not floors.has_any:
        # Don’t block shutdown for 10s if the DB is empty and no floors
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=10)
            return
        except TimeoutError:
            pass

    add_index, settle_index = apply_invoice_index_floors(add_index, settle_index, floors)
    logger.info(
        f"{lnd_client.icon} SubscribeInvoices cursors: "
        f"add_index>{add_index} settle_index>{settle_index} "
        f"(config floors add={floors.add_index} settle={floors.settle_index})",
        extra={"notification": False},
    )

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
            orig = (
                e.original_error
                if isinstance(getattr(e, "original_error", None), AioRpcError)
                else None
            )
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeInvoices", original_error=orig
            )
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in invoices_loop", exc_info=e, stack_info=True)
            raise e
        except KeyboardInterrupt as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except asyncio.CancelledError as e:
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeInvoices", cancelled=e
            )
        except Exception as e:
            logger.exception(e)


async def payments_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    logger.info(f"{lnd_client.icon} payments_loop task started")
    floors = get_lnd_index_floors(lnd_client.connection.name)
    if floors.payment_index > 0:
        logger.info(
            f"{lnd_client.icon} Payment store floor: payment_index>{floors.payment_index} "
            f"(TrackPayments is live-only; pre-floor payments are dropped at store/backfill)",
            extra={"notification": False},
        )
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
            orig = (
                e.original_error
                if isinstance(getattr(e, "original_error", None), AioRpcError)
                else None
            )
            await _reconnect_after_stream_drop(
                lnd_client, call_name="TrackPayments", original_error=orig
            )
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in payments_loop", exc_info=e, stack_info=True)
            raise e
        except KeyboardInterrupt as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except asyncio.CancelledError as e:
            await _reconnect_after_stream_drop(lnd_client, call_name="TrackPayments", cancelled=e)
        except Exception as e:
            logger.exception(e)


async def htlc_events_loop(lnd_client: LNDClient, lnd_events_group: LndEventsGroup) -> None:
    logger.info(f"{lnd_client.icon} htlc_events_loop task started")
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
            orig = (
                e.original_error
                if isinstance(getattr(e, "original_error", None), AioRpcError)
                else None
            )
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeHtlcEvents", original_error=orig
            )
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in htlc_events_loop", exc_info=e, stack_info=True)
            raise e
        except KeyboardInterrupt as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except asyncio.CancelledError as e:
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeHtlcEvents", cancelled=e
            )
        except Exception as e:
            logger.exception(e)


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
    logger.info(f"{lnd_client.icon} channel_events_loop task started")
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
                # - FULLY_RESOLVED_CHANNEL
                # - CHANNEL_FUNDING_TIMEOUT
                # - CHANNEL_UPDATE

                decoded_event = MessageToDict(channel_event, preserving_proto_field_name=True)
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
                else:
                    event_type = decoded_event.get("type", "Unknown")
                    logger.info(
                        f"{lnd_client.icon} Other (fully resolved, funding timeout, update) channel event received {event_type}",
                        extra={"channel_event": decoded_event},
                    )

        except LNDSubscriptionError as e:
            orig = (
                e.original_error
                if isinstance(getattr(e, "original_error", None), AioRpcError)
                else None
            )
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeChannelEvents", original_error=orig
            )
        except LNDConnectionError as e:
            # Raised after the max number of retries is reached.
            logger.error("🔴 Connection error in channel_events_loop", exc_info=e, stack_info=True)
            raise e
        except KeyboardInterrupt as e:
            logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
            return
        except asyncio.CancelledError as e:
            await _reconnect_after_stream_drop(
                lnd_client, call_name="SubscribeChannelEvents", cancelled=e
            )
        except Exception as e:
            logger.exception(e)


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
        open_channels = channels_dict.get("channels", [])
        open_ids = [int(ch["chan_id"]) for ch in open_channels if ch.get("chan_id") is not None]

        # Skip only when every open channel already has a *resolved* name.
        # Do not short-circuit on length alone: placeholders named "Unknown"
        # (or a failed GetInfo path that left the map incomplete) must retry.
        def _name_resolved(chan_id: int) -> bool:
            entry = lnd_events_group.channel_names.get(chan_id)
            return entry is not None and entry.name and entry.name != "Unknown"

        if open_ids and all(_name_resolved(cid) for cid in open_ids):
            logger.debug("No new channels to fill")
            return

        # Resolve identity once (avoid N× GetInfo when LND is busy/unreachable).
        get_info = getattr(lnd_client, "get_info", None)
        if get_info is None:
            try:
                get_info = await lnd_client.node_get_info
            except Exception as e:
                logger.warning(
                    f"{lnd_client.icon} fill_channel_names: GetInfo unavailable ({e}); "
                    f"will retry aliases on next fill (not caching Unknown)",
                    extra={"notification": False},
                )
                return
        own_pub_key = getattr(get_info, "identity_pubkey", None) or None
        if not own_pub_key:
            logger.warning(
                f"{lnd_client.icon} fill_channel_names: empty identity_pubkey; "
                f"will retry on next fill",
                extra={"notification": False},
            )
            return

        # Only resolve channels still missing a real name (new opens or prior Unknown).
        to_resolve = [cid for cid in open_ids if not _name_resolved(cid)]
        tasks = [
            get_channel_name(
                channel_id=cid,
                lnd_client=lnd_client,
                own_pub_key=own_pub_key,
            )
            for cid in to_resolve
        ]
        names_list: list[LndChannelName] = await asyncio.gather(*tasks)
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
    Sync invoices from LND into Mongo.

    - Inserts docs missing from Mongo.
    - Updates Mongo OPEN → SETTLED when LND already shows settled (cutover /
      missed SubscribeInvoices settle catch-up).
    - Does not regress SETTLED docs back to OPEN.

    Floor-aware fetch:
    - With ``start_add_index`` > 0: list **forward** from that index so LND
      does not return pre-floor history (filter on fetch).
    - Without a floor: reverse list from newest (full history), with an
      early stop if a floor is later applied mid-walk.
    Store path still skips pre-floor indexes as defense in depth.
    """
    try:
        num_max_invoices = 1000
        total_fetched = 0
        total_skipped_floor = 0
        total_settle_refresh = 0
        floors = get_lnd_index_floors(lnd_client.connection.name)
        floor = floors.add_index
        # Forward from floor when set: LND returns add_index > index_offset.
        # Reverse full history only when no floor (bootstrap / no cutover).
        reversed_list = floor <= 0
        index_offset = 0 if reversed_list else floor
        if floor > 0:
            logger.info(
                f"{lnd_client.icon} Reading invoices after add_index>{floor} "
                f"(floor-aware ListInvoices, forward)..."
            )
        else:
            logger.info(f"{lnd_client.icon} Reading all invoices (no add_index floor; reverse)...")
        while True:
            if shutdown_event.is_set():
                return
            request = lnrpc.ListInvoiceRequest(
                pending_only=False,
                index_offset=index_offset,
                num_max_invoices=num_max_invoices,
                reversed=reversed_list,
            )
            invoices_raw: lnrpc.ListInvoiceResponse = await lnd_client.call(
                lnd_client.lightning_stub.ListInvoices,
                request,
            )
            list_invoices = ListInvoiceResponse(invoices_raw)
            batch = list_invoices.invoices
            if not batch:
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Finished reading invoices (empty page); "
                    f"fetched={total_fetched} skipped_floor={total_skipped_floor} "
                    f"settle_refresh={total_settle_refresh}"
                )
                break

            bulk_updates = []
            for invoice in batch:
                if should_ignore_invoice_index(invoice.add_index, floors):
                    total_skipped_floor += 1
                    continue
                read_invoice = await Invoice.collection().find_one(
                    filter={"r_hash": invoice.r_hash},
                )
                if read_invoice:
                    mongo_settled = read_invoice.get("state") == InvoiceState.SETTLED or bool(
                        read_invoice.get("settled")
                    )
                    # Already fully settled in Mongo — leave alone.
                    if mongo_settled:
                        continue
                    # Still open on LND — nothing to refresh.
                    if invoice.state != InvoiceState.SETTLED:
                        continue
                    # Mongo OPEN, LND SETTLED → push settle fields into Mongo.
                    total_settle_refresh += 1
                invoice.node_name = lnd_client.connection.name
                insert_one = invoice.model_dump(
                    exclude_none=True, exclude_unset=True, exclude={"conv"}
                )
                bulk_updates.append(
                    {
                        "filter": {"r_hash": invoice.r_hash},
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
                batch_min = min(int(i.add_index or 0) for i in batch)
                batch_max = max(int(i.add_index or 0) for i in batch)
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Invoices add_index {batch_min}..{batch_max} "
                    f"(offset→{index_offset}) "
                    f"modified: {modified} inserted: {inserted} "
                    f"skipped_floor: {total_skipped_floor} "
                    f"settle_refresh: {total_settle_refresh}"
                )
                total_fetched += len(batch)
            except BulkWriteError as e:
                logger.debug(e.details)
            except Exception as e:
                logger.exception(str(e), extra={"error": e})
                break

            # Pagination: reverse walks newest→oldest via first_index_offset;
            # forward walks floor→newest via last_index_offset.
            if reversed_list:
                index_offset = int(list_invoices.first_index_offset)
                # Early stop: entire page is at/below floor (no more post-floor data).
                if floor > 0:
                    batch_max = max(int(i.add_index or 0) for i in batch)
                    if batch_max <= floor:
                        logger.info(
                            f"{lnd_client.icon} {DATABASE_ICON} "
                            f"Stopped invoice reverse sync at floor "
                            f"(batch max add_index={batch_max} <= {floor}); "
                            f"fetched={total_fetched} skipped_floor={total_skipped_floor} "
                            f"settle_refresh={total_settle_refresh}"
                        )
                        break
            else:
                index_offset = int(list_invoices.last_index_offset)

            if len(batch) < num_max_invoices:
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Finished reading invoices; "
                    f"fetched={total_fetched} skipped_floor={total_skipped_floor} "
                    f"settle_refresh={total_settle_refresh}"
                )
                break
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"Keyboard interrupt or Cancelled: {__name__} {e}")
        raise e


async def read_all_payments(lnd_client: LNDClient) -> None:
    """
    Sync payments from LND into Mongo (fill missing route/description).

    Floor-aware fetch:
    - With ``start_payment_index`` > 0: list **forward** from that index so LND
      does not return pre-floor history.
    - Without a floor: reverse full history, with early stop if a floor is set.
    Store path still skips pre-floor indexes as defense in depth.
    """
    try:
        num_max_payments = 1000
        total_fetched = 0
        total_skipped_floor = 0
        floors = get_lnd_index_floors(lnd_client.connection.name)
        floor = floors.payment_index
        reversed_list = floor <= 0
        index_offset = 0 if reversed_list else floor
        if floor > 0:
            logger.info(
                f"{lnd_client.icon} Reading payments after payment_index>{floor} "
                f"(floor-aware ListPayments, forward)..."
            )
        else:
            logger.info(
                f"{lnd_client.icon} Reading all payments (no payment_index floor; reverse)..."
            )
        while True:
            if shutdown_event.is_set():
                return
            request = lnrpc.ListPaymentsRequest(
                include_incomplete=True,
                index_offset=index_offset,
                max_payments=num_max_payments,
                reversed=reversed_list,
            )
            payments_raw: lnrpc.ListPaymentsResponse = await lnd_client.call(
                lnd_client.lightning_stub.ListPayments,
                request,
            )
            list_payments = ListPaymentsResponse(payments_raw)
            batch = list_payments.payments
            if not batch:
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Finished reading payments (empty page); "
                    f"fetched={total_fetched} skipped_floor={total_skipped_floor}"
                )
                break

            bulk_updates = []
            for payment in batch:
                if should_ignore_payment_index(payment.payment_index, floors):
                    total_skipped_floor += 1
                    continue
                query = {"payment_hash": payment.payment_hash}
                read_payment = await Payment.collection().find_one(
                    filter=query,
                )
                # The invoice_description "Not set" is used in pub_key_alias.py if there is no description.
                route_str = read_payment.get("route_str", None) if read_payment else None
                invoice_description = (
                    read_payment.get("invoice_description", None) if read_payment else None
                )
                status = read_payment.get("status", None) if read_payment else None
                if read_payment and route_str and invoice_description:
                    continue
                if (
                    read_payment
                    and route_str
                    and (not invoice_description or invoice_description == "Not set")
                ):
                    continue
                logger.info(
                    f"Updating payment {payment.payment_index} {route_str} "
                    f"{invoice_description} {payment.payment_hash} {status}"
                )
                await update_payment_route_with_alias(
                    lnd_client=lnd_client,
                    payment=payment,
                    fill_cache=True,
                    col_pub_keys="pub_keys",
                )
                await decode_payment_request_and_attach(lnd_client=lnd_client, payment=payment)

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
                batch_min = min(int(p.payment_index or 0) for p in batch)
                batch_max = max(int(p.payment_index or 0) for p in batch)
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Payments payment_index {batch_min}..{batch_max} "
                    f"(offset→{index_offset}) "
                    f"modified: {modified} inserted: {inserted} "
                    f"skipped_floor: {total_skipped_floor}"
                )
                total_fetched += len(batch)
            except BulkWriteError as e:
                logger.debug(e.details)
            except Exception as e:
                logger.exception(str(e), extra={"error": e})

            if reversed_list:
                index_offset = int(payments_raw.first_index_offset)
                if floor > 0:
                    batch_max = max(int(p.payment_index or 0) for p in batch)
                    if batch_max <= floor:
                        logger.info(
                            f"{lnd_client.icon} {DATABASE_ICON} "
                            f"Stopped payment reverse sync at floor "
                            f"(batch max payment_index={batch_max} <= {floor}); "
                            f"fetched={total_fetched} skipped_floor={total_skipped_floor}"
                        )
                        break
            else:
                index_offset = int(payments_raw.last_index_offset)

            if len(batch) < num_max_payments:
                logger.info(
                    f"{lnd_client.icon} {DATABASE_ICON} "
                    f"Finished reading payments; "
                    f"fetched={total_fetched} skipped_floor={total_skipped_floor}"
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
    Fetches the invoice with the highest ``add_index`` from MongoDB.

    Note: that document is often still OPEN (``settle_index=0``). For
    SubscribeInvoices cursors use :func:`get_invoice_subscription_cursors`
    so the settle cursor is the max settle_index across all invoices.
    """
    query = {}
    sort = [("add_index", -1)]
    collection = Invoice.collection()
    logger.debug(f"{DATABASE_ICON} get_most_recent_invoice: querying invoices collection...")
    cursor = collection.find(query).sort(sort).limit(1)
    invoice = None
    try:
        async for ans in cursor:
            logger.debug(f"{DATABASE_ICON} get_most_recent_invoice: got result from cursor")
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


async def get_invoice_subscription_cursors() -> tuple[int, int]:
    """
    Return (max_add_index, max_settle_index) from the invoices collection.

    LND SubscribeInvoices uses two independent cursors. Using the settle_index
    from the highest-add_index document alone is wrong: that row is often OPEN
    with settle_index=0, which would re-stream every prior settle after restart.
    """
    collection = Invoice.collection()
    add_index = 0
    settle_index = 0
    try:
        top_add = await collection.find_one(filter={}, sort=[("add_index", -1)])
        if top_add:
            add_index = int(top_add.get("add_index") or 0)
        top_settle = await collection.find_one(
            filter={"settle_index": {"$gt": 0}},
            sort=[("settle_index", -1)],
        )
        if top_settle:
            settle_index = int(top_settle.get("settle_index") or 0)
        logger.info(
            f"{DATABASE_ICON} Invoice subscription DB cursors: "
            f"add_index={add_index} settle_index={settle_index}"
        )
    except Exception as e:
        logger.warning(f"{DATABASE_ICON} Failed to load invoice cursors: {e}")
    return add_index, settle_index


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
    logger.debug(f"{DATABASE_ICON} get_most_recent_payment: querying payments collection...")
    cursor = collection.find(query).sort(sort).limit(1)
    payment = None
    try:
        async for ans in cursor:
            logger.debug(f"{DATABASE_ICON} get_most_recent_payment: got result from cursor")
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
    except TimeoutError:
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
    # CRITICAL: Update notification_loop to the actual running event loop.
    # setup_logging() runs before asyncio.run() so it creates a detached
    # event loop (new_event_loop) that is never started.  When the
    # QueueListener thread later processes a notification record it calls
    # loop.run_until_complete() which BLOCKS the listener thread, preventing
    # all subsequent log records (including file writes) from being flushed.
    # By pointing notification_loop at the running loop, the handler uses
    # run_coroutine_threadsafe() instead, which is non-blocking.
    InternalConfig.notification_loop = asyncio.get_running_loop()

    process_name = os.path.splitext(os.path.basename(__file__))[0]
    health_check_port = os.environ.get("HEALTH_CHECK_PORT", "6001")
    status_api = StatusAPI(
        port=int(health_check_port),
        health_check_func=health_check,
        shutdown_event=shutdown_event,
        process_name=process_name,
        version=__version__,
    )  # Use a port from config if needed

    global _active_lnd_client
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
            _active_lnd_client = lnd_client
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
                asyncio.create_task(
                    htlc_events_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="htlc_events_loop",
                ),
                asyncio.create_task(
                    channel_events_loop(lnd_client=lnd_client, lnd_events_group=lnd_events_group),
                    name="channel_events_loop",
                ),
                # Schedule database sync as a non-blocking background task
                asyncio.create_task(
                    _background_sync(lnd_client),
                    name="synchronize_db",
                ),
                asyncio.create_task(
                    expired_invoices_maintenance_loop(lnd_client=lnd_client),
                    name="expired_invoices_maintenance",
                ),
                asyncio.create_task(status_api.start(), name="status_api"),
            ]
            critical_tasks = [
                t
                for t in running_tasks
                if t.get_name()
                in {"invoices_loop", "payments_loop", "htlc_events_loop", "channel_events_loop"}
            ]
            running_tasks.append(
                asyncio.create_task(
                    _task_watchdog(critical_tasks, shutdown_event, lnd_client),
                    name="task_watchdog",
                )
            )
            lnd_node = InternalConfig().config.lnd_config.default
            icon = InternalConfig().config.lnd_config.connections[lnd_node].icon
            logger.info(
                f"{icon}{Fore.WHITE}✅ LND gRPC client started. "
                f"Monitoring node: {lnd_node} {icon}. Version: {__version__} on {InternalConfig().local_machine_name}{Style.RESET_ALL}",
                extra={"notification": True},
            )
            startup_complete_event.set()
            print(
                f"[DIAG] {len(running_tasks)} tasks created, (outside logging process) "
                f"shutdown_event.is_set()={shutdown_event.is_set()}, "
                f"awaiting shutdown_event.wait()...",
                flush=True,
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
            except TimeoutError:
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


async def _task_watchdog(
    critical_tasks: list[asyncio.Task],
    shutdown_event: asyncio.Event,
    lnd_client: LNDClient,
    poll_interval: float = 10.0,
) -> None:
    """Watch critical tasks and trigger a non-zero exit if any die unexpectedly.

    When a critical streaming task dies outside of a normal shutdown, the main
    process stays alive (blocked on shutdown_event.wait) and Docker never sees
    a failure exit code — so 'restart: on-failure' never fires.  This watchdog
    detects that situation, logs the dead tasks, then asks main() to exit 1.
    ``sys.exit`` inside an asyncio Task does not terminate the process.

    Also exits if TrackPayments or SubscribeInvoices is hung (LND ahead of Mongo)
    for SUBSCRIPTION_LAG_POLLS_BEFORE_EXIT consecutive polls.
    """
    consecutive_subscription_lag = 0
    while not shutdown_event.is_set():
        await asyncio.sleep(poll_interval)
        if shutdown_event.is_set():
            break
        dead = [t for t in critical_tasks if t.done()]
        if dead:
            for t in dead:
                exc = t.exception() if not t.cancelled() else None
                logger.error(
                    f"Critical task '{t.get_name()}' died unexpectedly "
                    f"(exception: {exc}). Triggering restart.",
                    extra={"notification": True, "error_code": "hive_monitor_task_failure"},
                )
            _request_failed_restart()
            # Brief pause so the logger can flush the error before we exit
            await asyncio.sleep(2)
            return

        if startup_complete_event.is_set() and not _task_still_running("synchronize_db"):
            lags = await _subscription_lag_messages(lnd_client)
            if lags:
                consecutive_subscription_lag += 1
                lag = "; ".join(lags)
                logger.warning(
                    f"{ICON} {lag} ({consecutive_subscription_lag}/"
                    f"{SUBSCRIPTION_LAG_POLLS_BEFORE_EXIT})",
                    extra={"notification": False, "error_code": "lnd_subscription_lag"},
                )
                if consecutive_subscription_lag >= SUBSCRIPTION_LAG_POLLS_BEFORE_EXIT:
                    logger.error(
                        f"{ICON} {lag}. Triggering restart.",
                        extra={
                            "notification": True,
                            "error_code": "lnd_subscription_lag",
                        },
                    )
                    _request_failed_restart()
                    await asyncio.sleep(2)
                    return
            else:
                consecutive_subscription_lag = 0


async def _background_sync(lnd_client: LNDClient) -> None:
    """Run database synchronization in the background without blocking startup.

    Determines whether an immediate sync is needed (stale DB) and runs it with
    the appropriate delay. This replaces the previous blocking sync that
    prevented subscription loops from starting.
    """
    logger.info(f"{lnd_client.icon} _background_sync task started")
    try:
        pause_for_sync = await pause_for_database_sync()
        delay = 0 if pause_for_sync else 10
        await synchronize_db(lnd_client, delay=delay)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Background sync cancelled during shutdown.")
    except Exception as e:
        logger.warning(
            f"Background sync failed: {e}",
            extra={"notification": False},
        )


EXPIRED_INVOICE_PRUNE_INTERVAL_SECONDS = 3600  # 1 hour


async def expired_invoices_maintenance_loop(
    lnd_client: LNDClient,
    interval_seconds: int = EXPIRED_INVOICE_PRUNE_INTERVAL_SECONDS,
) -> None:
    """
    Periodically delete expired, unsettled invoices past the retention window.

    Deletes expired invoices. Runs once at start, then every ``interval_seconds``.
    """
    logger.info(
        f"{lnd_client.icon} expired_invoices_maintenance_loop started "
        f"(interval={interval_seconds}s)"
    )
    while not shutdown_event.is_set():
        try:
            count = await delete_expired_unsettled_invoices(
                collection=Invoice.collection(), retain_after_expiry=TIMEDELTA_RETAIN_AFTER_EXPIRY
            )
            if count > 0:
                logger.info(
                    f"{lnd_client.icon}{DATABASE_ICON} Expired unsettled invoices "
                    f"eligible for prune: {count}",
                    extra={
                        "notification": False,
                        "expired_unsettled_invoice_count": count,
                    },
                )
            else:
                logger.debug(
                    f"{lnd_client.icon}{DATABASE_ICON} No expired unsettled invoices found for prune",
                    extra={"notification": False},
                )
        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info(f"{lnd_client.icon} expired_invoices_maintenance_loop cancelled")
            return
        except Exception as e:
            logger.warning(
                f"{lnd_client.icon} expired invoice maintenance find failed: {e}",
                extra={"notification": False},
            )

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=interval_seconds)
            return
        except TimeoutError:
            pass


async def pause_for_database_sync() -> bool:
    try:
        recent_invoice = await asyncio.wait_for(get_most_recent_invoice(), timeout=30)
        recent_payment = await asyncio.wait_for(get_most_recent_payment(), timeout=30)
    except TimeoutError:
        logger.warning(
            "Timed out querying DB for recent invoice/payment during startup. "
            "Skipping database sync check.",
            extra={"notification": False},
        )
        return False
    if (
        recent_invoice
        and recent_payment
        and recent_invoice.creation_date
        and recent_payment.creation_date
    ):
        invoice_time_delta = datetime.now(tz=UTC) - recent_invoice.creation_date
        payment_time_delta = datetime.now(tz=UTC) - recent_payment.creation_date
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
        f"{icon}✅ LND gRPC client Started.  Version: {__version__}"
        f"Monitoring node: {lnd_node} {icon} on {InternalConfig().local_machine_name}",
        extra={"notification": False},
    )
    asyncio.run(main_async_start(lnd_node))
    logger.info("👋 Goodbye!")
    if _failed_restart_requested:
        sys.exit(1)


if __name__ == "__main__":
    try:
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    # TODO: change this on all the other monitors
    except StartupFailure as e:
        print(f"{ICON} Startup failure: {e}")
        sys.exit(0)

    except Exception as e:
        logger.error("🔴 Unhandled exception in lnd_monitor_v2", exc_info=e, stack_info=True)
        logger.exception(e, extra={"error": e, "notification": True})
        print(e)
        sys.exit(1)
