from datetime import datetime, timezone
import typer
import sys
import asyncio
from typing import Optional, Annotated, List
from google.protobuf.json_format import MessageToDict


from v4vapp_backend_v2.lnd_grpc.lnd_errors import (
    LNDConnectionError,
    LNDSubscriptionError,
)
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient

from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config

app = typer.Typer()


async def invoice_report(invoice: lnrpc.Invoice, client: LNDClient) -> None:
    logger.info(
        f"{client.icon} Invoice: {invoice.add_index} amount: {invoice.value} sat {invoice.settle_index}",
        extra={"invoice": MessageToDict(invoice, preserving_proto_field_name=True)},
    )


async def payment_report(payment: lnrpc.Payment, client: LNDClient) -> None:
    status = lnrpc.Payment.PaymentStatus.Name(payment.status)
    creation_date = datetime.fromtimestamp(
        payment.creation_time_ns / 1e9, tz=timezone.utc
    )
    pre_image = payment.payment_preimage if payment.payment_preimage else None
    in_flight_time = datetime.now(tz=timezone.utc) - creation_date
    logger.info(
        (
            f"{client.icon} Payment: {payment.payment_index} "
            f"amount: {payment.value_sat:,} sat "
            f"pre_image: {pre_image} "
            f"in flight time: {in_flight_time} "
            f"created: {creation_date} status: {status}"
        ),
        extra={"payment": MessageToDict(payment, preserving_proto_field_name=True)},
    )


async def htlc_event_report(htlc_event: routerrpc.HtlcEvent, client: LNDClient) -> None:
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
        (f"{client.icon} htlc:   {htlc_id} {event_type} {preimage}"),
        extra={
            "htlc_event": MessageToDict(htlc_event, preserving_proto_field_name=True)
        },
    )


async def invoices_loop(client: LNDClient) -> None:
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
                async_publish(Events.LND_INVOICE, invoice, client)
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


async def payments_loop(client: LNDClient) -> None:
    request = routerrpc.TrackPaymentRequest(no_inflight_updates=False)
    while True:
        try:
            async for payment in client.call_async_generator(
                client.router_stub.TrackPayments,
                request,
                call_name="TrackPayments",
            ):
                payment: lnrpc.Payment
                async_publish(Events.LND_PAYMENT, payment, client)
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


async def htlc_events_loop(client: LNDClient) -> None:
    request = routerrpc.SubscribeHtlcEventsRequest()
    while True:
        try:
            async for htlc_event in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request,
                call_name="SubscribeHtlcEvents",
            ):
                htlc_event: routerrpc.HtlcEvent
                async_publish(Events.HTLC_EVENT, htlc_event, client)
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


async def run(connection_name: str) -> None:
    """
    Main function to run the node monitor.
    Args:
        connection_name (str): The name of the connection to monitor.

    Returns:
        None
    """
    async with LNDClient(connection_name) as client:
        logger.info(f"{client.icon} 🔍 Monitoring node...")
        if client.get_info:
            logger.info(
                f"{client.icon} Node: {client.get_info.alias} pub_key: {client.get_info.identity_pubkey}"
            )
        async_subscribe(Events.LND_INVOICE, invoice_report)
        async_subscribe(Events.LND_PAYMENT, payment_report)
        async_subscribe(Events.HTLC_EVENT, htlc_event_report)
        tasks = [invoices_loop(client), payments_loop(client), htlc_events_loop(client)]
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
