from datetime import datetime, timezone
import signal
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

config = InternalConfig().config

app = typer.Typer()


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
            async for inv in client.call_async_generator(
                client.lightning_stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                inv: lnrpc.Invoice
                logger.info(
                    f"{client.icon} Invoice: {inv.add_index} amount: {inv.value} sat {inv.settle_index}",
                    extra={
                        "invoice": MessageToDict(inv, preserving_proto_field_name=True)
                    },
                )
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
                status = lnrpc.Payment.PaymentStatus.Name(payment.status)
                creation_date = datetime.fromtimestamp(
                    payment.creation_time_ns / 1e9, tz=timezone.utc
                )
                in_flight_time = datetime.now(tz=timezone.utc) - creation_date
                logger.info(
                    (
                        f"{client.icon} Payment: {payment.payment_index} "
                        f"amount: {payment.value_sat:,} sat "
                        f"in flight time: {in_flight_time} "
                        f"created: {creation_date} status: {status}"
                    ),
                    extra={
                        "payment": MessageToDict(
                            payment, preserving_proto_field_name=True
                        )
                    },
                )
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
        tasks = [
            invoices_loop(client),
            payments_loop(client),
        ]
        try:
            await asyncio.gather(*tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            print("👋 Received signal to stop. Exiting...")
            await client.channel.close()
            InternalConfig().__exit__(None, None, None)
            # sys.exit(0)


def signal_handler(signal, frame):
    logger.info("👋 Received signal to stop. Exiting...")
    sys.exit(0)


@app.command()
def main(
    node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The node to monitor. If not provided, defaults to the value: "
                f"{config.default_connection}.\n"
                f"Choose from: {config.connection_names}"
            )
        ),
    ] = config.default_connection
):
    f"""
    Main function to run the node monitor.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        {config.connection_names}

    Returns:
        None
    """
    icon = config.icon(node)
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: {node} {icon}. Version: {config.version}"
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
