import asyncio
import contextvars

import backoff
import grpc
from grpc.aio import AioRpcError

from v4vapp_backend_v2.config import logger, setup_logging
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.connect import (
    LNDConnectionError,
    connect_to_lnd,
    most_recent_invoice,
    subscribe_invoices,
    wallet_balance,
)
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

setup_logging()

# Create a temporary file
db = MyDB()


# Create a context variable for the flag
error_state = contextvars.ContextVar("flag", default=False)
error_code = contextvars.ContextVar("code", default=None)


@backoff.on_exception(
    lambda: backoff.expo(base=2, factor=1),
    (LNDConnectionError),
    max_tries=20,
    logger=logger,
)
async def subscribe_invoices_with_backoff():
    # check if temp_file exists
    global error_state, error_details
    most_recent = db.LND.most_recent
    most_recent_settled = db.LND.most_recent_settled
    while True:
        if not error_state.get() and error_code.get():
            logger.info("✅ Connection to LND server is OK", extra={"telegram": True})
            error_code.set(None)
        try:
            async for invoice in subscribe_invoices(
                add_index=most_recent.add_index,
                settle_index=most_recent_settled.settle_index,
            ):
                if invoice.settled:
                    logger.info(
                        f"✅ Settled invoice {invoice.add_index} with memo "
                        f"{invoice.memo} {invoice.value} sats",
                        extra={"telegram": True},
                    )
                    logger.info(f"{invoice.settle_date}")
                    most_recent = invoice
                else:
                    logger.info(
                        f"✅ Valid invoice {invoice.add_index} with memo "
                        f"{invoice.memo} {invoice.value} sats",
                        extra={"telegram": True},
                    )
                    most_recent = invoice
                    db.update_most_recent(invoice)
            error_state.set(False)
        except AioRpcError as ex:
            details = ""
            if isinstance(ex, AioRpcError):
                details = ex.details()
                send_telegram = not error_state.get()
                if error_code.get() and not error_code.get() == ex.code():
                    send_telegram = True
                logger.error(
                    f"Lost connection: {details}",
                    extra={"telegram": send_telegram, "details": details},
                )
            error_state.set(True)
            error_code.set(ex.code())
            raise LNDConnectionError(f"Error connecting to LND: {details}")
        except Exception as e:
            logger.error(f"Error: {e}")
            raise e


@backoff.on_exception(
    lambda: backoff.expo(base=1),
    (LNDConnectionError),
    max_tries=20,
    logger=logger,
)
async def heartbeat_check_connection():
    logger.info("❤️ Heartbeat check connection")
    while True:
        try:
            response = await wallet_balance()
            logger.debug(f"Wallet balance: {response.total_balance}")
            logger.info("✅ Connection to LND server is OK")
            await asyncio.sleep(60)
        except grpc.RpcError as e:
            logger.error(f"Lost connection to server: {e}")
            raise e
        except LNDConnectionError as e:
            logger.error(f"Error connecting to LND: {e}")
            raise e


async def get_startup_invoices():
    """
    Retrieves the most recent invoice and most recent settled invoice from the database.
    If the most recent invoice is not available in the database, it connects to the LND server,
    retrieves the most recent invoice and most recent settled invoice, and updates the database.
    Returns the most recent invoice and most recent settled invoice.

    Raises:
        - LNDConnectionError: If there is an error connecting to the LND server.
        - Exception: If there is a failure during startup.
    """
    try:
        most_recent = db.LND.most_recent
        most_recent_settled = db.LND.most_recent_settled
        if not most_recent.add_index:
            stub = await connect_to_lnd()
            most_recent, most_recent_settled = await most_recent_invoice(stub)
            db.update_most_recent(most_recent)
            db.update_most_recent(most_recent_settled)
        logger.info(f"Most recent invoice: {most_recent.add_index}")
        logger.info(f"Most recent settled invoice: {most_recent_settled.settle_index}")
        return most_recent, most_recent_settled

    except AioRpcError as ex:
        details = ""
        if isinstance(ex, AioRpcError):
            details = ex.details()
            logger.error(ex)
            logger.error(
                f"Failure during subscription startup: {details}",
                extra={"telegram": True, "details": details},
            )
        await asyncio.sleep(5)
        raise LNDConnectionError(f"Error connecting to LND: {details}")

    except Exception as e:
        logger.error("Failure during startup")
        raise e


async def main():
    logger.info("Starting LND gRPC client")
    try:
        await get_startup_invoices()
    except LNDConnectionError:
        logger.info("Error during startup. Exiting.")
        # exit this function
        return
    while True:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(subscribe_invoices_with_backoff())
                # tg.create_task(heartbeat_check_connection())
        except ExceptionGroup as e:
            for ex in e.exceptions:
                if isinstance(ex, AioRpcError):
                    logger.error(f"Lost connection to server: {ex}")
                    await asyncio.sleep(5)
                else:
                    logger.error(f"Error: {ex}")
                    raise ex
        except KeyboardInterrupt:
            logger.warning("❌ Keyboard interrupt LND gRPC client stopped")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            continue

    logger.info("❌ LND gRPC client stopped")


if __name__ == "__main__":

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e
