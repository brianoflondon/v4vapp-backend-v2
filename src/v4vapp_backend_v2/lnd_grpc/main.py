import asyncio

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

# Create a temporary file
db = MyDB()


async def subscribe_invoices_with_backoff():
    # check if temp_file exists
    try:
        most_recent = db.LND.most_recent
        most_recent_settled = db.LND.most_recent_settled
        if not most_recent.add_index:
            stub = await connect_to_lnd()
            most_recent, most_recent_settled = await most_recent_invoice(stub)
        logger.info(f"Most recent invoice: {most_recent.add_index}")
    except Exception as e:
        logger.error("Failure during startup")
        raise e
    while True:
        try:
            async for invoice in subscribe_invoices(
                add_index=most_recent.add_index,
                settle_index=most_recent_settled.settle_index,
            ):
                if invoice.settled:
                    logger.info(
                        f"✅ Settled invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}"
                    )
                    logger.info(f"{invoice.settle_date}")
                    most_recent = invoice
                else:
                    logger.info(
                        f"✅ Valid invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}",
                        extra={"telegram": True},
                    )
                    most_recent = invoice
                    db.update_most_recent(invoice)
        except grpc.RpcError as e:
            logger.error(f"Lost connection to server: {e}")
            raise e
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


async def main():
    logger.info("Starting LND gRPC client")
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

    setup_logging()

    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e
