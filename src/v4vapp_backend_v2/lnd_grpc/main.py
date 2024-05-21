import asyncio
import json
import posixpath
import tempfile

import backoff
import grpc
from grpc.aio import AioRpcError
from pydantic import ValidationError

from v4vapp_backend_v2.config import logger, setup_logging
from v4vapp_backend_v2.lnd_grpc.connect import (
    connect_to_lnd,
    most_recent_invoice,
    subscribe_invoices,
    wallet_balance,
)
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

# Create a temporary file
TEMP_FILE = posixpath.join(tempfile.gettempdir(), "add_index.json")


@backoff.on_exception(
    lambda: backoff.expo(base=10),
    (ValidationError, AioRpcError, grpc.RpcError),
    max_tries=20,
    logger=logger,
)
async def subscribe_invoices_with_backoff():
    stub = await connect_to_lnd()
    # check if temp_file exists
    try:
        with open(TEMP_FILE, "r") as f:
            invoice_json = json.load(f)
            most_recent = LNDInvoice.model_validate(json.loads(invoice_json))
    except FileNotFoundError:
        most_recent = await most_recent_invoice(stub)
    logger.info(f"Most recent invoice: {most_recent.add_index}")
    while True:
        try:
            async for invoice in subscribe_invoices(add_index=most_recent.add_index):
                if invoice.settled:
                    logger.info(
                        f"✅ Settled invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}"
                    )
                    logger.info(f"{invoice.settle_date}")
                    most_recent = invoice
                else:
                    logger.info(
                        f"✅ Valid invoice {invoice.add_index} with memo {invoice.memo} and value {invoice.value}"
                    )
                    most_recent = invoice
                with open(TEMP_FILE, "w") as f:
                    json.dump(invoice.model_dump_json(indent=2), f)
        except grpc.RpcError as e:
            logger.error(f"Lost connection to server: {e}")
            raise e

async def heartbeat_check_connection():
    while True:
        try:
            logger.info("❤️ Heartbeat check connection")
            response = await wallet_balance()
            logger.info("✅ Connection to LND server is OK")
            await asyncio.sleep(60)
        except grpc.RpcError as e:
            logger.error(f"Lost connection to server: {e}")
            raise e


async def main():
    logger.info("Starting LND gRPC client")
    while True:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(subscribe_invoices_with_backoff())
                tg.create_task(heartbeat_check_connection())
        except AioRpcError as e:
            logger.error(f"Lost connection to server: {e}")
            await asyncio.sleep(5)
            continue
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
