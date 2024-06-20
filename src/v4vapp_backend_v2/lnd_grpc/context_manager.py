import asyncio
import os

from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.config import logger, setup_logging
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionError
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

setup_logging()

# Create a temporary file
db = MyDB()


async def main():
    logger.info("Starting LND gRPC client")
    try:
        async with LNDClient() as client:
            balance = await client.call(
                client.stub.WalletBalance,
                ln.WalletBalanceRequest(),
            )
            logger.info(f"Balance: {balance.total_balance} sats")
            request_sub = ln.InvoiceSubscription()
            while True:
                logger.info("Subscribing to invoices")
                async for inv in client.call_async_generator(
                    client.stub.SubscribeInvoices,
                    request_sub,
                    call_name="SubscribeInvoices",
                ):
                    inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
                    invoice = LNDInvoice.model_validate(inv_dict)
                    logger.info(f"Received invoice: {invoice.add_index}")
    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped")
    except Exception as e:
        logger.error("❌ LND gRPC client stopped")
        logger.error(e)
        raise e

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
