import asyncio
import sys
from typing import Annotated, Optional

import typer
from bson import ObjectId

from v4vapp_backend_v2.config.setup import (
    InternalConfig,
    async_time_stats_decorator,
    logger,
)
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.pub_key_alias import update_payment_route_with_alias
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.payment_models import Payment

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
app = typer.Typer()


def get_final_destination(payment_alias: list[str]) -> str:
    if len(payment_alias) == 1:
        return payment_alias[0]
    if payment_alias[-1] == "Unknown":
        if payment_alias[-2] == "magnetron":
            return "Muun User"
        elif payment_alias[-2] == "ACINQ":
            return "Phoenix User"
    return payment_alias[-1]


@async_time_stats_decorator()
async def main_worker(node: str, database: str):
    """
    Main function to run the LND gRPC client.
    Args:
        node (str): The node to monitor.
        database (str): The database to update.

    Returns:
        None
    """
    payments_changed = 0
    async with MongoDBClient(
        db_conn=CONFIG.default_database_connection, db_name=database, db_user="default"
    ) as db_client:
        async with LNDClient(node) as lnd_client:
            cursor = await db_client.find("payments", {})
            task_blocks = [[]]
            task_block_limit = 1000
            count = 0
            async for document in cursor:
                try:
                    payment = Payment.model_validate(document)
                    if payment.route:
                        continue
                    payment.route = []
                except Exception as e:
                    logger.error(f"Error validating payment: {e}")
                    pass
                # unpack a tuple of the payment model destination pub_key
                pub_keys = payment.destination_pub_keys
                if payment.route or not pub_keys:
                    continue

                for pub_key in pub_keys:
                    await update_payment_route_with_alias(
                        db_client=db_client,
                        lnd_client=lnd_client,
                        payment=payment,
                        pub_key=pub_key,
                        fill_cache=True,
                        col_pub_keys="pub_keys",
                    )

                # logger.info(f"{payment.destination}  || {payment.route_str}")
                payment_id = ObjectId(document["_id"])
                block_count = count // task_block_limit
                if len(task_blocks) < block_count + 1:
                    task_blocks.append([])
                task_blocks[block_count].append(
                    db_client.update_one(
                        "payments",
                        query={"_id": payment_id},
                        update=payment.model_dump(
                            exclude_none=True,
                            exclude_unset=True,
                        ),
                        upsert=True,
                    )
                )
                count += 1
                # tasks.append(
                #     db_client.update_one(
                #         "payments",
                #         query={"_id": payment_id},
                #         update=payment.model_dump(
                #             exclude_none=True,
                #             exclude_unset=True,
                #         ),
                #         upsert=True,
                #     )
                # )
                payments_changed += 1
            ans = []
            for tasks in task_blocks:
                ans = await asyncio.gather(*tasks)

            logger.info(
                f"Payments Changed: {payments_changed} Updated {len(ans)} payments."
            )


@app.command()
def main(
    database: Annotated[
        str,
        typer.Argument(
            help=(f"The database to monitor." f"Choose from: {CONFIG.database_names}")
        ),
    ],
    node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The node to monitor. If not provided, defaults to the value: "
                f"{CONFIG.default_connection}.\n"
                f"Choose from: {CONFIG.connection_names}"
            )
        ),
    ] = CONFIG.default_connection,
):
    f"""
    Main function to do what you want.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        connections: {CONFIG.connection_names}
        databases: {CONFIG.database_names}

    Returns:
        None
    """
    icon = CONFIG.icon(node)
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: "
        f"{node} {icon}. Version: {CONFIG.version}"
    )
    logger.info(f"{icon} ✅ Database: {database}")
    asyncio.run(main_worker(node, database))
    logger.info("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "dest_alias_fill"
        app()
    except KeyboardInterrupt:
        logger.info("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
