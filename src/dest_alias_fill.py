import asyncio
import sys
from typing import Annotated, Optional

import typer
from bson import ObjectId

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import InternalConfig, async_time_stats_decorator, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.pub_key_alias import update_payment_route_with_alias
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.payment_models import Payment

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
app = typer.Typer()


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
        db_conn=CONFIG.default_db_connection, db_name=database, db_user="default"
    ) as db_client:
        async with LNDClient(node) as lnd_client:
            cursor = await db_client.find("payments", {})
            task_blocks = [[]]
            task_block_limit = 1000
            count = 0
            async for document in cursor:
                try:
                    payment = Payment.model_validate(document)
                except Exception as e:
                    logger.error(f"Error validating payment: {e}")
                    pass
                tasks = []
                tasks.append(
                    update_payment_route_with_alias(
                        db_client=db_client,
                        lnd_client=lnd_client,
                        payment=payment,
                        fill_cache=True,
                        col_pub_keys="pub_keys",
                        force_update=False,
                    )
                )
                await asyncio.gather(*tasks)

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
                payments_changed += 1
            ans = []
            for tasks in task_blocks:
                ans = await asyncio.gather(*tasks)

            logger.info(f"Payments Changed: {payments_changed} Updated {len(ans)} payments.")


@app.command()
def main(
    database: Annotated[
        str,
        typer.Argument(help=(f"The database to monitor.Choose from: {CONFIG.dbs_names}")),
    ],
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
):
    f"""
    Main function to do what you want.
    Args:
        node (Annotated[Optional[str], Argument]): The node to monitor.
        Choose from:
        connections: {CONFIG.lnd_connections_names}
        databases: {CONFIG.dbs_names}

    Returns:
        None
    """
    icon = CONFIG.lnd_connections[lnd_node].icon
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: "
        f"{lnd_node} {icon}. Version: {__version__}"
    )
    logger.info(f"{icon} ✅ Database: {database}")
    asyncio.run(main_worker(lnd_node, database))
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
