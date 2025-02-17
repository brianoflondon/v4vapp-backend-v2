import asyncio
import sys
from typing import Annotated, Optional
import typer

from v4vapp_backend_v2.lnd_grpc.lnd_functions import get_node_info
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from lnd_monitor_v2 import CONFIG, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.models.payment_models import Payment

app = typer.Typer()


async def run(node: str, database: str):
    """
    Main function to run the LND gRPC client.
    Args:
        node (str): The node to monitor.
        database (str): The database to update.

    Returns:
        None
    """

    all_pub_keys = set()
    all_aliases = {}
    async with MongoDBClient(
        db_conn=CONFIG.default_database_connection, db_name=database, db_user="default"
    ) as db_client:
        async with LNDClient(node) as lnd_client:
            cursor = await db_client.find("payments", {})
            async for document in cursor:
                payment = Payment.model_validate(document)
                # unpack a tuple of the payment model destination pub_key
                pub_keys = payment.destination_pub_keys
                if not pub_keys:
                    continue
                all_pub_keys.update(pub_keys)
                payment_aliases = {}
                for pub_key in pub_keys:
                    if pub_key not in all_aliases:
                        node_info = await get_node_info(pub_key, lnd_client)
                        if node_info.node.alias:
                            all_aliases[pub_key] = node_info.node.alias
                        else:
                            all_aliases[pub_key] = "Unknown"
                    payment_aliases[pub_key] = all_aliases[pub_key]
                reversed_aliases = " -> ".join(
                    reversed([alias for alias in payment_aliases.values()])
                )
                print(f"{reversed_aliases}\n")
                print("-------------")

    for pub_key, alias in all_aliases.items():
        print(pub_key, alias)

    # async with MongoDBClient(
    #     db_conn=CONFIG.default_database_connection, db_name=database, db_user="default"
    # ) as db_client:
    #     for pub_key, alias in all_aliases.items():
    #         await db_client.update_one(
    #             "payments", {"destination_pub_keys": pub_key}, {"$set": {"destination_alias": alias}}
    #         )


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
        f"{icon} ✅ LND gRPC client started. Monitoring node: {node} {icon}. Version: {CONFIG.version}"
    )
    logger.info(f"{icon} ✅ Database: {database}")
    asyncio.run(run(node, database))
    print("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "name_goes_here"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
