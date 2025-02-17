import asyncio
import sys
from typing import Annotated, Optional
from bson import ObjectId
import typer

from v4vapp_backend_v2.lnd_grpc.lnd_functions import get_node_info
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from lnd_monitor_v2 import CONFIG, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.models.payment_models import (
    NodeAlias,
    Payment,
)

app = typer.Typer()


async def get_all_pub_key_aliases(database: str) -> dict[str, str]:
    """
    Get all the pub keys from the database.
    Args:
        database (str): The database to query.

    Returns:
        set: A set of all the pub keys.
    """
    all_pub_key_aliases = {}
    async with MongoDBClient(
        db_conn=CONFIG.default_database_connection, db_name=database, db_user="default"
    ) as db_client:
        cursor = await db_client.find("pub_keys", {})
        async for document in cursor:
            all_pub_key_aliases[document["pub_key"]] = document["alias"]
    return all_pub_key_aliases


def get_final_destination(payment_alias: list[str]) -> str:
    if len(payment_alias) == 1:
        return payment_alias[0]
    if payment_alias[-1] == "Unknown":
        if payment_alias[-2] == "magnetron":
            return "Muun User"
        elif payment_alias[-2] == "ACINQ":
            return "Phoenix User"
    return payment_alias[-1]


async def run(node: str, database: str):
    """
    Main function to run the LND gRPC client.
    Args:
        node (str): The node to monitor.
        database (str): The database to update.

    Returns:
        None
    """
    all_pub_key_aliases = await get_all_pub_key_aliases(database)
    all_aliases = {}
    async with MongoDBClient(
        db_conn=CONFIG.default_database_connection, db_name=database, db_user="default"
    ) as db_client:
        async with LNDClient(node) as lnd_client:
            cursor = await db_client.find("payments", {})
            tasks = []
            async for document in cursor:
                document.pop("route", None)
                document.pop("destination_alias", None)
                document.pop("reversed_aliases", None)
                try:
                    payment = Payment.model_validate(document)
                    payment.route = []
                except Exception as e:
                    logger.error(f"Error validating payment: {e}")
                    pass
                # unpack a tuple of the payment model destination pub_key
                pub_keys = payment.destination_pub_keys
                if payment.route or not pub_keys:
                    continue

                for pub_key in pub_keys:
                    if pub_key not in all_pub_key_aliases.keys():
                        node_info = await get_node_info(pub_key, lnd_client)
                        if node_info.node.alias:
                            all_aliases[pub_key] = node_info.node.alias
                        else:
                            all_aliases[pub_key] = f"Unknown {pub_key[-6:]}"
                        hop_alias = NodeAlias(
                            pub_key=pub_key, alias=all_aliases[pub_key]
                        )
                        ans = await db_client.update_one(
                            collection_name="pub_keys",
                            query={"pub_key": pub_key},
                            update=hop_alias.model_dump(),
                            upsert=True,
                        )
                        all_pub_key_aliases[pub_key] = all_aliases[pub_key]
                        payment.route.append(hop_alias)
                    else:
                        hop_alias = NodeAlias(
                            pub_key=pub_key, alias=all_pub_key_aliases[pub_key]
                        )
                        payment.route.append(hop_alias)

                logger.info(f"{payment.destination}  || {payment.route_str}")
                payment_id = ObjectId(document["_id"])
                tasks.append(
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
            await asyncio.gather(*tasks)

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
