import asyncio
import sys
from typing import Annotated, Optional

import typer

from lnd_monitor_v2 import InternalConfig, logger

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
app = typer.Typer()


async def run(node: str):
    """
    Main function to run the LND gRPC client.
    Args:
        node (str): The node to monitor.

    Returns:
        None
    """
    pass


@app.command()
def main(
    database_connection: Annotated[
        str | None,
        typer.Argument(
            help=(f"The database connection to use. Choose from: {CONFIG.dbs_names}")
        ),
    ] = CONFIG.default_db_connection,
    database: Annotated[
        str,
        typer.Argument(
            help=(f"The database to monitor." f"Choose from: {CONFIG.dbs_names}")
        ),
    ] = CONFIG.default_,
    node: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The Lightning node to monitor. If not provided, defaults to the value: "
                f"{CONFIG.default_connection}.\n"
                f"Choose from: {CONFIG.lnd_connections_names}"
            )
        ),
    ] = CONFIG.default_connection,
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
    icon = CONFIG.icon(node)
    logger.info(
        f"{icon} ✅ LND gRPC client started. Monitoring node: {node} {icon}. Version: {CONFIG.version}"
    )
    logger.info(f"{icon} ✅ Database: {database}")
    asyncio.run(run(node))
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
