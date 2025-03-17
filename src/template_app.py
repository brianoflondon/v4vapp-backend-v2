import asyncio
import sys
from typing import Annotated, Optional

import typer

from lnd_monitor_v2 import InternalConfig, logger

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
ICON = "🏆"
app = typer.Typer()


async def main_async_start(database_connection: str, db_name: str, lnd_connection: str):
    """
    Main function to run Template app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    logger.info(
        f"🔗 Database connection: {database_connection} "
        f"🔗 Database name: {db_name} "
        f"🔗 Lightning node: {lnd_connection} "
    )
    pass


@app.command()
def main(
    database_connection: Annotated[
        str | None,
        typer.Argument(
            help=(
                f"The database connection to use. "
                f"Choose from: {CONFIG.db_connections_names}"
            )
        ),
    ] = CONFIG.default_db_connection,
    db_name: Annotated[
        Optional[str],
        typer.Argument(
            help=(f"The database to monitor." f"Choose from: {CONFIG.dbs_names}")
        ),
    ] = CONFIG.default_db_name,
    lnd_connection: Annotated[
        Optional[str],
        typer.Argument(
            help=(
                f"The Lightning node to monitor. If not provided, "
                f"defaults to the value: "
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
    icon = ICON
    logger.info(f"{icon} ✅ Template App. Started. Version: {CONFIG.version}")

    asyncio.run(main_async_start(database_connection, db_name, lnd_connection))
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
