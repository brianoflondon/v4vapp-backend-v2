import asyncio
import sys
from typing import Annotated, Optional

import typer

from v4vapp_backend_v2.config.setup import InternalConfig, logger

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
ICON = "🅑"
app = typer.Typer()


async def main_async_start():
    """
    Main function to run Template app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    logger.info(f"{ICON} Binance Monitor started.")

    await check_notifications()


async def check_notifications():
    await asyncio.sleep(1)
    while (
        INTERNAL_CONFIG.notification_loop.is_running()
        or INTERNAL_CONFIG.notification_lock
    ):
        print(
            f"Notification loop: {INTERNAL_CONFIG.notification_loop.is_running()} "
            f"Notification lock: {INTERNAL_CONFIG.notification_lock}"
        )
        await asyncio.sleep(0.1)
    return


@app.command()
def main(
    testnet: Annotated[
        bool,
        typer.Option(help=("Use the Binance testnet. Defaults to False.")),
    ] = False,
):
    f"""
    Monitors a Binance account
    Args:


    Returns:
        None
    """
    icon = ICON
    logger.info(
        f"{icon} ✅ Binance Monitor. Started. Version: {CONFIG.version}",
        extra={"notification": True},
    )

    asyncio.run(main_async_start())
    print("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "binance_monitor"
        app()
    except KeyboardInterrupt:
        print("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
