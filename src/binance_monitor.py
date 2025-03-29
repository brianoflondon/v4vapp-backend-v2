import asyncio
import sys
from timeit import default_timer as timer
from typing import Annotated

import typer

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.binance_extras import get_balances, get_current_price
from v4vapp_backend_v2.helpers.general_purpose_funcs import draw_percentage_meter

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
ICON = "🅑"
app = typer.Typer()

BINANACE_HIVE_ALERT_LEVEL_SATS = 300_000
BINANACE_BTC_ALERT_LEVEL = 0.02


async def check_binance_balances():
    """
    Asynchronously monitors Binance balances and logs updates.

    This function continuously checks Binance account balances in a loop,
    compares them with a saved state, and logs a message if there are changes
    or if certain conditions are met. It also ensures that notifications are
    sent only once per balance change unless the balance falls below a target.

    Key Features:
    - Retrieves and compares Binance balances.
    - Sends notifications when balances change or fall below a target.
    - Logs messages with additional metadata for notifications and balance details.
    - Handles exceptions gracefully and logs errors.
    - Resets the notification state every 10 minutes.

    Note:
    - The function runs indefinitely with a 60-second delay between iterations.
    - It uses an external `generate_message` function to compute new balances,
        target values, and the message to log.
    """
    """Get the Binance balances"""
    saved_balances = {}
    send_message = True
    start = timer()
    while True:
        testnet = False
        try:
            new_balances, hive_target, message = generate_message(
                saved_balances,
                testnet,
            )
            silent = True if new_balances.get("HIVE") > hive_target else False
            if new_balances != saved_balances:
                send_message = True
            if send_message:
                logger.info(
                    message,
                    extra={
                        "notification": True,
                        "binance-balances": new_balances,
                        "silent": silent,
                    },
                )
            send_message = False  # Send message once unless the balance changes
            saved_balances = new_balances
        except Exception as ex:
            logger.error(f"Problem with API. {ex} {ex.__class__}")
            logger.exception(ex, extra={"error": ex, "notification": False})

        finally:
            await asyncio.sleep(60)
            elapsed = timer() - start
            if elapsed > 3600:  # or 1 hour
                send_message = True
                start = timer()


def generate_message(saved_balances: dict, testnet: bool = False):
    """
    Generates a message summarizing the current and target balances of HIVE and SATS,
    along with any changes (delta) in balances since the last check.

    Args:
        saved_balances (dict): A dictionary containing the previously saved balances
            for comparison. Keys are asset symbols (e.g., "HIVE", "SATS") and values
            are their respective balances.
        testnet (bool, optional): A flag indicating whether to use the Binance testnet
            for fetching balances and prices. Defaults to False.

    Returns:
        tuple: A tuple containing:
            - balances (dict): The current balances of assets (e.g., "HIVE", "SATS").
            - hive_target (float): The target HIVE balance calculated based on the
              alert level in SATS and the current HIVEBTC price.
            - message (str): A formatted string summarizing the current status,
              including the percentage meter, delta balances, and target information.
    """
    delta_message = ""
    delta_balances = {}
    balances = get_balances(["BTC", "HIVE"], testnet=testnet)
    hive_balance = balances.get("HIVE", 0)
    sats_balance = balances.get("SATS", 0)
    if saved_balances and balances != saved_balances:
        delta_balances = {
            k: balances.get(k, 0) - saved_balances.get(k, 0) for k in balances
        }
        if delta_balances:
            hive_direction = "⬆️🟢" if delta_balances.get("HIVE", 0) >= 0 else "📉🟥"
            sats_direction = "⬆️🟢" if delta_balances.get("SATS", 0) >= 0 else "📉🟥"
            delta_message = (
                f"{hive_direction} {delta_balances.get('HIVE', 0):.3f} HIVE "
                f"({sats_direction} {int(delta_balances.get('SATS', 0)):,} sats)"
            )
    current_price = get_current_price("HIVEBTC", testnet=testnet)
    saved_balances = balances

    current_price_sats = float(current_price["current_price"]) * 1e8
    hive_target = BINANACE_HIVE_ALERT_LEVEL_SATS / current_price_sats
    percentage = hive_balance / hive_target * 100
    percentage_meter = draw_percentage_meter(
        percentage=percentage, max_percent=200, width=10
    )
    message = (
        f"{ICON} "
        f"{percentage_meter}\n"
        f"{hive_balance - hive_target:.0f} HIVE "
        f"{delta_message} "
        f"{float(hive_balance):,.3f} ({int(sats_balance):,} sats)\n"
        f"Target: {hive_target:.3f}"
    )

    return balances, hive_target, message


async def main_async_start():
    """
    Main function to run Template app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    try:
        logger.info(f"{ICON} Binance Monitor started.")
        await check_binance_balances()

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        logger.info(
            f"{ICON} 👋 Goodbye! from Hive Monitor", extra={"notification": True}
        )
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(
            f"{ICON} Irregular shutdown in Binance Monitor {e}", extra={"error": e}
        )
        raise e
    finally:
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
    """
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
