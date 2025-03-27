import asyncio
import sys
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
    """Get the Binance balances"""
    saved_balances = {}
    delta_balances = {}
    log_once = True
    while True:
        testnet = False
        try:
            notification = False
            delta_message = ""
            balances = get_balances(["BTC", "HIVE"], testnet=testnet)
            hive_balance = balances.get("HIVE", 0)
            sats_balance = balances.get("SATS", 0)
            if saved_balances and balances != saved_balances:
                log_once = True
                notification = True
                delta_balances = {
                    k: balances.get(k, 0) - saved_balances.get(k, 0) for k in balances
                }
                if delta_balances:
                    delta_message = (
                        f"Δ {delta_balances.get('HIVE', 0):.3f} HIVE "
                        f"({int(delta_balances.get('SATS', 0)):,} sats)"
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
                f"{percentage_meter} "
                f"{hive_balance - hive_target:.0f} HIVE "
                f"{delta_message} "
                f"{float(hive_balance):,.3f} ({int(sats_balance):,} sats) "
                f"Target: {hive_target:.3f}"
            )
            silent = True if balances.get("HIVE") > hive_target else False
            if log_once:
                logger.info(
                    message,
                    extra={
                        "notification": notification,
                        "binance-balances": balances,
                        "silent": silent,
                    },
                )
            delta_balances = {}
            log_once = False

        except Exception as ex:
            logger.error(f"Problem with API. {ex} {ex.__class__}")
            logger.exception(ex, extra={"error": ex, "notification": False})

        finally:
            await asyncio.sleep(60)


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
