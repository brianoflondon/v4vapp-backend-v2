import asyncio
import os
import signal
import sys
import threading
import time
from decimal import Decimal
from timeit import default_timer as timer
from typing import Annotated

import typer
from urllib3.exceptions import NameResolutionError

from status.status_api import StatusAPI, StatusAPIException
from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import (
    DEFAULT_CONFIG_FILENAME,
    InternalConfig,
    StartupFailure,
    logger,
)
from v4vapp_backend_v2.conversion.exchange_protocol import (
    ExchangeConnectionError,
    get_exchange_adapter,
)
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    RebalanceDirection,
    add_pending_rebalance,
)
from v4vapp_backend_v2.database.db_pymongo import DBConn, DBConnConnectionException
from v4vapp_backend_v2.helpers.general_purpose_funcs import draw_percentage_meter

ICON = "🅑"
app = typer.Typer()

BINANCE_HIVE_ALERT_LEVEL_SATS = 300_000
BINANCE_BTC_ALERT_LEVEL = 0.02

STATUS_MESSAGE_TIME_MIN = 60

# Outer bound for one poll. Each HTTP call times out at BINANCE_HTTP_TIMEOUT_S (15s)
# and get_balances / get_current_price retry up to 3 times, so this is longer than
# a single socket timeout and shorter than the health-check stale limit.
BINANCE_POLL_TIMEOUT_S = 50
HEALTH_MAX_STALE_S = 180
BALANCE_TASK_NAME = "binance_monitor.check_balances"

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()
_fetch_lock = threading.Lock()
_last_success_monotonic: float | None = None
_monitor_started_monotonic: float | None = None


class BinancePollInProgress(Exception):
    """Raised when a new poll starts while the previous HTTP call is still running."""


def fetch_balances_and_price() -> tuple[dict, Decimal]:
    """Blocking Binance balance and HIVE/BTC price fetch. Safe to run in a thread."""
    if not _fetch_lock.acquire(blocking=False):
        raise BinancePollInProgress("previous Binance poll is still running")
    try:
        adapter = get_exchange_adapter()
        balances = adapter.get_balances(["BTC", "HIVE"])
        price = adapter.get_current_price("HIVE", "BTC")
        return balances, price
    finally:
        _fetch_lock.release()


def _log_poll_failure(ex: Exception, started: float, *, error_code: str | None) -> None:
    elapsed_ms = round((time.monotonic() - started) * 1000)
    extra: dict = {
        "notification": error_code == "binance_api_error",
        "elapsed_ms": elapsed_ms,
    }
    if error_code:
        extra["error_code"] = error_code
    logger.warning(
        f"{ICON} Binance poll failed after {elapsed_ms}ms: {type(ex).__name__}: {ex}",
        extra=extra,
    )


async def health_check():
    """
    Fail when the balance loop is gone or the last successful poll is too old.

    A hung Binance call used to leave the check_balances task listed as running
    while the sleep task was absent, so the old task-name check did not describe
    the failure. Staleness of the last success does.
    """
    running_tasks = {t.get_name() for t in asyncio.all_tasks()}
    if BALANCE_TASK_NAME not in running_tasks:
        raise StatusAPIException(
            f"{ICON} Health check warning: Task '{BALANCE_TASK_NAME}' is not running."
        )

    now = time.monotonic()
    if _last_success_monotonic is None:
        started = _monitor_started_monotonic if _monitor_started_monotonic is not None else now
        age = now - started
        if age > HEALTH_MAX_STALE_S:
            raise StatusAPIException(
                f"{ICON} Binance Monitor has not completed a poll after {age:.0f}s"
            )
        return {
            "status": "OK",
            "message": "Binance Monitor is starting",
            "last_success_age_s": None,
        }

    age = now - _last_success_monotonic
    if age > HEALTH_MAX_STALE_S:
        raise StatusAPIException(
            f"{ICON} Binance Monitor last successful poll was {age:.0f}s ago"
        )
    return {
        "status": "OK",
        "message": "Binance Monitor is running",
        "last_success_age_s": round(age, 1),
    }


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info(f"{ICON} Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def sleep_with_shutdown_check(duration: int, check_interval: float = 1.0):
    """
    Sleep for a given duration, but check periodically if a shutdown event is set.

    Args:
        duration (int): Total duration to sleep in seconds.
        check_interval (float): Interval to check the shutdown event in seconds.

    Returns:
        None
    """
    elapsed = 0.0
    while elapsed < duration:
        if shutdown_event.is_set():
            logger.info(f"{ICON} Shutdown event detected during sleep.")
            raise asyncio.CancelledError("Shutdown event triggered")
        await asyncio.sleep(check_interval)
        elapsed += check_interval


async def poll_binance_balances(saved_balances: dict):
    """
    Fetch balances off the event loop and format the status message.

    Returns the generate_message tuple, or None when the poll failed and was logged.
    CancelledError propagates so shutdown is not turned into a failed poll.
    """
    started = time.monotonic()
    outcome = "ok"
    logger.info(f"{ICON} Binance poll started", extra={"notification": False})
    try:
        balances, price = await asyncio.wait_for(
            asyncio.to_thread(fetch_balances_and_price),
            timeout=BINANCE_POLL_TIMEOUT_S,
        )
        return generate_message(saved_balances, balances, price)
    except asyncio.CancelledError:
        outcome = "cancelled"
        raise
    except BinancePollInProgress as ex:
        outcome = f"{type(ex).__name__}: {ex}"
        _log_poll_failure(ex, started, error_code=None)
        return None
    except TimeoutError as ex:
        outcome = f"TimeoutError: {ex}"
        _log_poll_failure(ex, started, error_code="binance_api_error")
        return None
    except NameResolutionError as ex:
        outcome = f"{type(ex).__name__}: {ex}"
        _log_poll_failure(ex, started, error_code="network_error")
        return None
    except OSError as ex:
        outcome = f"{type(ex).__name__}: {ex}"
        _log_poll_failure(ex, started, error_code="network_error")
        return None
    except ExchangeConnectionError as ex:
        outcome = f"{type(ex).__name__}: {ex}"
        _log_poll_failure(ex, started, error_code="binance_api_error")
        return None
    except Exception as ex:
        outcome = f"{type(ex).__name__}: {ex}"
        elapsed_ms = round((time.monotonic() - started) * 1000)
        logger.exception(
            f"{ICON} Problem with Binance API after {elapsed_ms}ms: {type(ex).__name__}: {ex}",
            extra={"notification": False, "elapsed_ms": elapsed_ms},
        )
        return None
    finally:
        elapsed_ms = round((time.monotonic() - started) * 1000)
        logger.info(
            f"{ICON} Binance poll ended after {elapsed_ms}ms ({outcome})",
            extra={
                "notification": False,
                "elapsed_ms": elapsed_ms,
                "poll_outcome": outcome,
            },
        )


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
    global _last_success_monotonic, _monitor_started_monotonic
    _monitor_started_monotonic = time.monotonic()
    saved_balances = {}
    send_message = True
    start = timer()

    process_name = os.path.splitext(os.path.basename(__file__))[0]
    health_check_port = os.environ.get("HEALTH_CHECK_PORT", "6001")
    status_api = StatusAPI(
        port=int(health_check_port),
        health_check_func=health_check,
        shutdown_event=shutdown_event,
        process_name=process_name,
        version=__version__,
    )  # Use a port from config if needed
    asyncio.create_task(status_api.start(), name="status_api")
    logger.info(f"{ICON} Status API started on port {health_check_port}")
    while not shutdown_event.is_set():
        try:
            if shutdown_event.is_set():
                raise asyncio.CancelledError("Docker Shutdown")
            try:
                polled = await poll_binance_balances(saved_balances)
            except asyncio.CancelledError:
                logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
                raise
            if polled is None:
                send_message = True
            else:
                new_balances, hive_target, notification_str, log_str = polled
                _last_success_monotonic = time.monotonic()
                silent = new_balances.get("HIVE", 0) > hive_target
                if new_balances != saved_balances:
                    send_message = True
                if send_message:
                    logger.info(
                        log_str,
                        extra={
                            "notification": True,
                            "binance-balances": new_balances,
                            "silent": silent,
                            "notification_str": notification_str,
                            "error_code_clear": ["binance_api_error", "network_error"],
                        },
                    )
                send_message = False  # Send message once unless the balance changes
                saved_balances = new_balances

        finally:
            task = asyncio.create_task(
                sleep_with_shutdown_check(60, 1),
                name="binance_monitor.sleep_with_shutdown_check",
            )
            await task
            elapsed = timer() - start
            if elapsed > STATUS_MESSAGE_TIME_MIN * 60:
                send_message = True
                start = timer()


def generate_message(
    saved_balances: dict, balances: dict, current_price_decimal: Decimal
):
    """
    Generates a message summarizing the current and target balances of HIVE and SATS,
    along with any changes (delta) in balances since the last check.

    Args:
        saved_balances (dict): Previously saved balances for comparison.
        balances (dict): Current balances, already fetched off the event loop.
        current_price_decimal (Decimal): Current HIVE/BTC price.

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
    hive_balance = Decimal(balances.get("HIVE", 0))
    sats_balance = Decimal(balances.get("SATS", 0))
    if saved_balances and balances != saved_balances:
        delta_balances = {
            k: Decimal(balances.get(k, 0)) - Decimal(saved_balances.get(k, 0)) for k in balances
        }
        if delta_balances:
            hive_direction = "⬆️🟢" if delta_balances.get("HIVE", 0) >= 0 else "📉🟥"
            sats_direction = "⬆️🟢" if delta_balances.get("SATS", 0) >= 0 else "📉🟥"
            delta_message = (
                f"{hive_direction} {delta_balances.get('HIVE', 0):.3f} HIVE "
                f"({sats_direction} {int(delta_balances.get('SATS', 0)):,} sats)"
            )
    current_price_sats = current_price_decimal * Decimal("1e8")
    if current_price_sats <= 0:
        raise ExchangeConnectionError(
            "HIVE/BTC price returned as zero — Binance API may be unavailable"
        )
    hive_target = Decimal(str(BINANCE_HIVE_ALERT_LEVEL_SATS)) / current_price_sats
    percentage = hive_balance / hive_target * 100
    percentage_meter = draw_percentage_meter(percentage=percentage, max_percent=300, width=9)
    notification_str = (
        f"{ICON} "
        f"{percentage_meter}\n"
        f"{hive_balance - hive_target:.0f} HIVE "
        f"{delta_message} "
        f"{float(hive_balance):,.3f} ({int(sats_balance):,} sats)\n"
        f"Target: {hive_target:.3f}"
    )
    log_str = notification_str.replace("\n", " ")
    if percentage < 100:
        asyncio.create_task(testnet_rebalance(hive_balance, hive_target))

    return balances, hive_target, notification_str, log_str


async def testnet_rebalance(hive_qty: Decimal, hive_target: Decimal):
    """
    Only if we are set up to look at testnet, do a rebalance there to bring Hive to
    target level.

    """
    try:
        binance_config = InternalConfig().binance_config
        # Only do this on testnet
        if not binance_config.use_testnet:
            return
        if binance_config.active_network.no_trade:
            logger.info(
                f"{ICON} Skipping testnet rebalance: no_trade is enabled for active network.",
                extra={"notification": False},
            )
            return
    except Exception as e:
        logger.warning(
            f"{ICON} Error accessing Binance config: {e}",
            extra={"error": e, "notification": False},
        )
        return

    try:
        exchange_adapter = get_exchange_adapter()
        quantity_to_rebalance = hive_target - hive_qty

        result = await add_pending_rebalance(
            exchange_adapter=exchange_adapter,
            base_asset="HIVE",  # Always HIVE - Binance doesn't trade HBD
            quote_asset="BTC",
            direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
            qty=quantity_to_rebalance,
            transaction_id="binance_monitor_rebalance_to_target",
        )
        logger.info(
            f"{ICON} Testnet rebalance placed: {result}",
            extra={"notification": True},
        )
    except Exception as e:
        logger.error(
            f"{ICON} Error during testnet rebalance: {e}",
            extra={"error": e, "notification": False},
        )


def _log_balance_task_done(task: asyncio.Task) -> None:
    """Log when the balance loop ends. A hung poll never gets here; a crash does."""
    if task.cancelled():
        logger.info(f"{ICON} {BALANCE_TASK_NAME} cancelled")
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            f"{ICON} {BALANCE_TASK_NAME} exited: {type(exc).__name__}: {exc}",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        return
    if shutdown_event.is_set():
        logger.info(f"{ICON} {BALANCE_TASK_NAME} stopped after shutdown")
        return
    logger.error(f"{ICON} {BALANCE_TASK_NAME} exited while the monitor is still running")


async def main_async_start():
    """
    Main function to run Template app.
    Args:
        node (str): example command line param.

    Returns:
        None
    """
    # Ensure notification handler uses the running loop (non-blocking path)
    InternalConfig.notification_loop = asyncio.get_running_loop()

    try:
        db_conn = DBConn()
        await db_conn.setup_database()
        logger.info(f"{ICON} Binance Monitor started.")
        # Get the current event loop
        loop = asyncio.get_running_loop()

        # Register signal handlers for SIGTERM and SIGINT
        loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
        loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

        balance_task = asyncio.create_task(
            check_binance_balances(), name=BALANCE_TASK_NAME
        )
        balance_task.add_done_callback(_log_balance_task_done)
        tasks = [balance_task]

        # Wait until shutdown is requested
        await shutdown_event.wait()
        # Cancel tasks and wait for them to finish
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        raise
    except DBConnConnectionException as e:
        logger.error(
            f"{ICON} Database connection error in Binance Monitor: {e}",
            extra={"error": e, "notification": False},
        )
        return
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Binance Monitor {e}", extra={"error": e})
        raise e
    finally:
        # Cancel all other tasks and exit cleanly
        current_task = asyncio.current_task()
        remaining = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in remaining:
            task.cancel()
        await asyncio.gather(*remaining, return_exceptions=True)
        logger.info(f"{ICON} 👋 Goodbye! from Binance Monitor", extra={"notification": True})
        logger.info(f"{ICON} Clearing notifications")
        await asyncio.sleep(2)
        InternalConfig().shutdown()


@app.command()
def main(
    testnet: Annotated[
        bool,
        typer.Option(help=("Use the Binance testnet. Defaults to False.")),
    ] = False,
    config_filename: Annotated[
        str,
        typer.Option(
            "-c",
            "--config",
            "--config-filename",
            help="The name of the config file (in a folder called ./config)",
            show_default=True,
        ),
    ] = DEFAULT_CONFIG_FILENAME,
):
    """
    Monitors a Binance account
    Args:
        testnet (bool): Use the Binance testnet. Defaults to False.
        config_filename (str): The name of the config file (in a folder called ./config).


    Returns:
        None
    """
    icon = ICON
    InternalConfig(config_filename=config_filename)
    logger.info(
        f"{icon} ✅ Binance Monitor. Started. Version: {__version__}",
        extra={"notification": True},
    )

    asyncio.run(main_async_start())
    print("👋 Goodbye!")


if __name__ == "__main__":
    try:
        logger.name = "binance_monitor"
        app()
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("👋 Goodbye!")
        sys.exit(0)

    except StartupFailure as e:
        print(f"{ICON} Startup failure: {e}")
        sys.exit(0)

    except Exception as e:
        logger.error("🔴 Unhandled exception in binance_monitor", exc_info=e, stack_info=True)
        logger.exception(e, extra={"error": e, "notification": True})
        print(e)
        sys.exit(1)
