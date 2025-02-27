import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, List, Tuple

import typer
from beem.amount import Amount
from beem.blockchain import Blockchain
from colorama import Fore, Style
from pymongo.errors import DuplicateKeyError
from requests.exceptions import HTTPError

from lnd_monitor_v2 import InternalConfig, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.helpers.hive_extras import (
    MAX_HIVE_BATCH_SIZE,
    get_good_nodes,
    get_hive_block_explorer_link,
    get_hive_client,
)

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
HIVE_DATABASE_CONNECTION = "local_connection"
HIVE_DATABASE = "lnd_monitor_v2_voltage"
HIVE_DATABASE_USER = "lnd_monitor"
HIVE_TRX_COLLECTION = "hive_trx_beem"

app = typer.Typer()
icon = "🐝"

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"


def remove_ms(delta: timedelta) -> timedelta:
    return timedelta(days=delta.days, seconds=delta.seconds)


def check_time_diff(timestamp: str | datetime) -> timedelta:
    """
    Calculate the difference between the current time and a given timestamp
    Removes the milliseconds from the timedelta.

    Args:
        timestamp (str | datetime): The timestamp in ISO format or datetime object () to
        compare with the current time. Forces UTC if not timezone aware.

    Returns:
        timedelta: The difference between the current time and the given timestamp.

    Logs a warning if the time difference is greater than 1 minute.
    """
    try:
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp).replace(tzinfo=timezone.utc)
        else:
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        time_diff = remove_ms(datetime.now(tz=timezone.utc) - timestamp)
    except (ValueError, AttributeError, OverflowError, TypeError):
        time_diff = timedelta(seconds=0)
    return time_diff


def log_time_difference_errors(timestamp: str | datetime, error_code: str = ""):
    """
    Logs time difference errors based on the provided timestamp.

    This function checks the time difference between the current time and the provided
    timestamp. If the time difference is greater than 1 minute, it logs a warning with
    an error code indicating the time difference is greater than 1 minute. If an error
    code is provided and the time difference is less than 1 minute, it logs a warning
    indicating the error code should be cleared.

    Args:
        timestamp (str): The timestamp to compare against the current time.
        error_code (str, optional): The error code to log if the time difference is less
            than 1 minute. Defaults to an empty string.

    Returns:
        Tuple[str, bool]: A tuple containing the error code and a flag indicating whether
            the error code should be cleared
    """
    time_diff = check_time_diff(timestamp)
    if not error_code and time_diff > timedelta(minutes=1):
        error_code = "time_diff_greater_than_1_minute"
        logger.warning(
            f"{icon} Time diff: {time_diff} greater than 1 minute",
            extra={
                "notification": True,
                "error_code": error_code,
            },
        )
    if error_code and time_diff <= timedelta(minutes=1):
        logger.warning(
            f"{icon} Time diff: {time_diff} less than 1 minute",
            extra={
                "notification": True,
                "error_code_clear": error_code,
            },
        )
        error_code = ""
    return error_code


def format_hive_transaction(event: dict) -> Tuple[str, str]:
    """
    Format the Hive transaction event. Return two strings
    first one for a log message and the second one for a notification.
    Args:
        event (dict): The Hive transaction event.

    Returns:
        str: The formatted Hive transaction event.
    """
    time_diff = check_time_diff(event["timestamp"])

    link_url = get_hive_block_explorer_link(event["trx_id"])
    transfer = event

    amount = Amount(transfer["amount"])
    notification_str = (
        f"{icon} {transfer['from']} "
        f"sent {amount} "
        f"to {transfer['to']} "
        f" - {transfer['memo'][:16]} "
        f"{link_url}"
    )

    log_str = (
        f"{icon} {transfer['from']:<17} "
        f"sent {amount.amount_decimal:12,.3f} {amount.symbol:>4} "
        f"to {transfer['to']:<17} "
        f" - {transfer['memo'][:30]:>30} "
        f"{time_diff} ago "
        f"{link_url}"
    )
    return log_str, notification_str


def watch_users_notification(event: dict, watch_user: List[str]) -> bool:
    """
    Send notification if the user is in the watch list.
    Args:
        transfer (dict): The transaction transfer.
        watch_user (List[str]): The list of users to watch.
    Returns:
        bool: True if the user is in the watch list.
    """
    if event.get("to", "") in watch_user:
        return True
    if event.get("from", "") in watch_user:
        return True
    return False


async def review_good_nodes() -> List[str]:
    """
    Asynchronously reviews and logs good nodes.

    This function retrieves a list of good nodes using the `get_good_nodes` function,
    logs each node using the `logger`, and returns the list of good nodes.

    Returns:
        List[str]: A list of good nodes.
    """
    good_nodes = get_good_nodes()
    for node in good_nodes:
        logger.info(f"{icon} Node: {node}", extra={"node": node})
    return good_nodes


async def transactions_report(hive_event: dict, *args: Any, **kwargs: Any) -> None:
    """
    Asynchronously reports transactions.

    This function reports transactions by logging the transaction event.

    Args:
        hive_event (dict): The Hive transaction event.
    """
    _, notification_str = format_hive_transaction(hive_event)
    notification = True
    logger.info(
        notification_str,
        extra={"notification": notification, "event": hive_event},
    )


async def db_store_transaction(hive_event: dict, *args: Any, **kwargs: Any) -> None:
    """
    Asynchronously stores transactions in the database.

    This function stores transactions in the
    database by logging the transaction event.
    """
    try:
        async with MongoDBClient(
            db_conn=HIVE_DATABASE_CONNECTION,
            db_name=HIVE_DATABASE,
            db_user=HIVE_DATABASE_USER,
        ) as db_client:
            ans = await db_client.insert_one(HIVE_TRX_COLLECTION, hive_event)

    except DuplicateKeyError:
        pass

    except Exception as e:
        logger.error(e)


async def get_last_good_block() -> int:
    """
    Asynchronously retrieves the last good block.

    This function retrieves the last good block by getting the dynamic global properties
    from the Hive client and returning the head block number minus 30.

    Returns:
        int: The last good block.
    """
    try:
        async with MongoDBClient(
            db_conn=HIVE_DATABASE_CONNECTION,
            db_name=HIVE_DATABASE,
            db_user=HIVE_DATABASE_USER,
        ) as db_client:
            ans = await db_client.find_one(
                HIVE_TRX_COLLECTION, {}, sort=[("block_num", -1)]
            )
            if ans:
                time_diff = check_time_diff(ans["timestamp"])
                logger.info(
                    f"{icon} Last good block: {ans['block_num']} "
                    f"{ans['timestamp']} {time_diff} ago",
                    extra={"db": ans},
                )
                last_good_block = int(ans["block_num"])
            else:
                try:
                    last_good_block = get_hive_client().get_dynamic_global_properties()[
                        "head_block_number"
                    ]
                except Exception as e:
                    logger.error(e)
                    last_good_block = 93692232
            return last_good_block

    except Exception as e:
        logger.error(e)
        raise e
    return 0


async def transactions_loop(watch_users: List[str]):
    """
    Asynchronously loops through transactions.

    This function creates an event listener for transactions, then loops through
    the transactions and logs them.
    """

    logger.info(f"{icon} Watching users: {watch_users}")
    op_names = ["transfer"]
    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    last_good_block = await get_last_good_block()
    while True:
        logger.info(f"{icon} Last good block: {last_good_block}")
        async_stream = sync_to_async_iterable(
            hive_blockchain.stream(
                opNames=op_names,
                start=last_good_block,
                raw_ops=False,
                max_batch_size=MAX_HIVE_BATCH_SIZE,
            )
        )
        error_code = ""
        try:
            async for hive_event in async_stream:
                last_good_block = hive_event.get("block_num")
                notification = watch_users_notification(hive_event, watch_users)
                log_str, _ = format_hive_transaction(hive_event)
                error_code = log_time_difference_errors(
                    hive_event["timestamp"], error_code
                )
                logger.info(
                    log_str + f" {hive_client.rpc.url}",
                    extra={
                        "event": hive_event,
                    },
                )
                async_publish(Events.HIVE_TRANSFER, hive_event)
                if notification:
                    async_publish(Events.HIVE_TRANSFER_NOTIFY, hive_event)  # noqa

            #     if "op" in hive_event and hive_event["op"][0] == "transfer":
            #         last_good_block = hive_event["block"]
            #         notification = watch_users_notification(hive_event, watch_users)
            #         log_str, _ = format_hive_transaction(hive_event)
            #         error_code, error_code_clear = log_time_difference_errors(
            #             hive_event["timestamp"], error_code, error_code_clear
            #         )
            #         logger.info(
            #             log_str + f" {hive_client.current_node}",
            #             extra={
            #                 "event": hive_event,
            #             },
            #         )
            #         async_publish(Events.HIVE_TRANSFER, hive_event)
            #         if notification:
            #             async_publish(Events.HIVE_TRANSFER_NOTIFY, hive_event)
            # last_good_block = hive_event["block"]
            # # If no more events, raise an exception to switch to the next node
            # raise RPCNodeException("No more events")
        # except Exception as e:
        #     logger.warning(
        #         f"{icon} RPC Node: {hive_client.current_node} {e}",
        #         extra={
        #             "notification": False,
        #             "error": e,
        #             "hive_client": hive_client.__dict__,
        #         },
        #     )
        #     hive_client.circuit_breaker_cache[hive_client.current_node] = True
        #     hive_client.next_node()
        #     logger.warning(
        #         f"{icon} Switching to node: {hive_client.current_node}",
        #         extra={"hive_client": hive_client.__dict__},
        #     )

        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info("{icon} Keyboard interrupt: Stopping event listener.")
            raise e

        except HTTPError as e:
            logger.warning(f"{icon} HTTP Error {e}", extra={"error": e})

        except Exception as e:
            logger.warning(f"{icon} {e}", extra={"error": e})


async def run(watch_users: List[str]):
    """
    Main function to run the Hive Watcher client.
    Args:
        watch_users (List[str]): The Hive user(s) to watch for transactions.

    Returns:
        None
    """
    try:
        async_subscribe(Events.HIVE_TRANSFER_NOTIFY, transactions_report)
        async_subscribe(Events.HIVE_TRANSFER, db_store_transaction)
        await transactions_loop(watch_users)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{icon} 👋 Received signal to stop. Exiting...")
        INTERNAL_CONFIG.__exit__(None, None, None)


@app.command()
def main(
    watch_users: Annotated[
        List[str],
        typer.Argument(help=("Hive User(s) to watch for transactions")),
    ] = None,
):
    """
    Watch the Hive blockchain for transactions.
    Args:
        watch_user (Annotated[List[str] | None, Argument]): The Hive user(s)
                    to watch for transactions.

    Returns:
        None
    """
    logger.info(
        f"{icon} ✅ Hive Monitor v2: " f"{icon}. Version: {CONFIG.version}",
        extra={"notification": True},
    )
    if watch_users is None:
        watch_users = ["v4vapp", "brianoflondon"]
    asyncio.run(run(watch_users))
    print("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "hive_monitor_v2"
        app()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info(f"{icon} 👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
