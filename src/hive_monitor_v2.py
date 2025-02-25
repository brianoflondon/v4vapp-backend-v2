import asyncio
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Annotated, List, Tuple

import typer
from lighthive.client import Client  # type: ignore
from lighthive.exceptions import RPCNodeException  # type: ignore
from lighthive.helpers.amount import Amount  # type: ignore
from lighthive.helpers.event_listener import EventListener  # type: ignore
from requests.exceptions import HTTPError

from lnd_monitor_v2 import InternalConfig, logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.helpers.hive_extras import (
    get_good_nodes,
    get_hive_block_explorer_link,
)

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
app = typer.Typer()

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"


def remove_ms(delta: timedelta) -> timedelta:
    return timedelta(days=delta.days, seconds=delta.seconds)


def format_hive_transaction(event: dict) -> Tuple[str, str]:
    """
    Format the Hive transaction event. Return two strings
    first one for a log message and the second one for a notification.
    Args:
        event (dict): The Hive transaction event.

    Returns:
        str: The formatted Hive transaction event.
    """
    ICON = "🐝"
    time_diff = remove_ms(
        datetime.now(tz=timezone.utc)
        - datetime.fromisoformat(event["timestamp"]).replace(tzinfo=timezone.utc)
    )

    link_url = get_hive_block_explorer_link(event["trx_id"])
    transfer = event["op"][1]

    amount = Amount(transfer["amount"])
    notification_str = (
        f"{ICON} {transfer['from']} "
        f"sent {transfer['amount']} "
        f"to {transfer['to']} "
        f" - {transfer['memo']} "
        f"{link_url}"
    )

    log_str = (
        f"{ICON} {transfer['from']:<17} "
        f"sent {amount.amount:12,.3f} {amount.symbol:>4} "
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
    transfer = event["op"][1]
    if "to" in transfer and transfer["to"] in watch_user:
        return True
    if "from" in transfer and transfer["from"] in watch_user:
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
        logger.info(f"Node: {node}", extra={"node": node})
    return good_nodes


async def run(watch_users: List[str]):
    """
    Main function to run the Hive Watcher client.
    Args:
        watch_users (List[str]): The Hive user(s) to watch for transactions.

    Returns:
        None
    """

    def condition(operation_value: dict) -> bool:
        """
        Condition to check if a transaction is valid.
        Args:
            transaction (dict): The transaction to check.

        Returns:
            bool: True if the transaction is valid.
        """
        return True
        if "to" in operation_value and operation_value["to"] in watch_users:
            return True
        if "from" in operation_value and operation_value["from"] in watch_users:
            return True

        return False

    icon = "🐝"
    logger.info(f"{icon} Watching users: {watch_users}")
    good_nodes = await review_good_nodes()
    hive_client = Client(
        load_balance_nodes=True, circuit_breaker=True, nodes=good_nodes
    )
    last_good_block = (
        hive_client.get_dynamic_global_properties().get("head_block_number") - 30
    )
    while True:
        logger.info(f"Last good block: {last_good_block}")
        events = EventListener(client=hive_client, start_block=last_good_block + 1)
        async_events = sync_to_async_iterable(
            events.on(["transfer"], condition=condition)
        )
        try:
            async for event in async_events:
                if "op" in event and event["op"][0] == "transfer":
                    notification = watch_users_notification(event, watch_users)
                    log_str, notification_str = format_hive_transaction(event)
                    logger.info(
                        log_str + f" {hive_client.current_node}",
                        extra={"event": event},
                    )
                    if notification:
                        logger.info(
                            notification_str,
                            extra={"notification": notification, "event": event},
                        )
                last_good_block = event["block"]
        except RPCNodeException as e:
            logger.warning(
                f"RPC Node: {hive_client.current_node} {e}",
                extra={
                    "notification": False,
                    "error": e,
                    "hive_client": hive_client.__dict__,
                },
            )

        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info("Keyboard interrupt: Stopping event listener.")
            raise e

        except HTTPError as e:
            logger.warning(f"HTTP Error {e}", extra={"error": e})

        except Exception as e:
            logger.warning(e)


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
    icon = "🐝"
    logger.info(
        f"{icon} ✅ Hive Monitor v2: " f"{icon}. Version: {CONFIG.version}",
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
        logger.info("👋 Goodbye!")
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
