import asyncio
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import Annotated, Any, List, Tuple

import typer
from beem.amount import Amount  # type: ignore
from beem.blockchain import Blockchain  # type: ignore

# from colorama import Fore, Style
from pymongo.errors import DuplicateKeyError

from lnd_monitor_v2 import InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.hive_extras import (
    MAX_HIVE_BATCH_SIZE,
    get_good_nodes,
    get_hive_block_explorer_link,
    get_hive_client,
    get_hive_witness_details,
)
from v4vapp_backend_v2.helpers.voting_power import VotingPower

INTERNAL_CONFIG = InternalConfig()
CONFIG = INTERNAL_CONFIG.config
HIVE_DATABASE_CONNECTION = "local_connection"
HIVE_DATABASE = "lnd_monitor_v2_voltage"
HIVE_DATABASE_USER = "lnd_monitor"
HIVE_TRX_COLLECTION = "hive_trx_beem"
HIVE_WITNESS_PRODUCER_COLLECTION = "hive_witness"
HIVE_WITNESS_DELAY_FACTOR = 1.2  # 20% over mean block time


TRANSFER_OP_TYPES = ["transfer", "recurrent_transfer"]
OP_NAMES = TRANSFER_OP_TYPES + ["update_proposal_votes", "account_witness_vote"]

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
        Tuple[str, bool]: A tuple containing the error code and a flag indicating
        whether the error code should be cleared
    """
    time_diff = check_time_diff(timestamp)
    if not error_code and time_diff > timedelta(minutes=1):
        error_code = "Hive Time diff greater than 1 min"
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

    log_link = get_hive_block_explorer_link(event["trx_id"], markdown=False)
    markdown_link = (
        get_hive_block_explorer_link(event["trx_id"], markdown=True) + " no_preview"
    )
    transfer = event

    amount = Amount(transfer["amount"])
    notification_str = (
        f"{icon} {transfer['from']} "
        f"sent {amount} "
        f"to {transfer['to']} - "
        f"{transfer['memo'][:30]} - "
        f"{markdown_link}"
    )

    log_str = (
        f"{icon} {transfer['from']:<17} "
        f"sent {amount.amount_decimal:12,.3f} {amount.symbol:>4} "
        f"to {transfer['to']:<17} "
        f" - {transfer['memo'][:30]:>30} "
        f"{time_diff} ago "
        f"{log_link} {transfer['op_in_trx']:>3}"
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
    log_str, notification_str = format_hive_transaction(hive_event)
    notification = True
    logger.info(log_str, extra={"hive_event": hive_event, "notification": False})
    logger.info(
        notification_str,
        extra={"notification": notification, "hive_event": hive_event},
    )


async def witness_vote_report(hive_event: dict, *args: Any, **kwargs: Any) -> None:
    """
    Asynchronously reports witness votes.

    This function reports witness votes by logging the witness vote event.

    Args:
        hive_event (dict): The Hive witness vote event.
    """
    notification = True if hive_event.get("witness") == "brianoflondon" else False
    voted_for = "voted for" if hive_event.get("approve") else "unvoted"
    voter_power = VotingPower(hive_event["account"])
    message = (
        f"{icon}👁️ {hive_event.get('account')} "
        f"{voted_for} {hive_event.get('witness')} "
        f"with {voter_power.vote_value:,.0f} HP"
    )
    logger.info(
        message,
        extra={
            "notification": notification,
            "witness_vote": hive_event,
        },
    )


async def db_store_block_marker(
    hive_event: dict, db_client: MongoDBClient, *args: Any, **kwargs: Any
) -> None:
    """
    Stores a block marker in the database.

    This function updates or inserts a block marker document in the specified MongoDB
    collection.
    The block marker is identified by a unique transaction ID and operation index.

    Args:
        hive_event (dict): A dictionary containing the block event data.
        Expected keys are:
            - "block_num": The block number.
            - "timestamp": The timestamp of the block.
        db_client (MongoDBClient): An instance of the MongoDB client to interact
        with the database.
        *args (Any): Additional positional arguments.
        **kwargs (Any): Additional keyword arguments.

    Returns:
        None

    Raises:
        Exception: If an error occurs during the database operation, it is logged.
    """
    try:
        query = {"trx_id": "block_marker", "op_in_trx": 0}
        block_marker = {
            "block_num": hive_event["block_num"],
            "timestamp": hive_event["timestamp"],
            "trx_id": "block_marker",
            "op_in_trx": 0,
            "_id": "block_marker",
        }
        _ = await db_client.update_one(
            HIVE_TRX_COLLECTION, query=query, update=block_marker, upsert=True
        )
    except DuplicateKeyError:
        pass
    except Exception as e:
        logger.error(e, extra={"error": e})


def get_event_id(hive_event: dict) -> str:
    """
    Get the event id from the Hive event.

    Args:
        hive_event (dict): The Hive event.

    Returns:
        str: The event id.
    """
    trx_id = hive_event.get("trx_id", "")
    op_in_trx = hive_event.get("op_in_trx", 0)
    return f"{trx_id}_{op_in_trx}" if not int(op_in_trx) == 0 else str(trx_id)


async def db_store_transaction(
    hive_event: dict, db_client: MongoDBClient, *args: Any, **kwargs: Any
) -> None:
    """
    This function processes a hive event and stores the transaction details
    in the database. It handles various types of events, including those
    with amounts and account witness votes, and performs necessary
    conversions and calculations before storing the data.

    Args:
        hive_event (dict): The hive event containing transaction details.
        db_client (MongoDBClient): The database client used to store the transaction.
        *args (Any): Additional positional arguments.
        **kwargs (Any): Additional keyword arguments.

    Raises:
        DuplicateKeyError: If a duplicate key error occurs during the database
        operation.
        Exception: For any other exceptions that occur during the process.

    """
    try:
        trx_id = hive_event.get("trx_id", "")
        op_in_trx = hive_event.get("op_in_trx", 0)
        query = {"trx_id": trx_id, "op_in_trx": op_in_trx}
        if hive_event.get("amount"):
            amount = Amount(hive_event["amount"])
            hive_event["amount_decimal"] = str(amount.amount_decimal)
            hive_event["amount_value"] = amount.amount
            hive_event["amount_symbol"] = amount.symbol
            hive_event["amount_str"] = str(amount)
            conv = CryptoConversion(amount)
            await conv.get_quote()
            hive_event["conv"] = conv.c_dict
        if hive_event.get("type") == "account_witness_vote":
            voter_power = VotingPower(hive_event["account"])
            hive_event["vote_value"] = voter_power.vote_value
            hive_event["voter_details"] = asdict(voter_power)
        hive_event["_id"] = get_event_id(hive_event)
        ans = await db_client.update_one(
            HIVE_TRX_COLLECTION, query=query, update=hive_event, upsert=True
        )
        logger.debug(f"{icon} database: {ans}")
    except DuplicateKeyError:
        pass

    except Exception as e:
        logger.error(e, extra={"error": e})


async def get_last_good_block(collection: str = HIVE_TRX_COLLECTION) -> int:
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
                collection_name=collection, query={}, sort=[("block_num", -1)]
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


async def witness_first_run(watch_witness: str) -> dict:
    """
    Asynchronously retrieves the last good block produced by a specified witness
    from the database. If no such block is found, it streams recent blocks from
    the Hive blockchain to find and store the last block produced by the witness.

    Args:
        watch_witness (str): The name of the witness to monitor.

    Returns:
        dict: The last good block produced by the specified witness, or an empty
        dictionary if no such block is found.
    """
    async with MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    ) as db_client:
        last_good_event = await db_client.find_one(
            HIVE_WITNESS_PRODUCER_COLLECTION,
            {"producer": watch_witness},
            sort=[("block_num", -1)],
        )
        if last_good_event:
            time_diff = check_time_diff(last_good_event["timestamp"])
            logger.info(
                f"{icon} Last recorded witness producer block: "
                f"{last_good_event["block_num"]:,.0f} "
                f"for {watch_witness} "
                f"{last_good_event['timestamp']} "
                f"{time_diff}",
                extra={"db": last_good_event, "notification": True},
            )
            return last_good_event

        hive_client = get_hive_client()
        hive_blockchain = Blockchain(hive=hive_client)
        end_block = hive_client.get_dynamic_global_properties().get("head_block_number")
        async_stream = sync_to_async_iterable(
            hive_blockchain.stream(
                opNames=["producer_reward"],
                start=end_block
                - int(140 * 60 / 3),  # go back 140 minutes of 3 second blocks
                stop=end_block,
                only_virtual_ops=True,
                max_batch_size=MAX_HIVE_BATCH_SIZE,
            )
        )
        async for hive_event in async_stream:
            if hive_event.get("producer") == watch_witness:
                _ = await db_client.insert_one(
                    HIVE_WITNESS_PRODUCER_COLLECTION, hive_event
                )
                last_good_event = hive_event
                logger.info(
                    f"{icon} {watch_witness} "
                    f"block: {hive_event['block_num']:,.0f} ",
                    extra={"db": last_good_event, "notification": True},
                )
        if last_good_event:
            return last_good_event
    return {}


async def witness_average_block_time(watch_witness: str) -> timedelta:
    """
    Asynchronously calculates the average block time for a specified witness.

    This function calculates the average block time for a specified witness by
    streaming recent blocks from the Hive blockchain and calculating the time
    difference between each block produced by the witness.

    Args:
        watch_witness (str): The name of the witness to monitor.

    Returns:
        timedelta: The average block time for the specified witness.
    """
    async with MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    ) as db_client:
        cursor = await db_client.find(
            HIVE_WITNESS_PRODUCER_COLLECTION,
            {"producer": watch_witness},
            sort=[("block_num", -1)],
        )
        # loop through the blocks and calculate the average block time
        block_timestamps = []
        counter = 0
        async for block in cursor:
            block_timestamps.append((block["timestamp"]))
            counter += 1
            if counter > 10:
                break

    # Calculate the time differences between consecutive timestamps
    time_differences = [
        (block_timestamps[i - 1] - block_timestamps[i]).total_seconds()
        for i in range(1, len(block_timestamps))
    ]
    # Calculate the mean time difference
    mean_time_diff_seconds = sum(time_differences) / len(time_differences)

    # Convert the mean time difference back to a timedelta object
    mean_time_diff = remove_ms(timedelta(seconds=mean_time_diff_seconds))

    return mean_time_diff


async def witness_loop(watch_witness: str):
    """
    Asynchronously loops through witnesses.

    This function creates an event listener for witnesses, then loops through the
    witnesses and logs them. It connects to a Hive blockchain client and listens for
    producer reward operations. When a reward operation for the specified witness is
    detected, it logs the event and inserts it into a MongoDB collection.

    Args:
        watch_witness (str): The name of the witness to watch.

    Raises:
        KeyboardInterrupt: If the loop is interrupted by a keyboard interrupt.
        asyncio.CancelledError: If the loop is cancelled.
        HTTPError: If there is an HTTP error while streaming events.
        Exception: For any other exceptions that occur during the loop.
    """

    logger.info(f"{icon} Watching witness: {watch_witness}")
    last_good_event = await witness_first_run(watch_witness)
    last_good_timestamp = last_good_event.get("timestamp", "N/A")
    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    last_good_block = last_good_event.get("block_num", 0) + 1
    count = 0
    mean_time_diff = await witness_average_block_time(watch_witness)
    send_once = False
    async with MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    ) as db_client:
        while True:
            async_stream = sync_to_async_iterable(
                hive_blockchain.stream(
                    opNames=["producer_reward"],
                    start=last_good_block,
                    only_virtual_ops=True,
                    max_batch_size=MAX_HIVE_BATCH_SIZE,
                )
            )
            try:
                async for hive_event in async_stream:
                    hive_event_timestamp = hive_event.get("timestamp", "N/A")
                    seconds_since_last_block = (
                        hive_event_timestamp - last_good_timestamp
                    ).seconds
                    if (
                        not send_once
                        and seconds_since_last_block
                        > mean_time_diff.total_seconds() * HIVE_WITNESS_DELAY_FACTOR
                    ):
                        witness_details = await get_hive_witness_details(watch_witness)
                        missed_blocks = witness_details.get("missed_blocks", 0)
                        time_since_last_block = remove_ms(
                            timedelta(seconds=seconds_since_last_block)
                        )
                        block_diff = (
                            hive_event["block_num"] - last_good_event["block_num"]
                        )
                        logger.warning(
                            f"{icon} 🚨 Missed: {missed_blocks} "
                            f"Witness Time since last block: {time_since_last_block} "
                            f"Mean: {mean_time_diff} "
                            f"Block Now: {hive_event['block_num']:,.0f} "
                            f"Last Good Block: {last_good_event['block_num']:,.0f} "
                            f"Num blocks: {block_diff:,.0f}",
                            extra={
                                "notification": True,
                                "error_code": "Hive Witness delay",
                            },
                        )
                        send_once = True
                    if hive_event.get("producer") == watch_witness:
                        witness_details = await get_hive_witness_details(watch_witness)
                        missed_blocks = witness_details.get("missed_blocks", 0)
                        time_diff = remove_ms(
                            hive_event["timestamp"].replace(tzinfo=timezone.utc)
                            - last_good_timestamp
                        )
                        mean_time_diff = await witness_average_block_time(watch_witness)
                        hive_event["witness_details"] = witness_details
                        logger.info(
                            f"{icon} 🧱 "
                            f"Delta {time_diff} | "
                            f"Mean {mean_time_diff} | "
                            f"{hive_event['block_num']:,.0f} | "
                            f"Missed: {missed_blocks}",
                            extra={
                                "hive_event": hive_event,
                                "notification": True,
                                "error_code_clear": "Hive Witness delay",
                            },
                        )
                        send_once = False
                        last_good_timestamp = hive_event["timestamp"].replace(
                            tzinfo=timezone.utc
                        )
                        last_good_event = hive_event
                        try:
                            _ = await db_client.insert_one(
                                HIVE_WITNESS_PRODUCER_COLLECTION, hive_event
                            )
                        except DuplicateKeyError:
                            pass
                    count += 1
                    if count % 100 == 0:
                        hive_client.rpc.next()
            except (KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.info(f"{icon} Keyboard interrupt: Stopping event listener.")
                raise e

            except Exception as e:
                logger.exception(e)
                logger.warning(f"{icon} {e}", extra={"error": e})
                logger.warning(
                    f"{icon} last_good_block: {last_good_block:,.0f} "
                    f"rerun witness_first_run",
                    extra={"error": e},
                )
                last_good_event = await witness_first_run(watch_witness)
                last_good_block = last_good_event.get("block_num", 0) + 1


async def transactions_loop(watch_users: List[str]):
    """
    Asynchronously loops through transactions.

    This function creates an event listener for transactions, then loops through
    the transactions and logs them.
    """
    logger.info(f"{icon} Watching users: {watch_users}")
    op_names = ["transfer", "recurrent_transfer", "account_witness_vote"]

    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    last_good_block = await get_last_good_block() + 1
    count = 0
    start = timer()
    async with MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    ) as db_client:
        while True:
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
                last_trx_id = ""
                op_in_trx = 0
                async for hive_event in async_stream:
                    if hive_event.get("type") == "account_witness_vote":
                        async_publish(
                            Events.HIVE_WITNESS_VOTE,
                            hive_event=hive_event,
                            db_client=db_client,
                        )
                    if hive_event.get("type") in ["transfer", "recurrent_transfer"]:
                        # For trx_id's with multiple transfers, record position in trx
                        if hive_event.get("trx_id") == last_trx_id:
                            op_in_trx += 1
                        else:
                            last_trx_id = hive_event.get("trx_id")
                            op_in_trx = 0
                        # Only advance block count on new trx_id
                        if last_good_block < hive_event.get("block_num"):
                            last_good_block = hive_event.get("block_num")
                        hive_event["op_in_trx"] = op_in_trx
                        notification = watch_users_notification(hive_event, watch_users)
                        error_code = log_time_difference_errors(
                            hive_event["timestamp"], error_code
                        )
                        async_publish(
                            Events.HIVE_TRANSFER,
                            hive_event=hive_event,
                            db_client=db_client,
                        )
                        count += 1
                        if count % 100 == 0:
                            old_node = hive_client.rpc.url
                            hive_client.rpc.next()
                            logger.info(
                                f"{icon} {count} transactions processed. "
                                f"Node: {old_node} -> {hive_client.rpc.url}"
                            )
                        if timer() - start > 55:
                            await db_store_block_marker(hive_event, db_client)
                            start = timer()
                        if notification:
                            async_publish(
                                Events.HIVE_TRANSFER_NOTIFY,
                                hive_event=hive_event,
                                db_client=db_client,
                            )  # noqa

            except (KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.info(f"{icon} Keyboard interrupt: Stopping event listener.")
                raise e

            except Exception as e:
                logger.error(f"{icon} {e}", extra={"error": e})
                raise e


async def runner(watch_users: List[str]):
    """
    Main function to run the Hive Watcher client.
    Args:
        watch_users (List[str]): The Hive user(s) to watch for transactions.

    Returns:
        None
    """

    async with V4VAsyncRedis(decode_responses=False) as redis_cllient:
        await redis_cllient.ping()
        await redis_cllient.setex("test", 60, "test")
        logger.info(f"{icon} Redis connection established")

    try:
        async_subscribe(Events.HIVE_TRANSFER_NOTIFY, transactions_report)
        async_subscribe(Events.HIVE_TRANSFER_NOTIFY, db_store_transaction)
        async_subscribe(Events.HIVE_WITNESS_VOTE, witness_vote_report)
        async_subscribe(Events.HIVE_WITNESS_VOTE, db_store_transaction)
        tasks = [transactions_loop(watch_users), witness_loop("brianoflondon")]
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{icon} 👋 Received signal to stop. Exiting...")
        logger.info(
            f"{icon} 👋 Goodbye! from Hive Monitor", extra={"notification": True}
        )
        await asyncio.sleep(0.2)
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(
            f"{icon} Irregular shutdown in Hive Monitor {e}", extra={"error": e}
        )
        await asyncio.sleep(0.2)


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
    asyncio.run(runner(watch_users))
    INTERNAL_CONFIG.shutdown()
    print("👋 Goodbye!")


if __name__ == "__main__":

    try:
        logger.name = "hive_monitor_v2"
        app()
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.exit(0)

    except Exception as e:
        logger.exception(e)
        sys.exit(1)
