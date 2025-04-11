import asyncio
import signal
import sys
from datetime import timedelta, timezone
from timeit import default_timer as timer
from typing import Annotated, Any, List, Union

import typer
from nectar.amount import Amount
from nectar.blockchain import Blockchain

# from colorama import Fore, Style
from pymongo.errors import DuplicateKeyError

from lnd_monitor_v2 import InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.events.async_event import async_publish, async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.helpers.general_purpose_funcs import check_time_diff, seconds_only
from v4vapp_backend_v2.hive.hive_extras import MAX_HIVE_BATCH_SIZE, get_hive_client
from v4vapp_backend_v2.hive.internal_market_trade import account_trade
from v4vapp_backend_v2.hive.witness_details import get_hive_witness_details
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_all import op_any
from v4vapp_backend_v2.hive_models.op_base_counters import BlockCounter, OpInTrxCounter
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.op_types_enums import (
    MarketOpTypes,
    RealOpsLoopTypes,
    TransferOpTypes,
    VirtualOpTypes,
    WitnessOpTypes,
)
from v4vapp_backend_v2.models.hive_transfer_model import HiveTransaction

# INTERNAL_CONFIG = InternalConfig()
# CONFIG = INTERNAL_CONFIG.config
HIVE_DATABASE_CONNECTION = ""
HIVE_DATABASE = ""
HIVE_DATABASE_USER = ""
HIVE_TRX_COLLECTION_V2 = "hive_ops"
HIVE_WITNESS_PRODUCER_COLLECTION = "hive_witness_ops"
HIVE_WITNESS_DELAY_FACTOR = 1.2  # 20% over mean block time

AUTO_BALANCE_SERVER = True


COMMAND_LINE_WATCH_USERS = []
COMMAND_LINE_WATCH_ONLY = False


app = typer.Typer()
icon = "🐝"

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


def watch_users_notification(transfer: Transfer, watch_users: List[str]) -> bool:
    """
    Send notification if the user is in the watch list.
    Args:
        transfer (dict): The transaction transfer.
        watch_user (List[str]): The list of users to watch.
    Returns:
        bool: True if the user is in the watch list.
    """
    global COMMAND_LINE_WATCH_USERS
    if not watch_users:
        watch_users = COMMAND_LINE_WATCH_USERS
    if transfer.from_account in watch_users or transfer.to_account in watch_users:
        return True
    return False


async def transfer_report(transfer: Transfer, *args: Any, **kwargs: Any) -> None:
    logger.info(
        f"{icon} {transfer.log_str}",
        extra={
            "notification": True,
            "notification_str": f"{icon} {transfer.notification_str}",
            **transfer.log_extra,
        },
    )


async def witness_vote_report(
    vote: AccountWitnessVote, watch_witness: str, *args: Any, **kwargs: Any
) -> None:
    """
    Asynchronously reports witness votes.

    This function reports witness votes by logging the witness vote event.

    Args:
        hive_event (dict): The Hive witness vote event.
    """
    notification = True if vote.witness == watch_witness else False
    logger.info(
        f"{icon} {vote.log_str}",
        extra={"notification": notification, **vote.log_extra},
    )


async def market_report(
    hive_event: dict, watch_users: List[str], *args: Any, **kwargs: Any
) -> None:
    """
    Asynchronously reports market events.

    This function reports market events by logging the market event.

    Args:
        hive_event (dict): The Hive market event.
    """
    notification = True
    if (
        hive_event.get("current_owner", "") in watch_users
        or hive_event.get("open_owner", "") in watch_users
        or hive_event.get("owner", "") in watch_users
    ):
        if hive_event.get("type") == MarketOpTypes.LIMIT_ORDER_CREATE:
            market_op = LimitOrderCreate.model_validate(hive_event)
        elif hive_event.get("type") == MarketOpTypes.FILL_ORDER:
            market_op = FillOrder.model_validate(hive_event)
            if not market_op.completed_order:
                notification = False
        else:
            return
        logger.info(
            f"{market_op.log_str}",
            extra={
                "notification": notification,
                "notification_str": market_op.notification_str,
                **market_op.log_extra,
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
        block_marker = BlockMarker(
            block_num=hive_event["block_num"], timestamp=hive_event["timestamp"]
        )
        _ = await db_client.update_one(
            HIVE_TRX_COLLECTION_V2,
            query=query,
            update=block_marker.model_dump(),
            upsert=True,
        )
    except DuplicateKeyError:
        pass
    except Exception as e:
        logger.exception(e, extra={"error": e})


async def db_store_op(
    op: Union[Transfer, CustomJson],
    db_client: MongoDBClient,
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    Stores a Hive transaction in the database.

    This function processes a Hive event and stores the corresponding transaction
    in the MongoDB database. If the event type is a transfer operation, it converts
    the amount using the provided quote or fetches all quotes if none is provided.
    It then creates a HiveTransaction instance and updates the database with the
    transaction details.

    Args:
        hive_event (dict): The Hive event data.
        db_client (MongoDBClient): The MongoDB client instance.
        quote (AllQuotes, optional): The quote for currency conversion.
            Defaults to None.
        *args (Any): Additional positional arguments.
        **kwargs (Any): Additional keyword arguments.

    Returns:
        None

    Raises:
        DuplicateKeyError: If a duplicate key error occurs during the database update.
        Exception: For any other exceptions, logs the error with additional context.
    """
    global COMMAND_LINE_WATCH_USERS
    try:
        if isinstance(op, Transfer):
            op = await db_process_transfer(op)
        if not op:
            return
        # Update Database for CustomJson and Transfer
        _ = await db_client.update_one(
            HIVE_TRX_COLLECTION_V2,
            query={"trx_id": op.trx_id, "op_in_trx": op.op_in_trx},
            update=op.model_dump(by_alias=True),
            upsert=True,
        )

    except DuplicateKeyError:
        pass

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})


async def db_process_transfer(op: Transfer) -> Transfer | None:
    """
    Asynchronously processes a Hive blockchain transfer operation.
    This function handles notifications for watched users, updates exchange rate quotes if necessary,
    generates a transfer report, and triggers server balance adjustments based on specific conditions.
    Args:
        op (Transfer): The transfer operation to process.
    Returns:
        Transfer | None: The processed transfer object if conditions are met, otherwise None.
    Behavior:
    - Checks if the transfer involves watched users and sends notifications if applicable.
    - Updates the exchange rate quotes if the last quote is older than 60 seconds.
    - Logs the updated quotes for reference.
    - Generates a transfer report for the given operation.
    - Initiates server balance adjustments if the transfer involves specific server and treasury accounts.
    """
    global COMMAND_LINE_WATCH_USERS
    CONFIG = InternalConfig().config
    if watch_users_notification(transfer=op, watch_users=COMMAND_LINE_WATCH_USERS):
        if not Transfer.last_quote or (Transfer.last_quote and Transfer.last_quote.age > 60):
            await Transfer.update_quote()
            quote = Transfer.last_quote
            logger.info(
                f"{icon} Updating Quotes: {quote.hive_usd} {quote.sats_hive}",
                extra={
                    "notification": False,
                    "quote": HiveTransaction.last_quote.model_dump(exclude={"raw_response"}),
                },
            )
        await transfer_report(op)
        if AUTO_BALANCE_SERVER and (
            (
                op.from_account in CONFIG.hive.server_account_names
                and op.to_account not in CONFIG.hive.treasury_account_names
            )
            or (
                op.to_account in CONFIG.hive.server_account_names
                and op.from_account in CONFIG.hive.treasury_account_names
            )
        ):
            asyncio.create_task(balance_server_hbd_level(op))
        return op
    return None


async def balance_server_hbd_level(transfer: Transfer) -> None:
    """
    This function identifies the relevant Hive account from the provided transfer
    object and attempts to balance its HBD level by initiating a conversion transaction.
    The function ensures that the account has an active key before proceeding. If the
    conversion is successful, the transaction ID is logged. In case of an error, the
    error is logged.

        transfer (Transfer): The Hive transaction containing the account information
        to be balanced.

    Returns:
        None: The function does not return any value.
    """
    CONFIG = InternalConfig().config
    await asyncio.sleep(3)  # Sleeps to make sure we only balance HBD after time for a return
    try:
        if transfer.from_account in CONFIG.hive.server_account_names:
            use_account = transfer.from_account
        elif transfer.to_account in CONFIG.hive.server_account_names:
            use_account = transfer.to_account
        else:
            return
        hive_acc = CONFIG.hive.hive_accs.get(use_account, None)
        if hive_acc and hive_acc.active_key:
            # set the amount to the current HBD balance taken from Config
            set_amount_to = Amount(hive_acc.hbd_balance)
            nobroadcast = True if COMMAND_LINE_WATCH_ONLY else False
            trx = account_trade(
                hive_acc=hive_acc, set_amount_to=set_amount_to, nobroadcast=nobroadcast
            )
            if trx:
                logger.info(f"Transaction broadcast: {trx.get('trx_id')}", extra={"trx": trx})
    except Exception as e:
        logger.error(
            f"{icon} Error in {__name__}: {e}",
            extra={"notification": False, "error": e},
        )


async def db_store_witness_vote(
    vote: AccountWitnessVote,
    db_client: MongoDBClient,
    watch_witness: str = "",
    *args: Any,
    **kwargs: Any,
) -> None:
    """
    Asynchronously stores a witness vote in the database.

    This function processes a witness vote and updates the corresponding record
    in the database. If the record does not exist, it inserts a new one. The
    function handles duplicate key errors gracefully and logs any other exceptions.

    Args:
        vote (AccountWitnessVote): The witness vote object containing the details
            of the vote, including transaction ID and operation index.
        db_client (MongoDBClient): The database client used to interact with the
            MongoDB database.
        *args (Any): Additional positional arguments.
        **kwargs (Any): Additional keyword arguments.

    Raises:
        Exception: Logs any unexpected exceptions that occur during the database
            operation.
    """
    try:
        trx_id = vote.trx_id
        op_in_trx = vote.op_in_trx
        query = {"trx_id": trx_id, "op_in_trx": op_in_trx}
        if vote.type == "account_witness_vote" and vote.witness == watch_witness:
            _ = await db_client.update_one(
                HIVE_TRX_COLLECTION_V2,
                query=query,
                update=vote.model_dump(),
                upsert=True,
            )
    except DuplicateKeyError:
        pass

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})


async def get_last_good_block(collection: str = HIVE_TRX_COLLECTION_V2) -> int:
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


async def witness_first_run(watch_witness: str) -> ProducerReward | None:
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
            producer_reward = ProducerReward.model_validate(last_good_event)
            await producer_reward.get_witness_details()
            time_diff = check_time_diff(producer_reward.timestamp)
            logger.info(
                f"{icon} Last recorded witness producer block: "
                f"{producer_reward.block_num:,} "
                f"for {producer_reward.producer} "
                f"{producer_reward.timestamp} "
                f"{time_diff}",
                extra={"notification": True, **producer_reward.log_extra},
            )
            return producer_reward

        # Empty database
        hive_client = get_hive_client()
        hive_blockchain = Blockchain(hive=hive_client)
        end_block = hive_client.get_dynamic_global_properties().get("head_block_number")
        op_in_trx_counter = OpInTrxCounter(realm="virtual")
        async_stream = sync_to_async_iterable(
            hive_blockchain.stream(
                opNames=["producer_reward"],
                start=end_block - int(24 * 60 * 60 / 3),  # go back 24 hours of 3 second blocks
                stop=end_block,
                only_virtual_ops=True,
                max_batch_size=MAX_HIVE_BATCH_SIZE,
            )
        )
        async for hive_event in async_stream:
            if hive_event.get("producer") == watch_witness:
                hive_event["op_in_trx"] = op_in_trx_counter.inc(hive_event["trx_id"])
                producer_reward = ProducerReward.model_validate(hive_event)
                await producer_reward.get_witness_details()
                _ = await db_client.insert_one(
                    HIVE_WITNESS_PRODUCER_COLLECTION, producer_reward.model_dump()
                )
                logger.info(
                    f"{icon} {producer_reward.witness} block: {producer_reward.block_num:,} ",
                    extra={"notification": False, **producer_reward.log_extra},
                )
        if producer_reward:
            return producer_reward
    return None


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
    mean_time_diff = seconds_only(timedelta(seconds=mean_time_diff_seconds))

    return mean_time_diff


async def virtual_ops_loop(watch_witness: str, watch_users: List[str] = []):
    """
    Asynchronously loops through witnesses.

    This is looking at VIRTUAL OPS.

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
    op_names = VirtualOpTypes
    logger.info(f"{icon} Virtual Loop Watching witness: {watch_witness}")
    producer_reward = await witness_first_run(watch_witness)
    last_good_timestamp = producer_reward.timestamp
    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    last_good_block = await get_last_good_block() + 1
    mean_time_diff = await witness_average_block_time(watch_witness)
    send_once = False

    op_in_trx_counter = OpInTrxCounter(realm="virtual")
    block_counter = BlockCounter(
        last_good_block=last_good_block, hive_client=hive_client, id="virtual"
    )

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
                    only_virtual_ops=True,
                    max_batch_size=MAX_HIVE_BATCH_SIZE,
                )
            )
            logger.info(f"{icon} Virtual Loop Watching witness: {watch_witness}")
            try:
                async for hive_event in async_stream:
                    if shutdown_event.is_set():
                        raise asyncio.CancelledError("Docker Shutdown")
                    hive_event["op_in_trx"] = op_in_trx_counter.inc(hive_event["trx_id"])
                    new_block, marker = block_counter.inc(hive_event)
                    # Allow switch to other block loop
                    if new_block:
                        await asyncio.sleep(0.01)

                    hive_event_timestamp = hive_event.get("timestamp", "1970-01-01T00:00:00+00:00")
                    seconds_since_last_block = (hive_event_timestamp - last_good_timestamp).seconds
                    if (
                        not send_once
                        and seconds_since_last_block
                        > mean_time_diff.total_seconds() * HIVE_WITNESS_DELAY_FACTOR
                    ):
                        wd = await get_hive_witness_details(watch_witness)
                        witness = wd.witness
                        time_since_last_block = seconds_only(
                            timedelta(seconds=seconds_since_last_block)
                        )
                        block_diff = hive_event["block_num"] - witness.last_confirmed_block_num
                        logger.warning(
                            f"{icon} 🚨 Missed: {witness.missed_blocks} "
                            f"Witness Time since last block: {time_since_last_block} "
                            f"Mean: {mean_time_diff} "
                            f"Block Now: {hive_event['block_num']:,.0f} "
                            f"Last Good Block: {witness.last_confirmed_block_num:,} "
                            f"Num blocks: {block_diff:,} | "
                            f"{check_time_diff(hive_event_timestamp)}",
                            extra={
                                "notification": True,
                                "error_code": "Hive Witness delay",
                            },
                        )
                        send_once = True
                    if (
                        hive_event.get("type") == "producer_reward"
                        and hive_event.get("producer") == watch_witness
                    ):
                        producer_reward = ProducerReward.model_validate(hive_event)
                        await producer_reward.get_witness_details()
                        if not producer_reward.witness:
                            logger.error(
                                f"{icon} No witness found for {watch_witness}",
                                extra={"notification": True},
                            )
                            continue
                        time_diff = seconds_only(
                            hive_event["timestamp"].replace(tzinfo=timezone.utc)
                            - last_good_timestamp
                        )
                        mean_time_diff = await witness_average_block_time(watch_witness)
                        logger.info(
                            f"{icon} 🧱 "
                            f"Delta {time_diff} | "
                            f"Mean {mean_time_diff} | "
                            f"{producer_reward.log_str} | "
                            f"{check_time_diff(producer_reward.timestamp)}",
                            extra={
                                "notification": True,
                                "error_code_clear": "Hive Witness delay",
                                **producer_reward.log_extra,
                            },
                        )
                        send_once = False
                        last_good_timestamp = hive_event["timestamp"].replace(tzinfo=timezone.utc)
                        last_good_event = hive_event
                        try:
                            _ = await db_client.insert_one(
                                HIVE_WITNESS_PRODUCER_COLLECTION,
                                producer_reward.model_dump(),
                            )
                        except DuplicateKeyError:
                            pass
                    if hive_event.get("type") in MarketOpTypes:
                        asyncio.create_task(slow_publish_fill_event(hive_event, watch_users))
                        last_good_event = hive_event

            except (KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.info(
                    f"{icon} Keyboard interrupt or Cancelled: Stopping event listener. {e}"
                )
                return

            except Exception as e:
                logger.exception(e)
                logger.warning(f"{icon} {e}", extra={"error": e})
                logger.warning(
                    f"{icon} last_good_block: {last_good_block:,.0f} rerun witness_first_run",
                    extra={"error": e},
                )

            finally:
                producer_reward = await witness_first_run(watch_witness)
                last_good_block = last_good_event.get("block_num", 0) + 1


async def slow_publish_fill_event(hive_event: dict, watch_users: List[str]):
    """
    Because fill events arrive before the limit_order_create events, will wait before
    sending them to the event queue.
    """
    await asyncio.sleep(3)
    async_publish(Events.HIVE_MARKET, hive_event=hive_event, watch_users=watch_users)


async def real_ops_loop(
    watch_witness: str = "", watch_users: List[str] = COMMAND_LINE_WATCH_USERS
):
    """
    Asynchronously loops through transactions and processes them.

    This function sets up an event listener for specific transaction types on the Hive
    blockchain, processes each transaction, logs relevant information, and publishes
    events for further handling. It also periodically updates cryptocurrency quotes and
    stores block markers in a database.

    Uses Ops from:

    Args:
        watch_users (List[str]): A list of user accounts to monitor for transactions.

    Raises:
        KeyboardInterrupt: If the process is interrupted by a keyboard signal.
        asyncio.CancelledError: If the asyncio task is cancelled.
        Exception: For any other exceptions that occur during processing.

    Logs:
        Information about the transactions being processed, including the number of
        transactions, node changes, and cryptocurrency quotes.

    Publishes:
        Events.HIVE_WITNESS_VOTE: When an account witness vote transaction is detected.
        Events.HIVE_TRANSFER: When a transfer or recurrent transfer transaction is
        detected.
        Events.HIVE_TRANSFER_NOTIFY: When a transfer or recurrent transfer transaction
        involving a watched user is detected.
    """
    CONFIG = InternalConfig().config
    logger.info(f"{icon} Real Loop Watching users: {watch_users}")
    LimitOrderCreate.watch_users = watch_users
    op_names = RealOpsLoopTypes
    hive_client = get_hive_client()
    hive_blockchain = Blockchain(hive=hive_client)
    last_good_block = await get_last_good_block() + 1
    start = timer()
    await Transfer.update_quote()

    op_in_trx_counter = OpInTrxCounter(realm="real")
    block_counter = BlockCounter(
        last_good_block=last_good_block, hive_client=hive_client, id="real"
    )
    async with MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    ) as db_client:
        while True:
            logger.info(f"{icon} Real Loop")
            async_stream = sync_to_async_iterable(
                hive_blockchain.stream(
                    opNames=op_names,
                    start=last_good_block,
                    raw_ops=False,
                    max_batch_size=MAX_HIVE_BATCH_SIZE,
                )
            )
            try:
                async for hive_event in async_stream:
                    if shutdown_event.is_set():
                        raise asyncio.CancelledError("Docker Shutdown")
                    # For trx_id's with multiple transfers, record position in trx
                    # Moved outside the specific blocks for different op codes
                    hive_event["op_in_trx"] = op_in_trx_counter.inc(hive_event["trx_id"])
                    try:
                        new_block, marker = block_counter.inc(hive_event)
                        # Allow switch to other block loop
                        if new_block:
                            await asyncio.sleep(0.01)

                        op = op_any(hive_event)
                        if not op:
                            continue
                    except ValueError:
                        # Not one of the ops we want to track
                        continue
                    except Exception as ex:
                        print(ex)

                    if op.type in WitnessOpTypes:
                        vote = AccountWitnessVote.model_validate(hive_event)
                        vote = op
                        vote.get_voter_details()
                        async_publish(
                            Events.HIVE_WITNESS_VOTE,
                            vote=vote,
                            watch_witness=watch_witness,
                            db_client=db_client,
                        )
                    if op.type in TransferOpTypes:
                        # Only advance block count on new trx_id
                        hive_inst = get_hive_client(keys=CONFIG.hive.memo_keys)
                        hive_event["hive_inst"] = hive_inst
                        transfer = Transfer.model_validate(hive_event)
                        # Log ever transaction (even if not in watch list)
                        logger.debug(
                            f"{icon} {transfer.log_str}",
                            extra={"notification": False, **transfer.log_extra},
                        )

                        async_publish(
                            Events.HIVE_TRANSFER,
                            op=transfer,
                            watch_users=watch_users,
                            db_client=db_client,
                        )

                    if op.type == "custom_json":
                        custom_json: CustomJson = op
                        logger.info(
                            f"{custom_json.log_str}",
                            extra={"notification": True, **custom_json.log_extra},
                        )
                        async_publish(
                            Events.HIVE_TRANSFER,
                            op=custom_json,
                            watch_users=watch_users,
                            db_client=db_client,
                        )
                    if hive_event.get("type") in MarketOpTypes:
                        async_publish(
                            Events.HIVE_MARKET,
                            hive_event=hive_event,
                            watch_users=watch_users,
                        )
                    if timer() - start > 55:
                        # TODO: #48 move the BlockMarker object creation to here.
                        await db_store_block_marker(hive_event, db_client)
                        start = timer()
            except (KeyboardInterrupt, asyncio.CancelledError) as e:
                logger.info(f"{icon} {e}: Stopping event listener.")
                raise e

            except Exception as e:
                logger.exception(f"{icon} {e}", extra={"error": e})
                raise e

            finally:
                logger.warning(
                    f"{icon} Restarting real_ops_loop after error from {hive_client.rpc.url}",
                )
                hive_client.rpc.next()


async def main_async_start(watch_users: List[str], watch_witness: str) -> None:
    """
    Main function to run the Hive Watcher client.
    Args:
        watch_users (List[str]): The Hive user(s) to watch for transactions.

    Returns:
        None
    """
    loop = asyncio.get_running_loop()

    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

    logger.info(f"{icon} Main Loop: {loop._thread_id}")
    async with V4VAsyncRedis(decode_responses=False) as redis_client:
        try:
            await redis_client.ping()
        except Exception as e:
            logger.error(f"{icon} Redis connection test failed", extra={"error": e})
            raise e
        logger.info(f"{icon} Redis connection established")

    await HiveTransaction.update_quote()
    quote = HiveTransaction.last_quote
    logger.info(
        f"{icon} Updating Quotes: {quote.hive_usd} {quote.sats_hive}",
        extra={
            "notification": False,
            "quote": HiveTransaction.last_quote.model_dump(exclude={"raw_response"}),
        },
    )

    try:
        async_subscribe(Events.HIVE_TRANSFER, db_store_op)
        async_subscribe(Events.HIVE_WITNESS_VOTE, witness_vote_report)
        async_subscribe(Events.HIVE_WITNESS_VOTE, db_store_witness_vote)

        async_subscribe(Events.HIVE_MARKET, market_report)
        tasks = [
            real_ops_loop(watch_witness=watch_witness, watch_users=watch_users),
            virtual_ops_loop(watch_witness=watch_witness, watch_users=watch_users),
        ]
        await asyncio.gather(*tasks)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{icon} 👋 Received signal to stop. Exiting...")
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{icon} Irregular shutdown in Hive Monitor {e}", extra={"error": e})
        raise e
    finally:
        # Cancel all tasks except the current one
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{icon} 👋 Goodbye! from Hive Monitor", extra={"notification": True})
        logger.info(f"{icon} Clearing notifications")
        await asyncio.sleep(2)


@app.command()
def main(
    watch_users: Annotated[
        List[str],
        typer.Option(
            "--user",
            help="Hive User(s) to watch for transactions, can have multiple",
            show_default=True,
        ),
    ],
    watch_only: Annotated[
        bool,
        typer.Option(
            "--watch-only",
            help="Watch only mode, uses `nobroadcast` option for Hive so HBD will not be sold",
        ),
    ] = False,
    watch_witness: Annotated[
        str,
        typer.Option(
            "--witness",
            help="Hive Witness to watch for transactions",
            show_default=True,
        ),
    ] = "brianoflondon",
    database: Annotated[
        str,
        typer.Argument(help=("The database to monitor.")),
    ] = "",
    database_connection: Annotated[
        str,
        typer.Argument(help=("The database connection to use.")),
    ] = "",
    database_user: Annotated[
        str,
        typer.Argument(help=("The database user to use.")),
    ] = "",
):
    """
    Watch the Hive blockchain for transactions.

    Args:
        watch_users: The Hive user(s) to watch for transactions.
        Specify multiple users with repeated --watch-users, e.g.,
        --watch-users alice --watch-users bob.
        watch_witness: The Hive witness to watch for transactions.
        Defaults to "brianoflondon".

    Returns:
        None
    """
    CONFIG = InternalConfig().config
    global COMMAND_LINE_WATCH_USERS
    global COMMAND_LINE_WATCH_ONLY
    global HIVE_DATABASE
    global HIVE_DATABASE_CONNECTION
    global HIVE_DATABASE_USER

    if not database:
        HIVE_DATABASE = CONFIG.default_db_name
    if not database_connection:
        HIVE_DATABASE_CONNECTION = CONFIG.default_db_connection
    if not database_user:
        HIVE_DATABASE_USER = CONFIG.default_db_user

    logger.info(
        f"{icon} ✅ Hive Monitor v2: {icon}. Version: {CONFIG.version}",
        extra={"notification": True},
    )
    if not watch_users:
        watch_users = ["v4vapp", "brianoflondon"]
    COMMAND_LINE_WATCH_USERS = watch_users
    COMMAND_LINE_WATCH_ONLY = watch_only
    asyncio.run(main_async_start(watch_users, watch_witness))


if __name__ == "__main__":
    try:
        logger.name = "hive_monitor_v2"
        app()
        print("👋 Goodbye!")
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.exit(0)

    except Exception as e:
        print(e)
        sys.exit(1)
