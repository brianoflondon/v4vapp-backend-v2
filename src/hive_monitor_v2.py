import asyncio
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import Annotated, Any, Dict, List, Tuple

import typer
from nectar.amount import Amount

# from colorama import Fore, Style
from pymongo.errors import DuplicateKeyError
from pymongo.results import BulkWriteResult, UpdateResult

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import check_time_diff, seconds_only
from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive.internal_market_trade import account_trade
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_account_update2 import AccountUpdate2
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_all import OpAny, is_op_all_transfer
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_base_counters import BlockCounter
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes
from v4vapp_backend_v2.hive_models.stream_ops import stream_ops_async

HIVE_DATABASE_CONNECTION = ""
HIVE_DATABASE = ""
HIVE_DATABASE_USER = ""
HIVE_OPS_COLLECTION = "hive_ops"
HIVE_WITNESS_DELAY_FACTOR = 1.2  # 20% over mean block time

AUTO_BALANCE_SERVER = True


COMMAND_LINE_WATCH_USERS: List[str] = []
COMMAND_LINE_WATCH_ONLY = False


app = typer.Typer()
icon = "🐝"

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()


BLOCK_LIST = [
    "95793083",
    "95801581",
    "95802587",
    "95817721",
    "95819345",
    "95819821",
    "95819830",
    "95822146",
    "95822927",
    "95823857",
]


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def db_store_op(
    op: OpAny,
    db_collection: str | None = None,
    *args: Any,
    **kwargs: Any,
) -> List[BulkWriteResult | None] | UpdateResult:
    """
    Stores a Hive transaction in the database.

    This function processes a Hive event and stores the corresponding transaction
    in the MongoDB database. If the event type is a transfer operation, it converts
    the amount using the provided quote or fetches all quotes if none is provided.
    It then creates a HiveTransaction instance and updates the database with the
    transaction details.

    Args:
        op (OpAny): The Hive event to process. Can be a
            Transfer or CustomJson operation.
        db_collection (str | None): The name of the MongoDB collection to use for
            storing the transaction. If None, the default collection will be used.
        *args (Any): Additional positional arguments.
        **kwargs (Any): Additional keyword arguments.

    Returns:
        UpdateResult: The result of the database update operation.

    Raises:
        DuplicateKeyError: If a duplicate key error occurs during the database update.
        Exception: For any other exceptions, logs the error with additional context.
    """
    global COMMAND_LINE_WATCH_USERS, HIVE_DATABASE_CONNECTION, HIVE_DATABASE, HIVE_DATABASE_USER
    db_collection = HIVE_OPS_COLLECTION if not db_collection else db_collection

    try:
        collection = OpBase.collection()
        db_ans = await collection.update_one(
            filter=op.group_id_query,
            update={"$set": op.model_dump(by_alias=True, exclude_none=True, exclude_unset=True)},
            upsert=True,
        )
        return db_ans
    except DuplicateKeyError as e:
        logger.info(
            f"DuplicateKeyError: {op.block_num} {op.trx_id} {op.op_in_trx}",
            extra={"notification": False, "error": e, **op.log_extra},
        )
        return []

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False, **op.log_extra})
        return []


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
    logger.info("Waiting for 30 seconds to re-balance HBD level")
    await asyncio.sleep(30)  # Sleeps to make sure we only balance HBD after time for a return
    use_account = None
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
    except ValueError as ve:
        logger.error(
            f"{icon} ValueError in {__name__}: {ve} Maybe misconfigured account? No hbd_balance set?",
            extra={"notification": False, "error": ve},
        )
        if use_account:
            logger.error(
                f"{icon} Account {use_account} miss config, "
                f"please check your config file {DEFAULT_CONFIG_FILENAME}",
                extra={"notification": False, "error": ve},
            )

    except Exception as e:
        logger.exception(
            f"{icon} Error in {__name__}: {e}",
            extra={"notification": False, "error": e},
        )


async def get_last_good_block(collection: str = HIVE_OPS_COLLECTION) -> int:
    """
    Asynchronously retrieves the last good block.

    This function retrieves the last good block by getting the dynamic global properties
    from the Hive client and returning the head block number minus 30.

    Returns:
        int: The last good block.
    """
    try:
        ans = await OpBase.collection().find_one(filter={}, sort=[("block_num", -1)])
        if ans and "block_num" in ans:
            time_diff = check_time_diff(ans["timestamp"])
            logger.info(
                f"{icon} Last good block: {ans['block_num']:,} {ans['timestamp']} {time_diff} ago",
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
    last_good_event = await OpBase.collection().find_one(
        filter={"producer": watch_witness},
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
            f"{time_diff} "
            f"{producer_reward.log_str}",
            extra={"notification": True, **producer_reward.log_extra},
        )
        return producer_reward

    # Empty database
    look_back = timedelta(hours=3)
    async for op in stream_ops_async(
        opNames=["producer_reward"], look_back=look_back, stop_now=True
    ):
        if not isinstance(op, ProducerReward):
            continue
        if op.producer == watch_witness:
            await op.get_witness_details()
            op.mean, last_witness_timestamp = await witness_average_block_time(watch_witness)
            op.delta = op.timestamp - last_witness_timestamp
            _ = await OpBase.collection().insert_one(
                op.model_dump(),
            )
            logger.info(
                f"{icon} {op.log_str}",
                extra={
                    "notification": False,
                    **op.log_extra,
                },
            )
    if op:
        return op
    return None


async def witness_average_block_time(watch_witness: str) -> Tuple[timedelta, datetime]:
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
    count_back = 10
    cursor = OpBase.collection().find(
        filter={"producer": watch_witness},
        sort=[("block_num", -1)],
    )
    # loop through the blocks and calculate the average block time
    block_timestamps: List[datetime] = []
    counter = 0
    async for block in cursor:
        block_timestamps.append((block["timestamp"]))
        counter += 1
        if counter > count_back:
            break

    # Calculate the time differences between consecutive timestamps
    time_differences = [
        (block_timestamps[i - 1] - block_timestamps[i]).total_seconds()
        for i in range(1, len(block_timestamps))
    ]
    # Calculate the mean time difference
    try:
        mean_time_diff_seconds = sum(time_differences) / len(time_differences)
    except ZeroDivisionError:
        logger.info(
            f"{icon} No time differences found for witness {watch_witness}",
            extra={"notification": True},
        )
        return timedelta(seconds=0), datetime.now(tz=timezone.utc) - timedelta(days=1)

    # Convert the mean time difference back to a timedelta object
    mean_time_diff = seconds_only(timedelta(seconds=mean_time_diff_seconds))

    return mean_time_diff, block_timestamps[0]


async def all_ops_loop(
    watch_witnesses: List[str] = [], watch_users: List[str] = [], start_block: int = 0
) -> None:
    """
    Asynchronously loops through transactions and processes them.

    This function sets up an event listener for specific transaction types on the Hive
    blockchain, processes each transaction, logs relevant information, and publishes
    events for further handling. It also periodically updates cryptocurrency quotes and
    stores block markers in a database.

    Args:
        watch_witnesses (List[str]): A list of witness accounts to monitor for transactions.
        watch_users (List[str]): A list of user accounts to monitor for transactions.

    Raises:
        KeyboardInterrupt: If the process is interrupted by a keyboard signal.
        asyncio.CancelledError: If the asyncio task is cancelled.
        Exception: For any other exceptions that occur during processing.
    """
    logger.info(
        f"{icon} Combined Loop Watching users: {watch_users} and witnesses {watch_witnesses}"
    )
    OpBase.watch_users = watch_users
    OpBase.proposals_tracked = InternalConfig().config.hive.proposals_tracked
    OpBase.custom_json_ids_tracked = InternalConfig().config.hive.custom_json_ids_tracked
    server_accounts = InternalConfig().config.hive.server_account_names
    if server_accounts:
        v4v_config = V4VConfig(server_accname=server_accounts[0])
    else:
        v4v_config = V4VConfig(server_accname="")
    async with asyncio.TaskGroup() as tg:
        for witness in watch_witnesses:
            tg.create_task(witness_first_run(witness))

    hive_client = get_hive_client(keys=InternalConfig().config.hive.memo_keys)
    if start_block == 0:
        last_good_block = await get_last_good_block() + 1
    elif start_block == -1:
        global_properties: Dict = hive_client.get_dynamic_global_properties()  # type: ignore
        last_good_block = global_properties.get("head_block_number", 97112440)
    else:
        last_good_block = start_block
    block_counter = BlockCounter(
        last_good_block=last_good_block, hive_client=hive_client, id="combined"
    )
    start = timer()
    while True:
        try:
            async for op in stream_ops_async(
                opNames=OpBase.op_tracked, start=last_good_block, stop_now=False, hive=hive_client
            ):
                notification = False
                log_it = False
                extra_bots: List[str] = []
                db_store = False
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Docker Shutdown")
                new_block, marker = block_counter.inc(op.raw_op)

                if watch_witnesses and isinstance(op, AccountWitnessVote):
                    op.get_voter_details()
                    log_it = True
                    if op.witness in watch_witnesses:
                        asyncio.create_task(db_store_op(op))
                        notification = True
                        db_store = True

                elif is_op_all_transfer(op):
                    if op.is_watched:
                        await TrackedBaseModel.update_quote()
                        await op.update_conv()
                        if not COMMAND_LINE_WATCH_ONLY:
                            asyncio.create_task(balance_server_hbd_level(op))
                        log_it = True
                        db_store = True
                        notification = True

                elif op.known_custom_json:
                    notification = True
                    if not op.conv:
                        await op.update_quote_conv()
                    log_it = True
                    db_store = True

                elif (
                    isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder)
                ) and op.is_watched:
                    await TrackedBaseModel.update_quote()
                    await op.update_conv()
                    notification = (
                        False if isinstance(op, FillOrder) and not op.completed_order else True
                    )
                    log_it = True
                    db_store = True

                elif isinstance(op, ProducerReward):
                    if op.producer in watch_witnesses:
                        notification = True
                        await op.get_witness_details()
                        op.mean, last_witness_timestamp = await witness_average_block_time(
                            op.producer
                        )
                        op.delta = abs(op.timestamp - last_witness_timestamp)
                        log_it = True
                        db_store = True

                elif OpBase.proposals_tracked and isinstance(op, UpdateProposalVotes):
                    op.get_voter_details()
                    log_it = True
                    if op.is_tracked:
                        notification = True
                        db_store = True

                elif isinstance(op, AccountUpdate2):
                    if op.is_watched:
                        log_it = True
                        notification = True
                        db_store = True
                        if v4v_config.server_accname == op.account:
                            v4v_config.fetch()
                else:
                    # If the op is not in the list of tracked ops, skip it
                    continue

                await combined_logging(op, log_it, notification, db_store, extra_bots)

                if timer() - start > 55:
                    block_marker = BlockMarker(op.block_num, op.timestamp)
                    await db_store_op(block_marker)
                    start = timer()

        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"{icon} {e}: Stopping event listener.")
            raise e

        except Exception as e:
            logger.exception(f"{icon} {e}", extra={"notification": False})
            raise e

        finally:
            logger.warning(
                f"{icon} Restarting real_ops_loop after error from {hive_client.rpc.url} no_preview",
                extra={"notification": False},
            )
            if hive_client.rpc:
                hive_client.rpc.next()
            else:
                logger.error(
                    f"{icon} Hive client not available, re-fetching new hive-client",
                    extra={"notification": False},
                )
                hive_client = get_hive_client(keys=InternalConfig().config.hive.memo_keys)


async def combined_logging(
    op: OpAny, log_it: bool, notification: bool, db_store: bool, extra_bots: List[str] | None
) -> None:
    """
    Asynchronously logs and stores events.

    This function handles the logging and storage of events based on the provided
    parameters. It can log to a file, store in a database, or send notifications
    based on the event type.

    Args:
        log_it (bool): Flag indicating whether to log the event.
        db_store (bool): Flag indicating whether to store the event in the database.
        extra_bots (List[str] | None): List of additional bot names for notifications.

    Returns:
        None
    """

    if db_store:
        asyncio.create_task(db_store_op(op))

    if log_it:
        log_extras = {
            "notification": notification,
            "notification_str": f"{icon} {op.notification_str}",
            **op.log_extra,
        }
        # Only send extra notifications if the bot is not in watch-only mode
        # so we don't double notify.
        if extra_bots:
            log_extras["extra_bot_names"] = extra_bots
        logger.info(f"{icon} {op.log_str}", extra=log_extras)


async def store_rates() -> None:
    """
    Asynchronously stores cryptocurrency rates in the database every 10 minutes.

    This function retrieves the latest cryptocurrency rates and stores them in the database.
    It wakes up every 10 minutes, but will exit promptly if a shutdown or keyboard event is triggered.

    Returns:
        None
    """
    try:
        while not shutdown_event.is_set():
            try:
                await TrackedBaseModel.update_quote()
                quote = TrackedBaseModel.last_quote
                logger.info(
                    f"{icon} Updating Quotes: {quote.hive_usd} {quote.sats_hive:.0f} fetch date {quote.fetch_date}",
                    extra={
                        "notification": False,
                        "quote": TrackedBaseModel.last_quote.model_dump(
                            exclude={"raw_response", "raw_op"}
                        ),
                    },
                )
            except Exception as e:
                logger.error(
                    f"{icon} Error storing rates: {e}", extra={"error": e, "notification": False}
                )
            # Wait for up to 10 minutes, but wake up early if shutdown_event is set
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=600)
            except asyncio.TimeoutError:
                continue  # Timeout means 10 minutes passed, so loop again
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info(f"{icon} store_rates cancelled or interrupted, exiting.")
        return


async def main_async_start(
    watch_users: List[str], watch_witnesses: List[str], start_block: int
) -> None:
    """
    Main function to run the Hive Watcher client.
    Args:
        watch_users (List[str]): The Hive user(s) to watch for transactions.
        watch_witnesses (List[str]): The Hive witness(es) to watch for transactions.
        start_block (int): The block number to start processing from.

    Returns:
        None
    """
    db_conn = DBConn()
    await db_conn.setup_database()

    loop = asyncio.get_running_loop()

    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

    logger.info(f"{icon} Main Loop running in thread: {threading.get_ident()}")
    async with V4VAsyncRedis(decode_responses=False) as redis_client:
        try:
            await redis_client.ping()
        except Exception as e:
            logger.error(f"{icon} Redis connection test failed", extra={})
            raise e
        logger.info(f"{icon} Redis connection established")

    try:
        tasks = [
            all_ops_loop(
                watch_witnesses=watch_witnesses, watch_users=watch_users, start_block=start_block
            ),
            store_rates(),
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
    ] = [],
    watch_only: Annotated[
        bool,
        typer.Option(
            "--watch-only",
            help="Watch only mode, uses `nobroadcast` option for Hive so HBD will not be sold",
            show_default=True,
        ),
    ] = False,
    watch_witnesses: Annotated[
        List[str],
        typer.Option(
            "--witness",
            help="Hive Witness(es) to watch for transactions",
            show_default=True,
        ),
    ] = [],
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
    start_block: Annotated[
        int,
        typer.Option(
            "--start-block",
            help="""The block number to start from. 0 will start from the last good block,
            -1 will start from the current head block.""",
            show_default=True,
        ),
    ] = 0,
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
    CONFIG = InternalConfig(
        config_filename=config_filename, log_filename="hive_monitor_v2.log.jsonl"
    ).config
    global COMMAND_LINE_WATCH_ONLY
    global HIVE_DATABASE
    global HIVE_DATABASE_CONNECTION
    global HIVE_DATABASE_USER

    if not database_connection:
        HIVE_DATABASE_CONNECTION = CONFIG.dbs_config.default_connection
    if not database:
        HIVE_DATABASE = CONFIG.dbs_config.default_name
    if not database_user:
        HIVE_DATABASE_USER = CONFIG.dbs_config.default_user
    # TODO: This is redundant, remove it no setting database here any more

    logger.info(
        f"{icon} ✅ Hive Monitor v2: {icon}. Version: {__version__}",
        extra={"notification": True},
    )
    if not watch_users:
        watch_users = CONFIG.hive.watch_users
    if not watch_witnesses:
        watch_witnesses = CONFIG.hive.watch_witnesses
    COMMAND_LINE_WATCH_ONLY = watch_only
    asyncio.run(main_async_start(watch_users, watch_witnesses, start_block=start_block))


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
