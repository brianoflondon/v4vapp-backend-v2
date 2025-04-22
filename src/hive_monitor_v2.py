import asyncio
import signal
import sys
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import Annotated, Any, List, Tuple

import typer
from nectar.amount import Amount

# from colorama import Fore, Style
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.general_purpose_funcs import check_time_diff, seconds_only
from v4vapp_backend_v2.hive.hive_extras import get_hive_client
from v4vapp_backend_v2.hive.internal_market_trade import account_trade
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm
from v4vapp_backend_v2.hive_models.op_base_counters import BlockCounter
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
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


async def db_store_op(
    op: OpAny,
    db_collection: str | None = None,
    *args: Any,
    **kwargs: Any,
) -> UpdateResult | None:
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
        db_client (MongoDBClient | None): The MongoDB client instance to use for
            database operations. If None, a new client will be created.
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
        async with OpBase.db_client as db_client:
            if op.realm == OpRealm.MARKER:
                query = {"realm": OpRealm.MARKER, "trx_id": op.trx_id}
            else:
                query = {"trx_id": op.trx_id, "op_in_trx": op.op_in_trx, "block_num": op.block_num}
            db_ans = await db_client.update_one(
                db_collection,
                query=query,
                update=op.model_dump(
                    by_alias=True,
                ),
                upsert=True,
            )
            return db_ans
    except DuplicateKeyError as e:
        logger.info(
            f"DuplicateKeyError: {op.block_num} {op.trx_id} {op.op_in_trx}",
            extra={"notification": False, "error": e, **op.log_extra},
        )
        return None

    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False, **op.log_extra})
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
    logger.info("Waiting for 30 seconds to re-balance HBD level")
    await asyncio.sleep(30)  # Sleeps to make sure we only balance HBD after time for a return
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


async def get_last_good_block(collection: str = HIVE_OPS_COLLECTION) -> int:
    """
    Asynchronously retrieves the last good block.

    This function retrieves the last good block by getting the dynamic global properties
    from the Hive client and returning the head block number minus 30.

    Returns:
        int: The last good block.
    """
    try:
        async with OpBase.db_client as db_client:
            ans = await db_client.find_one(
                collection_name=collection, query={}, sort=[("block_num", -1)]
            )
            if ans:
                time_diff = check_time_diff(ans["timestamp"])
                logger.info(
                    f"{icon} Last good block: {ans['block_num']:,} "
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
    async with OpBase.db_client as db_client:
        last_good_event = await db_client.find_one(
            HIVE_OPS_COLLECTION,
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
                f"{time_diff} "
                f"{producer_reward.log_str}",
                extra={"notification": True, **producer_reward.log_extra},
            )
            return producer_reward

        # Empty database
        look_back = timedelta(days=1)
        async for op in stream_ops_async(
            opNames=["producer_reward"], look_back=look_back, stop_now=True
        ):
            op: ProducerReward
            if op.producer == watch_witness:
                await op.get_witness_details()
                op.mean, last_witness_timestamp = await witness_average_block_time(watch_witness)
                op.delta = op.timestamp - last_witness_timestamp
                _ = await db_client.insert_one(
                    HIVE_OPS_COLLECTION,
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
    async with OpBase.db_client as db_client:
        cursor = await db_client.find(
            HIVE_OPS_COLLECTION,
            {"producer": watch_witness},
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


async def all_ops_loop(watch_witness: str = "", watch_users: List[str] = COMMAND_LINE_WATCH_USERS):
    """
    Asynchronously loops through transactions and processes them.

    This function sets up an event listener for specific transaction types on the Hive
    blockchain, processes each transaction, logs relevant information, and publishes
    events for further handling. It also periodically updates cryptocurrency quotes and
    stores block markers in a database.

    Args:
        watch_users (List[str]): A list of user accounts to monitor for transactions.

    Raises:
        KeyboardInterrupt: If the process is interrupted by a keyboard signal.
        asyncio.CancelledError: If the asyncio task is cancelled.
        Exception: For any other exceptions that occur during processing.
    """
    logger.info(f"{icon} Combined Loop Watching users: {watch_users} and witness {watch_witness}")
    OpBase.watch_users = watch_users
    OpBase.proposals_tracked = [303, 342]
    OpBase.db_client = MongoDBClient(
        db_conn=HIVE_DATABASE_CONNECTION,
        db_name=HIVE_DATABASE,
        db_user=HIVE_DATABASE_USER,
    )

    producer_reward = await witness_first_run(watch_witness)
    last_witness_timestamp = producer_reward.timestamp

    hive_client = get_hive_client(keys=InternalConfig().config.hive.memo_keys)
    last_good_block = await get_last_good_block() + 1
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

                if isinstance(op, AccountWitnessVote):
                    op.get_voter_details()
                    log_it = True
                    if op.witness == watch_witness:
                        asyncio.create_task(db_store_op(op))
                        notification = True
                        db_store = True

                if isinstance(op, Transfer):
                    if op.is_watched:
                        await Transfer.update_quote()
                        op.update_conv()
                        asyncio.create_task(balance_server_hbd_level(op))
                        log_it = True
                        db_store = True
                        notification = True

                if op.known_custom_json:
                    op: CustomJson
                    notification = True
                    if not op.conv:
                        await op.update_quote_conv()
                    log_it = True
                    db_store = True
                    if op.cj_id in ["vsc.transfer", "vsc.withdraw"]:
                        extra_bots = ["VSC_Proposals"]

                if (
                    isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder)
                ) and op.is_watched:
                    notification = (
                        False if isinstance(op, FillOrder) and not op.completed_order else True
                    )
                    log_it = True
                    db_store = True

                if isinstance(op, ProducerReward):
                    if op.producer in [watch_witness, "vsc.network"]:
                        notification = True
                        await op.get_witness_details()
                        op.mean, last_witness_timestamp = await witness_average_block_time(
                            op.producer
                        )
                        op.delta = op.timestamp - last_witness_timestamp
                        log_it = True
                        db_store = True
                        if op.producer == "vsc.network":
                            extra_bots = ["VSC_Proposals"]

                if isinstance(op, UpdateProposalVotes):
                    op.get_voter_details()
                    log_it = True
                    if op.is_tracked:
                        notification = True
                        db_store = True
                        extra_bots = ["VSC_Proposals"]

                await combined_logging(op, log_it, notification, db_store, extra_bots)

                if timer() - start > 55:
                    block_marker = BlockMarker(op.block_num, op.timestamp)
                    await db_store_op(block_marker)
                    start = timer()

        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"{icon} {e}: Stopping event listener.")
            raise e

        except Exception as e:
            logger.exception(f"{icon} {e}", extra={"error": e})
            raise e

        finally:
            logger.warning(
                f"{icon} Restarting real_ops_loop after error from {hive_client.rpc.url} no_preview",
                extra={"notification": False},
            )
            hive_client.rpc.next()


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
        if extra_bots and not COMMAND_LINE_WATCH_ONLY:
            log_extras["extra_bot_names"] = extra_bots
        logger.info(f"{icon} {op.log_str}", extra=log_extras)


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

    await Transfer.update_quote()
    quote = Transfer.last_quote
    logger.info(
        f"{icon} Updating Quotes: {quote.hive_usd} {quote.sats_hive}",
        extra={
            "notification": False,
            "quote": Transfer.last_quote.model_dump(exclude={"raw_response", "raw_op"}),
        },
    )

    try:
        tasks = [
            all_ops_loop(watch_witness=watch_witness, watch_users=watch_users),
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
            show_default=True,
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
    CONFIG = InternalConfig(config_filename=config_filename).config
    global COMMAND_LINE_WATCH_USERS
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

    logger.info(
        f"{icon} ✅ Hive Monitor v2: {icon}. Version: {__version__}",
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
