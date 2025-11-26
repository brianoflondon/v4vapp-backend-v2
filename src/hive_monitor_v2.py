import asyncio
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import uniform
from time import sleep
from timeit import default_timer as timer
from typing import Annotated, Any, Dict, List, Tuple

import typer
from colorama import Fore, Style
from nectar.amount import Amount
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult

from status.status_api import StatusAPI, StatusAPIException
from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import DEFAULT_CONFIG_FILENAME, InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    check_time_diff,
    format_time_delta,
    seconds_only,
)
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
from v4vapp_backend_v2.hive_models.op_producer_missed import ProducerMissed
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.op_update_proposal_votes import UpdateProposalVotes
from v4vapp_backend_v2.hive_models.stream_ops import stream_ops_async
from v4vapp_backend_v2.witness_monitor.witness_events import check_witness_heartbeat

HIVE_DATABASE_CONNECTION = ""
HIVE_DATABASE = ""
HIVE_DATABASE_USER = ""
HIVE_OPS_COLLECTION = "hive_ops"
HIVE_WITNESS_DELAY_FACTOR = 1.2  # 20% over mean block time

AUTO_BALANCE_SERVER = True


COMMAND_LINE_WATCH_USERS: List[str] = []
COMMAND_LINE_WATCH_ONLY = False

TIME_DELAY: int = 0

app = typer.Typer()
ICON = "🐝"

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


@dataclass
class StatusObject:
    """
    Used to store status information for the StatusAPI health check.
    """

    last_good_block: int = 0
    time_diff: timedelta = timedelta(0)
    time_diff_str: str = ""
    is_catching_up: bool = False


STATUS_OBJ = StatusObject()


async def health_check() -> Dict[str, Any]:
    """
    Asynchronous health check function that verifies the status of critical background tasks.
    Used with the `StatusAPI` to provide health monitoring API endpoint especially for docker
    containers.

    This function checks if the 'all_ops_loop' and 'store_rates' tasks are currently running
    among all asyncio tasks. It also formats the time difference in STATUS_OBJ. If any tasks
    are not running, it raises a StatusAPIException with details. Otherwise, it returns the
    STATUS_OBJ dictionary.

    Returns:
        Dict[str, Any]: The dictionary representation of STATUS_OBJ containing status information.

    Raises:
        StatusAPIException: If one or more critical tasks are not running, with a message
            listing the issues and extra data from STATUS_OBJ.
    """

    exceptions = []
    check_for_tasks = ["all_ops_loop", "store_rates"]
    for task in check_for_tasks:
        if not any(t.get_name() == task and not t.done() for t in asyncio.all_tasks()):
            exceptions.append(f"{task} task is not running")
            logger.warning(f"{ICON} {task} task is not running", extra={"notification": True})

    STATUS_OBJ.time_diff_str = format_time_delta(STATUS_OBJ.time_diff)
    if exceptions:
        raise StatusAPIException(", ".join(exceptions), extra=STATUS_OBJ.__dict__)
    return STATUS_OBJ.__dict__


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event.
    """
    logger.info("Received shutdown signal. Setting shutdown event.")
    shutdown_event.set()


async def db_store_op(
    op: OpAny,
) -> UpdateResult | None:
    """
    Asynchronously stores a Hive transaction operation in the MongoDB database.

    This function processes a Hive event operation and attempts to save it to the database.
    It handles duplicate key errors, connection issues, and other exceptions, with automatic
    retries on connection failures. The operation is upserted into the appropriate collection,
    and logging is performed for errors and reconnections.

    Uses the OpAny Save method which is automatically an upsert.

        op (OpAny): The Hive event operation to process and store.

        UpdateResult | None: The result of the database update operation if successful,
            or None/empty list if an error occurs.
    """
    try:
        return await op.save(mongo_kwargs={"upsert": True})

    except DuplicateKeyError as e:
        logger.info(
            f"DuplicateKeyError: {op.block_num} {op.trx_id} {op.op_in_trx}",
            extra={"notification": False, "error": e, **op.log_extra},
        )
        return None

    except Exception as e:
        logger.error(f"{ICON} Error occurred while saving to MongoDB: {e}")
        logger.warning(f"{ICON} {op.log_str}", extra={"notification": False, **op.log_extra})
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
    logger.info("Waiting for 120 seconds to re-balance HBD level")
    await asyncio.sleep(120)  # Sleeps to make sure we only balance HBD after time for a return
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
            logger.info(f"{ICON} Balancing HBD level for account {use_account} to {set_amount_to}")
            nobroadcast = True if COMMAND_LINE_WATCH_ONLY else False
            trx = account_trade(
                hive_acc=hive_acc, set_amount_to=set_amount_to, nobroadcast=nobroadcast
            )
            if trx:
                logger.info(f"Transaction broadcast: {trx.get('trx_id')}", extra={"trx": trx})
    except ValueError as ve:
        logger.error(
            f"{ICON} ValueError in {__name__}: {ve} Maybe misconfigured account? No hbd_balance set?",
            extra={"notification": False, "error": ve},
        )
        if use_account:
            logger.error(
                f"{ICON} Account {use_account} miss config, "
                f"please check your config file {DEFAULT_CONFIG_FILENAME}",
                extra={"notification": False, "error": ve},
            )

    except Exception as e:
        logger.exception(
            f"{ICON} Error in {__name__}: {e}",
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
                f"{ICON} Last good block: {ans['block_num']:,} {ans['timestamp']} {time_diff} ago",
                extra={"db": ans},
            )
            last_good_block = int(ans["block_num"])
        else:
            try:
                hive = get_hive_client()
                global_properties = hive.get_dynamic_global_properties()
                if not global_properties:
                    raise Exception("Could not get global properties from hive client")
                else:
                    last_good_block = global_properties["head_block_number"]
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
            f"{ICON} Last recorded witness producer block: "
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
    op = None
    async for op in stream_ops_async(
        opNames=["producer_reward"], look_back=look_back, stop_now=True
    ):
        if not isinstance(op, ProducerReward):
            continue
        if op.producer == watch_witness:
            await op.get_witness_details()
            op.mean, last_witness_timestamp = await witness_average_block_time(watch_witness)
            op.delta = op.timestamp - last_witness_timestamp
            await db_store_op(op)
            logger.info(
                f"{ICON} {op.log_str}",
                extra={
                    "notification": False,
                    **op.log_extra,
                },
            )
    if op and isinstance(op, ProducerReward):
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
            f"{ICON} No time differences found for witness {watch_witness}",
            extra={"notification": True},
        )
        return timedelta(seconds=0), datetime.now(tz=timezone.utc) - timedelta(days=1)

    # Convert the mean time difference back to a timedelta object
    mean_time_diff = seconds_only(timedelta(seconds=mean_time_diff_seconds))

    return mean_time_diff, block_timestamps[0]


async def witness_check_heartbeat_loop(witness_name: str) -> None:
    """
    Asynchronously checks the heartbeat of a specified witness.

    This function checks the heartbeat of a specified witness by retrieving
    the last good block produced by the witness from the database and comparing
    its timestamp to the current time. If the time difference exceeds a certain
    threshold, a warning is logged.

    Args:
        watch_witness (str): The name of the witness to monitor.

    Returns:

        None
    """
    global TIME_DELAY
    failure_state = False
    witness_configs = InternalConfig().config.hive.witness_configs
    witness_config = witness_configs.get(witness_name, None)
    if not witness_config:
        logger.warning(
            f"{ICON} Witness {witness_name} configuration not found.",
            extra={"notification": False},
        )
        return
    try:
        while True:
            await asyncio.sleep(TIME_DELAY)
            failure_state = await check_witness_heartbeat(
                witness_name=witness_name, failure_state=failure_state
            )
            await asyncio.sleep(witness_config.kuma_heartbeat_time)
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        logger.info(f"{ICON} {e}: Stopping Witness Check {witness_name}.")
        # Exit loop on cancellation
        return
    except Exception as e:
        logger.exception(f"{ICON} {e}", extra={"notification": False})
        raise e
    finally:
        logger.info(
            f"{ICON} Witness {witness_name} check complete.", extra={"notification": False}
        )
    return


async def witness_check_startup() -> None:
    """
    Asynchronously performs the initial heartbeat check for all configured witnesses.

    This function initiates the heartbeat check for all witnesses configured
    in the system by calling the `witness_check_heartbeat_loop` function.

    Returns:
        None
    """
    try:
        witness_configs = InternalConfig().config.hive.witness_configs
        for witness_name in witness_configs.keys():
            asyncio.create_task(witness_check_heartbeat_loop(witness_name=witness_name))
    except Exception as e:
        logger.exception(
            f"{ICON} Error in Witness Check startup {e}", extra={"notification": False}
        )
        raise e


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
    global TIME_DELAY
    logger.info(
        f"{ICON} Combined Loop Watching users: {watch_users} and witnesses {watch_witnesses}"
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
                time_delay = TIME_DELAY if not block_counter.is_catching_up else 0
                notification = False
                log_it = False
                extra_bots: List[str] = []
                db_store = False
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Shutdown requested")
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
                        await TrackedBaseModel.update_quote(time_delay=time_delay)
                        await op.update_conv()
                        if not COMMAND_LINE_WATCH_ONLY:
                            # Now only balance the server account HBD level if this is a send back to a customer
                            # i.e. after a successful conversion.
                            if isinstance(op, Transfer) and (
                                op.from_account in server_accounts
                                and op.to_account not in server_accounts
                            ):
                                asyncio.create_task(balance_server_hbd_level(op))
                        log_it = True
                        db_store = True
                        notification = True

                elif op.known_custom_json:
                    notification = True
                    if not op.conv:
                        await op.update_conv()
                    log_it = True
                    db_store = True

                elif (
                    isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder)
                ) and op.is_watched:
                    await TrackedBaseModel.update_quote(time_delay=time_delay)
                    await op.update_conv()
                    notification = (
                        False if isinstance(op, FillOrder) and not op.completed_order else True
                    )
                    log_it = True
                    db_store = True

                elif isinstance(op, ProducerReward):
                    if op.producer in watch_witnesses:
                        notification = True
                        await op.get_witness_details(ignore_cache=True, time_delay=time_delay)
                        op.mean, last_witness_timestamp = await witness_average_block_time(
                            op.producer
                        )
                        op.delta = abs(op.timestamp - last_witness_timestamp)
                        log_it = True
                        db_store = True

                elif isinstance(op, ProducerMissed):
                    # Only check details for missed blocks if we are watching the witnesses
                    if watch_witnesses:
                        await op.get_witness_details(ignore_cache=False, time_delay=time_delay)
                        if op.producer in watch_witnesses:
                            notification = True
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

                STATUS_OBJ.last_good_block = op.block_num
                STATUS_OBJ.time_diff = block_counter.time_diff
                STATUS_OBJ.is_catching_up = block_counter.is_catching_up
                if timer() - start > 55:
                    block_marker = BlockMarker(op.block_num, op.timestamp)
                    await db_store_op(block_marker)
                    start = timer()

        except (KeyboardInterrupt, asyncio.CancelledError) as e:
            logger.info(f"{ICON} {e}: Stopping event listener.")
            # Exit loop on cancellation
            return
        except Exception as e:
            logger.exception(f"{ICON} {e}", extra={"notification": False})
            # Removing a RAISE here to allow automatic restart of the loop
            # raise e
        finally:
            # Do not restart if we’re shutting down
            if shutdown_event.is_set():
                logger.info(f"{ICON} Shutdown requested; exiting all_ops_loop.")
                return
            logger.warning(
                f"{ICON} Restarting real_ops_loop after error from {getattr(hive_client.rpc, 'url', 'unknown')} no_preview",
                extra={"notification": False},
            )
            if getattr(hive_client, "rpc", None):
                try:
                    hive_client.rpc.next()
                except Exception:
                    pass
            else:
                logger.error(
                    f"{ICON} Hive client not available, re-fetching new hive-client",
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
        message = f"{ICON} {op.log_str}"
        log_extras = {
            "notification": notification,
            "notification_str": f"{ICON} {op.notification_str}",
            **op.log_extra,
        }
        # Only send extra notifications if the bot is not in watch-only mode
        # so we don't double notify.
        if extra_bots:
            log_extras["extra_bot_names"] = extra_bots
        logger.info(message, extra=log_extras)


async def store_rates() -> None:
    """
    Asynchronously stores cryptocurrency rates in the database every 10 minutes.

    This function retrieves the latest cryptocurrency rates and stores them in the database.
    It wakes up every 10 minutes, but will exit promptly if a shutdown or keyboard event is triggered.

    Returns:
        None
    """
    await asyncio.sleep(
        6 + uniform(0, 4)
    )  # Initial sleep to avoid immediate execution and duplicate hits to check rates.
    try:
        while not shutdown_event.is_set():
            try:
                await TrackedBaseModel.update_quote(time_delay=TIME_DELAY)
                quote = TrackedBaseModel.last_quote
                logger.info(
                    f"{ICON} Updating Quotes: {quote.hive_usd:.3f} hive/usd {quote.sats_hive:.0f} sats/hive fetch date {quote.fetch_date}",
                    extra={
                        "notification": False,
                        "quote": TrackedBaseModel.last_quote.model_dump(
                            exclude={"raw_response", "raw_op"}
                        ),
                    },
                )
            except Exception as e:
                logger.error(
                    f"{ICON} Error storing rates: {e}", extra={"error": e, "notification": False}
                )
            # Wait for up to 10 minutes, but wake up early if shutdown_event is set
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=600)
            except asyncio.TimeoutError:
                continue  # Timeout means 10 minutes passed, so loop again
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        logger.info(f"{ICON} store_rates cancelled or interrupted, exiting.")
        raise e


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
    process_name = os.path.splitext(os.path.basename(__file__))[0]
    health_check_port = os.environ.get("HEALTH_CHECK_PORT", "6001")
    status_api = StatusAPI(
        port=int(health_check_port),
        health_check_func=health_check,
        shutdown_event=shutdown_event,
        process_name=process_name,
        version=__version__,
    )  # Use a port from config if needed

    db_conn = DBConn()
    await db_conn.setup_database()

    loop = asyncio.get_running_loop()

    # Register signal handlers for SIGTERM and SIGINT
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown_signal)
    loop.add_signal_handler(signal.SIGINT, handle_shutdown_signal)

    logger.info(f"{ICON} Main Loop running in thread: {threading.get_ident()}")

    try:
        # Create tasks so we can cancel them on shutdown_event
        await witness_check_startup()
        tasks = [
            asyncio.create_task(
                all_ops_loop(
                    watch_witnesses=watch_witnesses,
                    watch_users=watch_users,
                    start_block=start_block,
                ),
                name="all_ops_loop",
            ),
            asyncio.create_task(store_rates(), name="store_rates"),
            asyncio.create_task(status_api.start(), name="status_api"),
        ]
        # Wait until shutdown is requested
        await shutdown_event.wait()
        # Cancel tasks and wait for them to finish
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        raise e
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Hive Monitor {e}", extra={"error": e})
        raise e
    finally:
        # Cancel all other tasks and exit cleanly
        current_task = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info(f"{ICON} 👋 Goodbye! from Hive Monitor", extra={"notification": True})
        logger.info(f"{ICON} Clearing notifications")
        await asyncio.sleep(2)
        InternalConfig().shutdown()


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
            help="""The block number to start from. Default or 0 will start from the last good block,
            -1 will start from the current head block.""",
            show_default=True,
        ),
    ] = 0,
    time_delay: Annotated[
        int,
        typer.Option(
            "--time-delay",
            help="""After a block is received, time delay before taking actions.
            If this is running alongside another instance, this will help stagger actions and
            improve the use of any shared cache for things
            like Witness details and exchange rates.""",
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
    global TIME_DELAY
    TIME_DELAY = time_delay

    if not database_connection:
        HIVE_DATABASE_CONNECTION = CONFIG.dbs_config.default_connection
    if not database:
        HIVE_DATABASE = CONFIG.dbs_config.default_name
    if not database_user:
        HIVE_DATABASE_USER = CONFIG.dbs_config.default_user
    # TODO: This is redundant, remove it no setting database here any more

    logger.info(
        f"{ICON}{Fore.WHITE}✅ Hive Monitor v2: {ICON}. Version: {__version__} on {InternalConfig().local_machine_name}{Style.RESET_ALL} pause: {time_delay:.2f}s",
        extra={"notification": True},
    )
    # sleep for a random amount of time 0.1 to 0.8 seconds
    sleep(time_delay)
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
