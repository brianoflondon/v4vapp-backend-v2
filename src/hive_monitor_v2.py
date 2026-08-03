import asyncio
import os
import signal
import sys
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from random import uniform
from time import sleep
from timeit import default_timer as timer
from typing import Annotated, Any

import typer
from nectar.account import Account
from nectar.amount import Amount
from nectar.hive import Hive
from pymongo.errors import DuplicateKeyError
from pymongo.results import UpdateResult

from status.status_api import StatusAPI, StatusAPIException
from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import (
    DEFAULT_CONFIG_FILENAME,
    InternalConfig,
    StartupFailure,
    logger,
)
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    check_time_diff,
    format_time_delta,
    seconds_only,
)
from v4vapp_backend_v2.hive.hive_extras import (
    close_hive_client,
    default_hive_nodes,
    make_stream_hive,
    send_transfer,
)
from v4vapp_backend_v2.hive.internal_market_trade import account_trade
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_account_update2 import AccountUpdate2
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_all import OpAny, is_op_all_transfer
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_base_counters import BlockCounter
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled
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
QUOTE_REFRESH_MIN_INTERVAL_CATCHUP_SECONDS = 20

AUTO_BALANCE_SERVER = True


COMMAND_LINE_WATCH_USERS: list[str] = []
COMMAND_LINE_WATCH_ONLY = False

TIME_DELAY: int = 0

app = typer.Typer()
ICON = "🐝"

NOTIFICATION_QUITE_MODE = (
    True  # Set to True to disable notifications if db_monitor will provide these
)

# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"

# Define a global flag to track shutdown and startup completion
startup_complete_event = asyncio.Event()
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
    drift_no_recovery_since: datetime | None = None
    last_marker_time_diff: timedelta = timedelta(0)
    # Wall-clock of last successfully processed stream op (for stall detection).
    last_progress_at: datetime | None = None
    last_progress_age_s: float = 0.0
    fatal_reason: str = ""


STATUS_OBJ = StatusObject()
force_restart: bool = False

# In-process recovery: try a few times, then exit(1) so Docker `restart: on-failure` kicks in.
MAX_CONSECUTIVE_EMPTY_STREAM_RESTARTS = 5
# No processed op for this long → unhealthy; with empty restarts → fatal exit.
MAX_PROGRESS_STALL_SECONDS = 180.0
# Health endpoint fails after this with no progress (Docker marks unhealthy).
HEALTH_STALE_PROGRESS_SECONDS = 300.0


def request_fatal_restart(reason: str) -> None:
    """
    Mark the process for a non-zero exit so Docker restarts the container.

    Sets ``force_restart`` (main() exits 1) and ``shutdown_event`` so the async
    main loop tears down cleanly. Prefer this over hanging forever in recovery.
    """
    global force_restart
    force_restart = True
    STATUS_OBJ.fatal_reason = reason
    logger.critical(
        f"{ICON} Fatal restart requested: {reason}. "
        f"Exiting with code 1 so Docker restarts the container.",
        extra={
            "notification": True,
            "error_code": "hive_monitor_fatal_restart",
            "fatal_reason": reason,
        },
    )
    shutdown_event.set()


async def health_check() -> dict[str, Any]:
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

    if not startup_complete_event.is_set():
        logger.warning(f"{ICON} Startup not complete", extra={"notification": False})

    for task in check_for_tasks:
        if not any(t.get_name() == task and not t.done() for t in asyncio.all_tasks()):
            exceptions.append(f"{task} task is not running")
            logger.warning(
                f"{ICON} {task} task is not running",
                extra={"notification": True, "error_code": "hive_monitor_task_failure"},
            )

    STATUS_OBJ.time_diff_str = format_time_delta(STATUS_OBJ.time_diff)
    if STATUS_OBJ.last_progress_at is not None:
        STATUS_OBJ.last_progress_age_s = (
            datetime.now(tz=UTC) - STATUS_OBJ.last_progress_at
        ).total_seconds()
        # Only enforce after startup has produced at least one op.
        if (
            startup_complete_event.is_set()
            and STATUS_OBJ.last_progress_age_s > HEALTH_STALE_PROGRESS_SECONDS
        ):
            exceptions.append(
                f"no stream progress for {STATUS_OBJ.last_progress_age_s:.0f}s "
                f"(limit {HEALTH_STALE_PROGRESS_SECONDS:.0f}s)"
            )
    if STATUS_OBJ.fatal_reason:
        exceptions.append(f"fatal: {STATUS_OBJ.fatal_reason}")

    if exceptions:
        logger.error(
            f"{ICON} Health check failed: {', '.join(exceptions)}",
            extra={"notification": True, "error_code": "hive_monitor_task_failure"},
        )
        # raise rather than exit; StatusAPI will return 500
        raise StatusAPIException(", ".join(exceptions), extra=STATUS_OBJ.__dict__)

    logger.debug(
        f"{ICON} Hive Monitor Health check passed",
        extra={"notification": False, "error_code_clear": "hive_monitor_task_failure"},
    )
    return STATUS_OBJ.__dict__


def handle_shutdown_signal():
    """
    Signal handler to set the shutdown event (graceful stop, exit 0 unless force_restart).
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


async def balance_server_hive_level() -> None:
    """
    This function checks the Hive balance of the server account and attempts to rebalance it
    if it exceeds a certain threshold. It ensures that the server account has an active key
    before proceeding. If the rebalance is successful, the transaction ID is logged. In case of
    an error, the error is logged.

    Returns:
        None: The function does not return any value.
    """
    # Placeholder for future implementation of balancing Hive level
    server_account = InternalConfig().config.hive_config.server_account
    if not server_account:
        return

    if not server_account.auto_rebalance.enabled:
        return

    if not server_account.active_key:
        logger.warning(
            f"{ICON} Server account {server_account.name} does not have an active key set. Cannot auto-rebalance Hive level.",
            extra={"notification": False},
        )
        return

    logger.info(f"{ICON} Waiting for 30 seconds to re-balance HIVE level")
    await asyncio.sleep(30)  # Sleeps to make sure we only balance HIVE after time for a return
    try:
        current_target_hive_balance = Amount(server_account.hive_balance)
        nobroadcast = bool(COMMAND_LINE_WATCH_ONLY)
        hive = Hive(keys=server_account.keys, nobroadcast=nobroadcast, node=default_hive_nodes())
        account = Account(server_account.name, blockchain_instance=hive)
        balance: dict[str, Amount] = {}
        balance["HIVE"] = account.available_balances[0]
        balance["HBD"] = account.available_balances[1]
        delta = balance["HIVE"] - current_target_hive_balance
        if delta > Amount("20.000 HIVE"):
            logger.info(
                f"{ICON} Balancing Hive level for account {server_account.name} by {delta}",
                extra={"notification": False},
            )
            trx = await send_transfer(
                to_account=server_account.auto_rebalance.target_hive_acc,
                amount=delta,
                from_account=server_account.name,
                memo=server_account.auto_rebalance.memo,
                hive_client=hive,
                nobroadcast=nobroadcast,
            )
            trx_id = trx.get("trx_id", None)
            if trx_id:
                logger.info(
                    f"{ICON} Hive server rebalance transaction broadcast: {trx_id}",
                    extra={"trx": trx},
                )
    except Exception as e:
        logger.exception(
            f"{ICON} Error in {__name__}: {e}",
            extra={"notification": False, "error": e},
        )


async def balance_server_hbd_level(transfer: Transfer | None = None) -> None:
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
    logger.info(f"{ICON} Waiting for 30 seconds to re-balance HBD level")
    await asyncio.sleep(30)  # Sleeps to make sure we only balance HBD after time for a return
    use_account = None
    try:
        if not transfer:
            use_account = CONFIG.hive_config.server_account_names[0]
        else:
            if transfer.from_account in CONFIG.hive_config.server_account_names:
                use_account = transfer.from_account
            elif transfer.to_account in CONFIG.hive_config.server_account_names:
                use_account = transfer.to_account
            else:
                return
        hive_acc = CONFIG.hive_config.hive_accs.get(use_account, None)
        if hive_acc and hive_acc.active_key:
            # set the amount to the current HBD balance taken from Config
            set_amount_to = Amount(hive_acc.hbd_balance)
            logger.info(f"{ICON} Balancing HBD level for account {use_account} to {set_amount_to}")
            nobroadcast = bool(COMMAND_LINE_WATCH_ONLY)
            trx = account_trade(
                hive_acc=hive_acc, set_amount_to=set_amount_to, nobroadcast=nobroadcast
            )
            if trx:
                logger.info(
                    f"{ICON} Transaction broadcast: {trx.get('trx_id')}", extra={"trx": trx}
                )
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
    # Only consider stored `producer_reward` documents — other ops (e.g. producer_missed)
    # also contain a `producer` field and previously caused a Pydantic validation
    # error when we tried to coerce them into `ProducerReward`.
    last_good_event = await OpBase.collection().find_one(
        filter={"producer": watch_witness, "type": "producer_reward"},
        sort=[("block_num", -1)],
    )
    if last_good_event:
        # validate as a ProducerReward (safe because we filtered by type)
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


async def witness_average_block_time(watch_witness: str) -> tuple[timedelta, datetime]:
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
    block_timestamps: list[datetime] = []
    counter = 0
    async for block in cursor:
        block_timestamps.append(block["timestamp"])
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
        return timedelta(seconds=0), datetime.now(tz=UTC) - timedelta(days=1)

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
    failure_state = False
    witness_configs = InternalConfig().config.hive_config.witness_configs
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
        # Do not put exc_info in extra — logger.exception already sets it (LogRecord reserved).
        logger.exception(f"{ICON} {e}", extra={"notification": False})
        raise
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
        witness_configs = InternalConfig().config.hive_config.witness_configs
        for witness_name in witness_configs:
            asyncio.create_task(witness_check_heartbeat_loop(witness_name=witness_name))
    except Exception as e:
        logger.exception(
            f"{ICON} Error in Witness Check startup {e}",
            extra={"notification": False},
        )
        raise


async def all_ops_loop(
    watch_witnesses: list[str] | None = None, watch_users: list[str] | None = None, start_block: int = 0
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
    if not watch_witnesses:
        watch_witnesses = []
    if not watch_users:
        watch_users = []
    logger.info(
        f"{ICON} Combined Loop Watching users: {watch_users} and witnesses {watch_witnesses}"
    )
    OpBase.watch_users = watch_users
    OpBase.proposals_tracked = InternalConfig().config.hive_config.proposals_tracked
    OpBase.custom_json_ids_tracked = InternalConfig().config.hive_config.custom_json_ids_tracked
    server_accounts = InternalConfig().config.hive_config.server_account_names
    if server_accounts:
        v4v_config = V4VConfig(server_accname=server_accounts[0])
    else:
        v4v_config = V4VConfig(server_accname="")
    # Run witness initialization in the background so it doesn't block
    # the main stream from starting (and delay startup_complete_event).
    for witness in watch_witnesses:
        asyncio.create_task(witness_first_run(witness), name=f"witness_first_run_{witness}")

    # Fail-fast Hive: Nectar defaults (60s × 100 retries) can freeze the event loop for hours.
    hive_client = make_stream_hive(keys=InternalConfig().config.hive_config.memo_keys)
    if start_block == 0:
        last_good_block = await OpBase.get_last_good_block() + 1
    elif start_block == -1:
        global_properties: dict = await asyncio.to_thread(
            hive_client.get_dynamic_global_properties
        )  # type: ignore
        last_good_block = global_properties.get("head_block_number", 97112440)
    else:
        last_good_block = start_block
    block_counter = BlockCounter(
        last_good_block=last_good_block, hive_client=hive_client, id="combined"
    )
    last_quote_update_timer = 0.0

    async def maybe_update_quote() -> None:
        """Throttle quote updates while catching up to reduce RPC load and 429 risk."""
        nonlocal last_quote_update_timer
        if block_counter.is_catching_up:
            now = timer()
            if now - last_quote_update_timer < QUOTE_REFRESH_MIN_INTERVAL_CATCHUP_SECONDS:
                return
            last_quote_update_timer = now
            await TrackedBaseModel.update_quote(time_delay=0)
            return

        await TrackedBaseModel.update_quote(time_delay=TIME_DELAY)

    start = timer()
    # Outer safety net: if stream_ops yields nothing for this long, abandon and rebuild.
    # Must be > STREAM_TIMEOUT_SECONDS and longer than a normal quiet filtered stretch.
    STREAM_PROGRESS_WATCHDOG_SECONDS = 90.0
    consecutive_empty_restarts = 0
    last_progress_mono = timer()
    while True:
        if shutdown_event.is_set():
            logger.info(f"{ICON} Shutdown requested; exiting all_ops_loop.")
            return

        loop_error = False
        stream_agen = None
        ops_this_session = 0
        try:
            logger.info(
                f"{ICON} Entering stream_ops from block {last_good_block:,} "
                f"via {getattr(getattr(hive_client, 'rpc', None), 'url', 'unknown')}",
                extra={"notification": False},
            )
            stream_agen = stream_ops_async(
                opNames=OpBase.op_tracked, start=last_good_block, stop_now=False, hive=hive_client
            )
            stream_iter = stream_agen.__aiter__()
            while True:
                try:
                    op = await asyncio.wait_for(
                        stream_iter.__anext__(),
                        timeout=STREAM_PROGRESS_WATCHDOG_SECONDS,
                    )
                except StopAsyncIteration:
                    break
                except TimeoutError as te:
                    # wait_for fires on true idle OR on TimeoutError raised inside stream_ops
                    # (e.g. rebuild budget). Log distinctly so we can tell them apart.
                    loop_error = True
                    detail = str(te).strip() or "no op yielded"
                    logger.error(
                        f"{ICON} Stream progress timeout after "
                        f"{STREAM_PROGRESS_WATCHDOG_SECONDS:.0f}s at block "
                        f"{last_good_block:,} ({detail}); abandoning stream and rebuilding",
                        extra={"notification": False, "error_code": "stream_restart"},
                    )
                    break

                time_delay = TIME_DELAY if not block_counter.is_catching_up else 0
                notification = False
                log_it = False
                extra_bots: list[str] = []
                db_store = False
                if shutdown_event.is_set():
                    raise asyncio.CancelledError("Shutdown requested")
                # Keep resume point current so outer restarts do not re-scan history.
                last_good_block = max(last_good_block, op.block_num)
                ops_this_session += 1
                consecutive_empty_restarts = 0
                last_progress_mono = timer()
                STATUS_OBJ.last_progress_at = datetime.now(tz=UTC)
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
                        await maybe_update_quote()
                        await op.update_conv()
                        if not COMMAND_LINE_WATCH_ONLY and isinstance(op, Transfer) and (
                            op.from_account in server_accounts
                            and op.to_account not in server_accounts
                        ):
                            # Now only balance the server account HBD level if this is a send back to a customer
                            # i.e. after a successful conversion.
                            logger.info(
                                f"{ICON} Rebalance triggered by transfer {op.from_account} to {op.to_account} {op.amount}"
                            )
                            asyncio.create_task(balance_server_hbd_level(op))
                            asyncio.create_task(balance_server_hive_level())
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
                    await maybe_update_quote()
                    await op.update_conv()
                    notification = (
                        False if isinstance(op, FillOrder) and not op.completed_order else True
                    )
                    log_it = True
                    db_store = True

                # new virtual op for cancelled orders – treat like a watched limit order event
                elif isinstance(op, LimitOrderCancelled):
                    # only log/store when seller is watched (similar to others)
                    if op.seller in watch_users:
                        log_it = True
                        notification = True
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
                            db_store = True
                        log_it = True

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

                # Detect unrecoverable drift: catching up but time_diff not improving for >5 min
                if marker:
                    if (
                        block_counter.is_catching_up
                        and block_counter.time_diff >= STATUS_OBJ.last_marker_time_diff
                        and STATUS_OBJ.last_marker_time_diff > timedelta(0)
                    ):
                        if STATUS_OBJ.drift_no_recovery_since is None:
                            STATUS_OBJ.drift_no_recovery_since = datetime.now(tz=UTC)
                            logger.warning(
                                f"{ICON} Drift not recovering (behind: {block_counter.time_diff}). Monitoring for forced restart.",
                                extra={"notification": False},
                            )
                        elif (
                            datetime.now(tz=UTC) - STATUS_OBJ.drift_no_recovery_since
                        ) > timedelta(minutes=5):
                            request_fatal_restart(
                                f"drift unrecoverable for >5 minutes "
                                f"(behind: {block_counter.time_diff})"
                            )
                            return
                    else:
                        if STATUS_OBJ.drift_no_recovery_since is not None:
                            logger.info(
                                f"{ICON} Drift recovering (behind: {block_counter.time_diff}). Cancelling forced restart.",
                                extra={"notification": False},
                            )
                        STATUS_OBJ.drift_no_recovery_since = None
                    STATUS_OBJ.last_marker_time_diff = block_counter.time_diff

                if timer() - start > 55:
                    block_marker = BlockMarker(op.block_num, op.timestamp)
                    await db_store_op(block_marker)
                    start = timer()

        except KeyboardInterrupt as e:
            logger.info(f"{ICON} {e}: Stopping event listener.")
            shutdown_event.set()
            return
        except asyncio.CancelledError as e:
            # Real shutdown vs spurious cancel (e.g. wait_for on aclose/anext).
            if shutdown_event.is_set():
                logger.info(f"{ICON} {e}: Shutdown cancel; exiting all_ops_loop.")
                return
            loop_error = True
            logger.warning(
                f"{ICON} Spurious CancelledError in all_ops_loop ({e!r}); "
                f"will rebuild and re-enter (not exiting)",
                extra={"notification": False},
            )
        except Exception as e:
            loop_error = True
            # Do not put exc_info in extra — logger.exception already sets it (LogRecord reserved).
            logger.exception(f"{ICON} {e}", extra={"notification": False})
            # Do not re-raise: outer loop rebuilds a fresh client and resumes.
        finally:
            # Best-effort close of the async generator (does not stop zombie next() threads).
            if stream_agen is not None:
                aclose = getattr(stream_agen, "aclose", None)
                if aclose is not None:
                    try:
                        await asyncio.wait_for(aclose(), timeout=2.0)
                    except Exception:
                        pass
            # Do not restart if we’re shutting down (graceful or fatal).
            if shutdown_event.is_set():
                logger.info(f"{ICON} Shutdown requested; exiting all_ops_loop.")
                return

            # Prefer in-memory progress; STATUS_OBJ is updated on every processed op.
            if getattr(STATUS_OBJ, "last_good_block", None):
                last_good_block = max(last_good_block, int(STATUS_OBJ.last_good_block))

            if ops_this_session == 0:
                consecutive_empty_restarts += 1
            else:
                consecutive_empty_restarts = 0

            stall_s = timer() - last_progress_mono
            if consecutive_empty_restarts >= MAX_CONSECUTIVE_EMPTY_STREAM_RESTARTS:
                request_fatal_restart(
                    f"{consecutive_empty_restarts} consecutive stream restarts with no ops "
                    f"(limit {MAX_CONSECUTIVE_EMPTY_STREAM_RESTARTS})"
                )
                return
            if consecutive_empty_restarts >= 2 and stall_s >= MAX_PROGRESS_STALL_SECONDS:
                request_fatal_restart(
                    f"no stream progress for {stall_s:.0f}s after "
                    f"{consecutive_empty_restarts} empty restarts "
                    f"(limit {MAX_PROGRESS_STALL_SECONDS:.0f}s)"
                )
                return

            previous_url = getattr(getattr(hive_client, "rpc", None), "url", "unknown")
            reason = "after error" if loop_error else "stream ended"
            logger.warning(
                f"{ICON} Restarting all_ops_loop {reason} from block "
                f"{last_good_block:,} (was {previous_url}) — rebuild "
                f"{consecutive_empty_restarts}/{MAX_CONSECUTIVE_EMPTY_STREAM_RESTARTS} empty",
                extra={"notification": False},
            )
            # Always discard the old client (and close its httpx pool / monitor thread).
            # Leaving them open is the EMFILE / "Too many open files" path on port 6001.
            try:
                old_client = hive_client
                keys = InternalConfig().config.hive_config.memo_keys

                def _close_and_make() -> Hive:
                    close_hive_client(old_client)
                    return make_stream_hive(keys=keys)

                hive_client = await asyncio.wait_for(
                    asyncio.to_thread(_close_and_make),
                    timeout=30.0,
                )
                # BlockCounter must not keep a closed Hive (pool_manager=None).
                block_counter.hive_client = hive_client
                logger.info(
                    f"{ICON} Rebuilt Hive client → "
                    f"{getattr(getattr(hive_client, 'rpc', None), 'url', 'unknown')}",
                    extra={"notification": False},
                )
            except Exception as e:
                logger.error(
                    f"{ICON} Failed to rebuild Hive client: {e}; sleeping before retry",
                    extra={"notification": False},
                )
                consecutive_empty_restarts += 1
                await asyncio.sleep(5.0)


async def combined_logging(
    op: OpAny, log_it: bool, notification: bool, db_store: bool, extra_bots: list[str] | None
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

    if InternalConfig().config.logging.notification_quiet_mode:
        notification = False

    if db_store:
        asyncio.create_task(db_store_op(op))

    if log_it:
        message = f"{ICON} {op.log_str}"
        log_extras = {
            "notification": notification,
            "silent": True,
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
                logger.debug(
                    f"{ICON} Updating Quotes: {quote.hive_usd:.3f} hive/usd {quote.sats_hive:.0f} sats/hive fetch date {quote.fetch_date}",
                    extra={
                        "notification": False,
                        "quote": TrackedBaseModel.last_quote.model_dump(
                            exclude={"raw_response", "raw_op"}
                        ),
                    },
                )
            except asyncio.CancelledError:
                await asyncio.sleep(10)
                continue  # Ignore cancellation during rate update, continue updating rates regardless
            except Exception as e:
                logger.error(
                    f"{ICON} Error storing rates: {e}", extra={"error": e, "notification": False}
                )
            # Wait for up to 10 minutes, but wake up early if shutdown_event is set
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=600)
            except TimeoutError:
                continue  # Timeout means 10 minutes passed, so loop again
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        logger.info(f"{ICON} store_rates cancelled or interrupted, exiting.")
        raise
    except Exception as e:
        logger.exception(f"{ICON} Exception in store_rates: {e}", extra={"notification": False})
        asyncio.create_task(store_rates(), name="store_rates")


async def main_async_start(
    watch_users: list[str], watch_witnesses: list[str], start_block: int
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
    # Ensure notification handler uses the running loop (non-blocking path)
    InternalConfig.notification_loop = asyncio.get_running_loop()

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
        all_ops_task = asyncio.create_task(
            all_ops_loop(
                watch_witnesses=watch_witnesses,
                watch_users=watch_users,
                start_block=start_block,
            ),
            name="all_ops_loop",
        )
        tasks = [
            all_ops_task,
            asyncio.create_task(store_rates(), name="store_rates"),
            asyncio.create_task(status_api.start(), name="status_api"),
        ]

        async def _watch_all_ops_loop() -> None:
            """If the stream loop dies without an intentional shutdown, exit non-zero."""
            try:
                await all_ops_task
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not shutdown_event.is_set():
                    request_fatal_restart(f"all_ops_loop crashed: {e}")
                return
            if not shutdown_event.is_set():
                request_fatal_restart("all_ops_loop exited unexpectedly")

        asyncio.create_task(_watch_all_ops_loop(), name="watch_all_ops_loop")

        startup_complete_event.set()
        logger.info(
            f"{ICON}✅ Hive Monitor v2: Started. Version: {__version__} on {InternalConfig().local_machine_name}",
            extra={"notification": True},
        )
        # Wait until shutdown is requested (graceful SIGTERM or fatal restart)
        await shutdown_event.wait()
        # Cancel tasks and wait for them to finish *before* closing Redis/Mongo/Hive.
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except (asyncio.CancelledError, KeyboardInterrupt) as e:
        logger.info(f"{ICON} 👋 Received signal to stop. Exiting...")
        raise
    except Exception as e:
        logger.exception(e, extra={"error": e, "notification": False})
        logger.error(f"{ICON} Irregular shutdown in Hive Monitor {e}", extra={"error": e})
        request_fatal_restart(f"main_async_start error: {e}")
        raise
    finally:
        # Cancel all other tasks and exit cleanly
        current_task = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        # Let thread-pool stream workers notice cancellation / closed clients
        # before we tear down Redis/Mongo (avoids "Session must be initialized"
        # noise on Ctrl-C from abandoned Nectar next() threads).
        try:
            from v4vapp_backend_v2.helpers.async_wrapper import thread_pool

            thread_pool.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass
        await asyncio.sleep(0.3)
        if force_restart:
            logger.critical(
                f"{ICON} 👋 Fatal exit (Docker restart): {STATUS_OBJ.fatal_reason or 'force_restart'}",
                extra={"notification": True},
            )
        else:
            logger.info(f"{ICON} 👋 Goodbye! from Hive Monitor", extra={"notification": True})
        logger.info(f"{ICON} Clearing notifications")
        await asyncio.sleep(0.5)
        InternalConfig().shutdown()


@app.command()
def main(
    watch_users: Annotated[
        list[str],
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
        list[str],
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
    CONFIG = InternalConfig(config_filename=config_filename).config
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

    logger.info(
        f"{ICON}✅ Hive Monitor v2: {ICON}. Version: {__version__} on {InternalConfig().local_machine_name} pause: {time_delay:.2f}s",
        extra={"notification": False},
    )
    # sleep for a random amount of time 0.1 to 0.8 seconds
    sleep(time_delay)
    if not watch_users:
        watch_users = CONFIG.hive_config.watch_users
    if not watch_witnesses:
        watch_witnesses = CONFIG.hive_config.watch_witnesses
    COMMAND_LINE_WATCH_ONLY = watch_only
    asyncio.run(main_async_start(watch_users, watch_witnesses, start_block=start_block))
    # Docker compose uses `restart: on-failure` — non-zero exit triggers container restart.
    if force_restart:
        logger.critical(
            f"{ICON} Exiting with code 1 to trigger Docker restart "
            f"({STATUS_OBJ.fatal_reason or 'force_restart'})",
            extra={"notification": True},
        )
        sys.exit(1)


if __name__ == "__main__":
    try:
        logger.name = "hive_monitor_v2"
        app()
        if force_restart:
            # In case main() returned without sys.exit (shouldn't), still fail hard.
            sys.exit(1)
        print("👋 Goodbye!")
    except (KeyboardInterrupt, asyncio.CancelledError):
        sys.exit(0)

    except StartupFailure as e:
        print(f"{ICON} Startup failure: {e}")
        # Startup config failures should not spin Docker forever; exit 0.
        sys.exit(0)

    except Exception as e:
        logger.error("🔴 Unhandled exception in hive_monitor_v2", exc_info=e, stack_info=True)
        logger.exception(e, extra={"error": e, "notification": True})
        print(e)
        sys.exit(1)
