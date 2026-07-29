"""
Async Hive operation streaming.

Wraps Nectar's synchronous ``Blockchain.stream`` with async iteration and
resumes after failures using Nectar's built-in node rotation (``rpc.next()``).

Node health, retries, and working-node selection are left to Nectar
(``Hive`` + ``default_hive_nodes()``). This module only:

- computes start/stop block ranges
- interleaves real and virtual ops
- validates events into Op models
- restarts the stream from the last processed block after errors/timeouts
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import AsyncGenerator

from nectar.blockchain import Blockchain
from nectar.exceptions import NectarException
from nectar.hive import Hive
from nectarapi.exceptions import NumRetriesReached, UnhandledRPCError, WorkingNodeMissing

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import (
    default_hive_nodes,
    get_blockchain_instance,
)
from v4vapp_backend_v2.hive_models.custom_json_data import custom_json_test_data
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OP_TRACKED, OpBase, op_realm
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter

ICON = "🔗"

# Hive blocks ~3s; filtered streams can be quiet for several blocks.
STREAM_TIMEOUT_SECONDS = 15
MAX_RESTART_BACKOFF_SECONDS = 8
# Catch-up only: far-behind scans may use batched get_block_range when the node
# supports it. Live / near-head always uses max_batch_size=None.
CATCHUP_BATCH_SIZE = 50
CATCHUP_NEAR_HEAD_BLOCKS = 50
QUOTE_REFRESH_MIN_INTERVAL_SECONDS = 30


def _is_rate_limit_error(error: BaseException) -> bool:
    text = str(error)
    return "429" in text or "Too Many Requests" in text


def _is_batch_not_supported_error(error: BaseException) -> bool:
    text = str(error).lower()
    return (
        "batched calls" in text
        or "batchedcallsnotsupported" in type(error).__name__.lower()
        or "does not support batch" in text
    )


def _rpc_url(hive: Hive | None) -> str:
    if hive is None or getattr(hive, "rpc", None) is None:
        return "No RPC"
    return str(hive.rpc.url)


def _nectar_rotate_node(hive: Hive | None, *, disable_current: bool = True) -> str:
    """
    Move to another working node using Nectar's node pool.

    ``rpc.next()`` alone often re-selects the same "best" node after a timeout.
    When ``disable_current`` is True we mark the active node failed first so the
    pool manager actually rotates.

    Returns the node URL after rotation (best-effort).
    """
    if hive is None or getattr(hive, "rpc", None) is None:
        return "No RPC"
    previous = _rpc_url(hive)
    try:
        nodes = getattr(hive.rpc, "nodes", None)
        if disable_current and nodes is not None and hasattr(nodes, "disable_node"):
            try:
                nodes.disable_node()
            except Exception as e:
                logger.debug(
                    f"{ICON} disable_node failed on {previous}: {e}",
                    extra={"notification": False},
                )
        # Prefer explicit next() for side effects; fall back to __next__.
        if hasattr(hive.rpc, "next"):
            hive.rpc.next()
        elif nodes is not None:
            next(nodes)
    except Exception as e:
        logger.warning(
            f"{ICON} Nectar node rotation failed: {e}",
            extra={"notification": False, "error": e},
        )
    return _rpc_url(hive)


def _rebuild_hive_client(hive: Hive | None) -> tuple[Hive, Blockchain]:
    """
    Build a fresh Hive client from the good-node list when the current pool is dead.

    Preserves memo keys from the previous client when available.
    """
    keys = None
    if hive is not None:
        keys = getattr(hive, "keys", None) or None
    if keys:
        new_hive = Hive(keys=keys, node=default_hive_nodes())
    else:
        new_hive = Hive(node=default_hive_nodes())
    blockchain = get_blockchain_instance(hive_instance=new_hive)
    OpBase.hive_inst = new_hive
    return new_hive, blockchain


class SwitchToLiveStream(Exception):
    """Reserved for callers that want to force a live-mode restart."""

    pass


async def stream_ops_async(
    start: int = 0,
    stop: int | None = None,
    stop_now: bool = False,
    look_back: timedelta | None = None,
    hive: Hive | None = None,
    opNames: list[str] = OP_TRACKED,
    filter_custom_json: bool = True,
) -> AsyncGenerator[OpAny, None]:
    """
    Async generator of Hive operations from Nectar's ``Blockchain.stream``.

    Failover strategy (intentionally thin):
      1. Prefer ``hive.rpc.next()`` so Nectar's node pool picks the next node.
      2. On WorkingNodeMissing / exhausted pool, rebuild via Nectar ``Hive``.
      3. Resume from the last successfully processed block number.

    Virtual ops are streamed per preceding real-op block so op_in_trx counters stay correct.
    """
    hive = Hive(node=default_hive_nodes()) if hive is None else hive
    blockchain = get_blockchain_instance(hive_instance=hive)
    OpBase.hive_inst = hive

    if opNames:
        op_realms = [op_realm(op_type) for op_type in opNames]
        only_virtual_ops = all(realm == "virtual" for realm in op_realms)
    else:
        only_virtual_ops = False

    current_block = blockchain.get_current_block_num()
    time_now = datetime.now(tz=timezone.utc)
    start_time = time_now

    if look_back:
        start_time = time_now - look_back
        try:
            start_block = blockchain.get_estimated_block_num(start_time)
        except Exception as e:
            start_block = current_block - int(look_back.total_seconds() / 3)
            logger.warning(
                f"{ICON} Error getting start block from time {start_time}; "
                f"using estimate {start_block:,}: {e}"
            )
    else:
        start_block = start or current_block

    if stop_now:
        stop_block = current_block
    else:
        stop_block = stop if stop is not None else (2**31) - 1

    last_block = start_block or 1
    restart_count = 0
    # Once a node rejects batch RPC, stay single-block for the rest of this session.
    batch_disabled = False
    last_quote_refresh_at: datetime | None = None
    # Finite catch-up (stop_now / explicit stop): exit when the stream ends cleanly.
    finite_stop = stop is not None or stop_now
    stream_ended_cleanly = False

    async def maybe_refresh_quote() -> None:
        nonlocal last_quote_refresh_at
        now = datetime.now(tz=timezone.utc)
        if last_quote_refresh_at and (now - last_quote_refresh_at) < timedelta(
            seconds=QUOTE_REFRESH_MIN_INTERVAL_SECONDS
        ):
            return
        await TrackedBaseModel.update_quote()
        last_quote_refresh_at = now

    def choose_batch_size() -> int | None:
        if batch_disabled or only_virtual_ops:
            return None
        try:
            head_now = blockchain.get_current_block_num()
        except Exception:
            return None
        gap = head_now - start_block
        if gap >= CATCHUP_NEAR_HEAD_BLOCKS:
            return CATCHUP_BATCH_SIZE
        return None

    while last_block is not None and stop_block is not None and last_block < stop_block:
        stream_ended_cleanly = False
        await maybe_refresh_quote()
        rpc_url = _rpc_url(hive)
        effective_batch = choose_batch_size()
        try:
            op_in_trx_counter = OpInTrxCounter()
            async_stream_real = sync_to_async_iterable(
                blockchain.stream(
                    start=start_block,
                    stop=stop_block,
                    only_virtual_ops=only_virtual_ops,
                    opNames=opNames,
                    max_batch_size=effective_batch,
                    threading=False,
                )
            )
            logger.info(
                f"{ICON} Starting Hive scan at {start_block:,} "
                f"({start_time:%Y-%m-%d %H:%M:%S}) → {stop_block:,} "
                f"via {rpc_url} batch={effective_batch}",
                extra={
                    "error_code_clear": "stream_restart",
                    "notification": False,
                    "opNames": opNames,
                },
            )

            async_iter = async_stream_real.__aiter__()
            while True:
                try:
                    hive_event = await asyncio.wait_for(
                        async_iter.__anext__(), timeout=STREAM_TIMEOUT_SECONDS
                    )
                    restart_count = 0
                except StopAsyncIteration:
                    stream_ended_cleanly = True
                    break
                except asyncio.TimeoutError:
                    logger.warning(
                        f"{ICON} {start_block:,} Stream idle >{STREAM_TIMEOUT_SECONDS}s "
                        f"on {rpc_url}; rotating node",
                        extra={"notification": False, "error_code": "stream_restart"},
                    )
                    raise

                # Interleave virtual ops for the previous block when the stream advances.
                if (
                    not only_virtual_ops
                    and hive_event["block_num"] > last_block
                    and hive_event["block_num"] <= stop_block
                ):
                    start_block = last_block
                    async for virtual_event in sync_to_async_iterable(
                        blockchain.stream(
                            start=last_block - 1,
                            stop=last_block - 1,
                            raw_ops=False,
                            only_virtual_ops=True,
                            max_batch_size=None,
                            threading=False,
                        )
                    ):
                        last_block = hive_event.get("block_num", start_block)
                        try:
                            op_virtual_base = op_any_or_base(virtual_event)
                        except ValueError as e:
                            logger.warning(
                                f"{ICON} ValidationError virtual "
                                f"{virtual_event.get('block_num')} "
                                f"{virtual_event.get('trx_id')}: {e}",
                                extra={
                                    "notification": True,
                                    "virtual_event": virtual_event,
                                },
                            )
                            continue
                        op_in_trx_counter.op_in_trx_inc(op_virtual_base)
                        if op_virtual_base.op_type in opNames:
                            yield op_virtual_base

                if not filter_custom_json and not custom_json_test_data(hive_event):
                    continue

                try:
                    op_base = op_any_or_base(hive_event)
                except ValueError as e:
                    logger.warning(
                        f"{ICON} ValidationError "
                        f"{hive_event.get('block_num')} {hive_event.get('trx_id')}: {e}",
                        extra={"notification": False, "hive_event": hive_event},
                    )
                    continue

                if only_virtual_ops:
                    start_block = op_base.block_num
                    last_block = op_base.block_num

                op_in_trx_counter.op_in_trx_inc(op_base)
                last_block = op_base.block_num
                yield op_base

        except SwitchToLiveStream as e:
            logger.info(f"{ICON} {start_block:,} | {e} last={last_block:,} {_rpc_url(hive)}")
            continue
        except (asyncio.CancelledError, KeyboardInterrupt) as e:
            logger.info(f"{ICON} Async streamer stopping: {e}")
            return
        except asyncio.TimeoutError:
            await asyncio.sleep(0.1)
        except (NectarException, NumRetriesReached, UnhandledRPCError, WorkingNodeMissing) as e:
            sleep_for = 1.0
            if _is_batch_not_supported_error(e):
                batch_disabled = True
                logger.warning(
                    f"{ICON} {start_block:,} Node rejects batched calls; "
                    f"disabling max_batch_size for this session ({_rpc_url(hive)})",
                    extra={"notification": False, "error": e},
                )
                sleep_for = 0.2
            elif _is_rate_limit_error(e) or isinstance(e, NumRetriesReached):
                sleep_for = 5.0
                logger.warning(
                    f"{ICON} {start_block:,} Rate limit / retries on {_rpc_url(hive)}: {e}",
                    extra={
                        "notification": False,
                        "error": e,
                        "error_code": "stream_restart",
                    },
                )
            elif isinstance(e, WorkingNodeMissing) or "No working nodes" in str(e):
                sleep_for = 3.0
                logger.warning(
                    f"{ICON} {start_block:,} No working nodes; rebuilding Hive client",
                    extra={
                        "notification": False,
                        "error": e,
                        "error_code": "stream_restart",
                    },
                )
            elif re.search(r"Block \d+ does not exist", str(e)):
                logger.info(
                    f"{ICON} {start_block:,} Block not on node {_rpc_url(hive)}; rotating"
                )
            else:
                logger.warning(
                    f"{ICON} {start_block:,} Nectar error: {e}; restarting stream",
                    extra={
                        "notification": False,
                        "error_code": "stream_restart",
                        "error": e,
                    },
                )
            await asyncio.sleep(sleep_for)
        except StopAsyncIteration as e:
            logger.error(
                f"{ICON} {start_block:,} Stream stopped unexpectedly: {e}"
            )
        except Exception as e:
            if _is_batch_not_supported_error(e):
                batch_disabled = True
                logger.warning(
                    f"{ICON} {start_block:,} Batch not supported: {e}",
                    extra={"notification": False, "error": e},
                )
            elif _is_rate_limit_error(e):
                logger.warning(
                    f"{ICON} {start_block:,} Rate limit: {e}",
                    extra={
                        "notification": False,
                        "error": e,
                        "error_code": "stream_restart",
                    },
                )
                await asyncio.sleep(5.0)
            else:
                logger.exception(
                    f"{ICON} {start_block:,} Stream error on {_rpc_url(hive)}: {e}",
                    extra={
                        "notification": False,
                        "error": e,
                        "error_code": "stream_restart",
                    },
                )
                await asyncio.sleep(1.0)
        finally:
            if last_block >= stop_block:
                logger.info(
                    f"{ICON} {start_block:,} Reached stop block {stop_block:,}, stopping."
                )
                break

            # Catch-up / stop_now: stream finished without error — do not restart forever.
            if stream_ended_cleanly and finite_stop:
                logger.info(
                    f"{ICON} Stream finished cleanly at block {last_block:,} "
                    f"(stop={stop_block:,}); ending catch-up.",
                    extra={"notification": False},
                )
                break

            # Resume from last good block (do not re-scan the whole catch-up range).
            if last_block and last_block > start_block:
                start_block = last_block

            previous_node = rpc_url
            need_rebuild = False
            try:
                # Disable stuck node, then let Nectar pick another.
                next_url = _nectar_rotate_node(hive, disable_current=True)
                working = 0
                if hive is not None and getattr(hive, "rpc", None) is not None:
                    working = getattr(hive.rpc.nodes, "working_nodes_count", 0) or 0
                if working == 0 or next_url == previous_node:
                    need_rebuild = True
            except Exception:
                need_rebuild = True
                next_url = previous_node

            if need_rebuild:
                try:
                    hive, blockchain = _rebuild_hive_client(hive)
                    next_url = _rpc_url(hive)
                    logger.info(
                        f"{ICON} {start_block:,} Rebuilt Hive client → {next_url}",
                        extra={"notification": False},
                    )
                except Exception as e:
                    logger.warning(
                        f"{ICON} {start_block:,} Failed to rebuild Hive client: {e}",
                        extra={"notification": False, "error": e},
                    )
                    await asyncio.sleep(MAX_RESTART_BACKOFF_SECONDS)
                    continue
            else:
                # Rebind Blockchain to the same Hive instance after rotation.
                blockchain = get_blockchain_instance(hive_instance=hive)
                OpBase.hive_inst = hive

            restart_count += 1
            if next_url == previous_node:
                backoff = min(MAX_RESTART_BACKOFF_SECONDS, 0.5 * restart_count)
                logger.warning(
                    f"{ICON} {start_block:,} Still on {previous_node}; "
                    f"sleeping {backoff:.1f}s before retry",
                    extra={"notification": False, "error_code": "stream_restart"},
                )
                await asyncio.sleep(backoff)

            logger.info(
                f"{ICON} {start_block:,} Resuming from block {last_block:,} "
                f"via {next_url} (was {previous_node})",
                extra={"notification": False},
            )


def get_virtual_ops_block(block_num: int, blockchain: Blockchain):
    """
    Get a block from the blockchain. Can't use this because it doesn't process the ops the way
    the stream method does.
    """
    return blockchain.wait_for_and_get_block(block_number=block_num, only_virtual_ops=True)


async def main() -> None:
    opNames: list[str] = []
    count = 0
    hive = Hive(node=["https://rpc.podping.org"])
    async for op in stream_ops_async(
        opNames=opNames, look_back=timedelta(days=1), stop_now=True, hive=hive
    ):
        count += 1
        if count % 10_000 == 0:
            logger.info(f"{ICON} {op.block_num:,} Processed {count:,} operations")


if __name__ == "__main__":
    asyncio.run(main())
