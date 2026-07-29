"""
Async Hive operation streaming.

Wraps Nectar's synchronous ``Blockchain.stream`` with async iteration and
resumes after failures.

Critical design notes (learned the hard way):

- Nectar defaults (timeout=60s, num_retries=100) can block a single RPC for
  tens of minutes. Streaming always uses fail-fast Hive settings.
- ``asyncio.wait_for`` does **not** cancel a worker thread already inside
  ``next()``. Abandoned workers keep retrying; recovery must **not** call
  ``rpc.next()`` on that same client (shared session / pool contention).
- Always **rebuild** a fresh Hive client on recovery instead of rotating the
  poisoned one.
- Real-op and virtual-op streams must not share one RPC client — concurrent
  ``next()`` on the same session is how we deadlocked after virtual-ops timeout.
- All recovery RPCs run via ``asyncio.to_thread`` with a hard asyncio budget.
"""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Callable, TypeVar

from nectar.blockchain import Blockchain
from nectar.exceptions import NectarException
from nectar.hive import Hive
from nectarapi.exceptions import NumRetriesReached, UnhandledRPCError, WorkingNodeMissing

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.async_wrapper import sync_to_async_iterable
from v4vapp_backend_v2.hive.hive_extras import (
    get_blockchain_instance,
    make_stream_hive,
)
from v4vapp_backend_v2.hive_models.custom_json_data import custom_json_test_data
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_base import OP_TRACKED, OpBase, op_realm
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter

ICON = "🔗"

# Hive blocks ~3s; filtered streams can be quiet for several blocks.
STREAM_TIMEOUT_SECONDS = 15
# Virtual-ops fetch is a one-block scan on a *separate* client; keep tight.
VIRTUAL_OPS_TIMEOUT_SECONDS = 8
# Hard budget for any single sync Nectar call run in a worker thread.
SYNC_RPC_BUDGET_SECONDS = 25
MAX_RESTART_BACKOFF_SECONDS = 8
# Catch-up only: far-behind scans may use batched get_block_range when the node
# supports it. Live / near-head always uses max_batch_size=None.
CATCHUP_BATCH_SIZE = 50
CATCHUP_NEAR_HEAD_BLOCKS = 50
QUOTE_REFRESH_MIN_INTERVAL_SECONDS = 30

T = TypeVar("T")


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


def _hive_keys(hive: Hive | None) -> Any:
    if hive is None:
        return None
    return getattr(hive, "keys", None) or None


def _rebuild_hive_client(hive: Hive | None) -> tuple[Hive, Blockchain]:
    """
    Build a fresh fail-fast Hive client, abandoning any poisoned pool/session.

    Preserves memo keys from the previous client when available.
    """
    new_hive = make_stream_hive(keys=_hive_keys(hive))
    blockchain = get_blockchain_instance(hive_instance=new_hive)
    OpBase.hive_inst = new_hive
    return new_hive, blockchain


def _ensure_stream_hive(hive: Hive | None) -> Hive:
    """Use fail-fast settings; rebuild if caller passed a default (slow) client."""
    if hive is None:
        return make_stream_hive()
    timeout = getattr(getattr(hive, "rpc", None), "timeout", None)
    try:
        if timeout is not None and float(timeout) <= 20:
            return hive
        if timeout is None:
            return hive
    except (TypeError, ValueError):
        return hive
    logger.info(
        f"{ICON} Replacing Hive client (timeout={timeout}) with fail-fast stream settings",
        extra={"notification": False},
    )
    return make_stream_hive(keys=_hive_keys(hive))


async def _run_sync(
    fn: Callable[..., T],
    *args: Any,
    timeout: float = SYNC_RPC_BUDGET_SECONDS,
    label: str = "rpc",
    **kwargs: Any,
) -> T:
    """Run a blocking Nectar call off the event loop with a hard timeout."""

    def _call() -> T:
        return fn(*args, **kwargs)

    try:
        return await asyncio.wait_for(asyncio.to_thread(_call), timeout=timeout)
    except asyncio.TimeoutError as e:
        logger.warning(
            f"{ICON} Sync {label} timed out after {timeout:.0f}s "
            f"(Nectar may still be retrying in a worker thread — client will be discarded)",
            extra={"notification": False, "error_code": "stream_restart"},
        )
        raise asyncio.TimeoutError(f"{label} timed out after {timeout:.0f}s") from e


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

    Failover strategy:
      1. On any stream idle/error, **discard** the Hive client and rebuild fresh.
         Do not call ``rpc.next()`` on a client that may still have zombie ``next()``
         workers (that was the multi-minute silent stall).
      2. Resume from the last successfully processed block number.
      3. Virtual ops use a **separate** Hive client so they cannot block/poison
         the real-op stream session.
    """
    hive = _ensure_stream_hive(hive)
    blockchain = get_blockchain_instance(hive_instance=hive)
    OpBase.hive_inst = hive
    # Dedicated client for virtual-ops only — never share RPC with the live stream.
    virtual_hive = make_stream_hive(keys=_hive_keys(hive))
    virtual_blockchain = get_blockchain_instance(hive_instance=virtual_hive)

    if opNames:
        op_realms = [op_realm(op_type) for op_type in opNames]
        only_virtual_ops = all(realm == "virtual" for realm in op_realms)
    else:
        only_virtual_ops = False

    try:
        current_block = await _run_sync(
            blockchain.get_current_block_num, label="get_current_block_num"
        )
    except Exception as e:
        logger.warning(
            f"{ICON} get_current_block_num failed at stream start: {e}; rebuilding client",
            extra={"notification": False, "error": e, "error_code": "stream_restart"},
        )
        hive, blockchain = await _run_sync(
            _rebuild_hive_client, hive, label="rebuild_hive_client"
        )
        virtual_hive = make_stream_hive(keys=_hive_keys(hive))
        virtual_blockchain = get_blockchain_instance(hive_instance=virtual_hive)
        current_block = await _run_sync(
            blockchain.get_current_block_num, label="get_current_block_num"
        )

    time_now = datetime.now(tz=timezone.utc)
    start_time = time_now

    if look_back:
        start_time = time_now - look_back
        try:
            start_block = await _run_sync(
                blockchain.get_estimated_block_num,
                start_time,
                label="get_estimated_block_num",
            )
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
    batch_disabled = False
    last_quote_refresh_at: datetime | None = None
    finite_stop = stop is not None or stop_now
    stream_ended_cleanly = False
    # After virtual-ops trouble, skip a few blocks of virtual fetch to protect live stream.
    virtual_ops_skip_until_block = 0

    async def maybe_refresh_quote() -> None:
        nonlocal last_quote_refresh_at
        now = datetime.now(tz=timezone.utc)
        if last_quote_refresh_at and (now - last_quote_refresh_at) < timedelta(
            seconds=QUOTE_REFRESH_MIN_INTERVAL_SECONDS
        ):
            return
        try:
            await asyncio.wait_for(
                TrackedBaseModel.update_quote(),
                timeout=SYNC_RPC_BUDGET_SECONDS,
            )
        except Exception as e:
            logger.warning(
                f"{ICON} Quote refresh failed (continuing stream): {e}",
                extra={"notification": False},
            )
        last_quote_refresh_at = now

    async def choose_batch_size() -> int | None:
        if batch_disabled or only_virtual_ops:
            return None
        try:
            head_now = await _run_sync(
                blockchain.get_current_block_num,
                timeout=min(15.0, SYNC_RPC_BUDGET_SECONDS),
                label="choose_batch_size/head",
            )
        except Exception:
            return None
        gap = head_now - start_block
        if gap >= CATCHUP_NEAR_HEAD_BLOCKS:
            return CATCHUP_BATCH_SIZE
        return None

    async def close_async_stream(agen: Any) -> None:
        """Best-effort close; abandoned next() threads may still run until Nectar exits."""
        aclose = getattr(agen, "aclose", None)
        if aclose is None:
            return
        try:
            await asyncio.wait_for(aclose(), timeout=1.0)
        except Exception:
            pass

    def _collect_virtual_ops_sync(block_num: int) -> list[dict]:
        """Run on a worker thread against the dedicated virtual client only."""
        events: list[dict] = []
        for event in virtual_blockchain.stream(
            start=block_num,
            stop=block_num,
            raw_ops=False,
            only_virtual_ops=True,
            max_batch_size=None,
            threading=False,
        ):
            events.append(event)
        return events

    async def fetch_virtual_ops_for_block(block_num: int) -> list[dict]:
        """Fetch virtual ops without touching the live stream's Hive client."""
        nonlocal virtual_hive, virtual_blockchain
        try:
            return await _run_sync(
                _collect_virtual_ops_sync,
                block_num,
                timeout=VIRTUAL_OPS_TIMEOUT_SECONDS,
                label=f"virtual_ops@{block_num}",
            )
        except Exception as e:
            logger.warning(
                f"{ICON} Virtual-ops fetch failed for block {block_num:,} "
                f"on {_rpc_url(virtual_hive)}: {e}; rebuilding virtual client only",
                extra={"notification": False, "error_code": "stream_restart"},
            )
            try:
                virtual_hive = make_stream_hive(keys=_hive_keys(hive))
                virtual_blockchain = get_blockchain_instance(hive_instance=virtual_hive)
            except Exception as rebuild_err:
                logger.warning(
                    f"{ICON} Virtual client rebuild failed: {rebuild_err}",
                    extra={"notification": False},
                )
            return []

    while last_block is not None and stop_block is not None and last_block < stop_block:
        stream_ended_cleanly = False
        await maybe_refresh_quote()
        rpc_url = _rpc_url(hive)
        try:
            effective_batch = await choose_batch_size()
        except asyncio.TimeoutError:
            effective_batch = None
        async_stream_real = None
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
                        f"on {rpc_url}; discarding client and rebuilding "
                        f"(in-flight next() may still retry on the old client)",
                        extra={"notification": False, "error_code": "stream_restart"},
                    )
                    await close_async_stream(async_stream_real)
                    async_stream_real = None
                    raise

                # Interleave virtual ops for the previous block when the stream advances.
                if (
                    not only_virtual_ops
                    and hive_event["block_num"] > last_block
                    and hive_event["block_num"] <= stop_block
                    and last_block >= virtual_ops_skip_until_block
                ):
                    start_block = last_block
                    virtual_block = last_block - 1
                    virtual_events = await fetch_virtual_ops_for_block(virtual_block)
                    if not virtual_events and virtual_block > 0:
                        # Brief cooldown so a bad virtual node doesn't stall every block.
                        virtual_ops_skip_until_block = last_block + 5
                    for virtual_event in virtual_events:
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
                sleep_for = 3.0
                logger.warning(
                    f"{ICON} {start_block:,} Rate limit / retries on {_rpc_url(hive)}: {e}",
                    extra={
                        "notification": False,
                        "error": e,
                        "error_code": "stream_restart",
                    },
                )
            elif isinstance(e, WorkingNodeMissing) or "No working nodes" in str(e):
                sleep_for = 2.0
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
                    f"{ICON} {start_block:,} Block not on node {_rpc_url(hive)}; rebuilding"
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
            logger.error(f"{ICON} {start_block:,} Stream stopped unexpectedly: {e}")
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
                await asyncio.sleep(3.0)
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
            if async_stream_real is not None:
                await close_async_stream(async_stream_real)

            if last_block >= stop_block:
                logger.info(
                    f"{ICON} {start_block:,} Reached stop block {stop_block:,}, stopping."
                )
                break

            if stream_ended_cleanly and finite_stop:
                logger.info(
                    f"{ICON} Stream finished cleanly at block {last_block:,} "
                    f"(stop={stop_block:,}); ending catch-up.",
                    extra={"notification": False},
                )
                break

            if last_block and last_block > start_block:
                start_block = last_block

            previous_node = rpc_url
            logger.info(
                f"{ICON} {start_block:,} Recovering stream (last_block={last_block:,}, "
                f"was {previous_node}) — abandoning client, rebuilding fresh…",
                extra={"notification": False},
            )
            # Never rpc.next() on the poisoned client: zombie next() workers still
            # hold that session and next()/rotate can contend for minutes.
            try:
                hive, blockchain = await _run_sync(
                    _rebuild_hive_client,
                    hive,
                    timeout=SYNC_RPC_BUDGET_SECONDS,
                    label="rebuild_hive_client",
                )
                virtual_hive = make_stream_hive(keys=_hive_keys(hive))
                virtual_blockchain = get_blockchain_instance(hive_instance=virtual_hive)
                next_url = _rpc_url(hive)
                logger.info(
                    f"{ICON} {start_block:,} Rebuilt Hive client → {next_url}",
                    extra={"notification": False},
                )
            except Exception as e:
                restart_count += 1
                backoff = min(MAX_RESTART_BACKOFF_SECONDS, 0.5 * restart_count)
                logger.warning(
                    f"{ICON} {start_block:,} Rebuild failed ({e}); "
                    f"sleeping {backoff:.1f}s before retry",
                    extra={"notification": False, "error_code": "stream_restart"},
                )
                await asyncio.sleep(backoff)
                try:
                    # Last resort: construct on a short budget; if this fails, loop again.
                    hive, blockchain = await _run_sync(
                        _rebuild_hive_client,
                        hive,
                        timeout=15.0,
                        label="rebuild_hive_client_retry",
                    )
                    virtual_hive = make_stream_hive(keys=_hive_keys(hive))
                    virtual_blockchain = get_blockchain_instance(hive_instance=virtual_hive)
                    next_url = _rpc_url(hive)
                except Exception as e2:
                    logger.warning(
                        f"{ICON} {start_block:,} Rebuild retry failed: {e2}",
                        extra={"notification": False},
                    )
                    continue

            restart_count += 1
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
    hive = make_stream_hive()
    async for op in stream_ops_async(
        opNames=opNames, look_back=timedelta(days=1), stop_now=True, hive=hive
    ):
        count += 1
        if count % 10_000 == 0:
            logger.info(f"{ICON} {op.block_num:,} Processed {count:,} operations")


if __name__ == "__main__":
    asyncio.run(main())
