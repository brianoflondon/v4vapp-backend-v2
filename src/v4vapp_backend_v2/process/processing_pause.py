"""Temporary pause for db_monitor change-stream processing.

The admin screen writes a Redis flag and publishes ``pause`` or ``unpause``
on a control channel. ``db_monitor`` treats that flag as the source of truth:
while it is set, change-stream watches are closed and their resume tokens are
left where they are, so Hive, Lightning, and other database events wait in
MongoDB. An ``unpause`` event (or the flag disappearing) opens the watches
again from those tokens.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from v4vapp_backend_v2.config.setup import InternalConfig, logger

PAUSE_MESSAGE = "pause"
UNPAUSE_MESSAGE = "unpause"
POLL_SECONDS = 1.0

_pause_event = asyncio.Event()
_resume_event = asyncio.Event()


def pause_redis_key() -> str:
    """Redis key whose presence means database processing is paused."""
    dbs = InternalConfig().config.dbs_config
    return f"db_monitor:processing_paused:{dbs.default_connection}:{dbs.default_name}"


def pause_channel() -> str:
    """Redis pub/sub channel that carries pause and unpause events."""
    return f"{pause_redis_key()}:events"


def is_processing_paused() -> bool:
    """Return the in-process pause flag. This does not read Redis."""
    return _pause_event.is_set()


def set_local_pause(paused: bool, reason: str) -> None:
    """Record a pause transition inside this process and wake waiters.

    A repeated call with the same state does nothing, so a one-second flag
    poll does not send a notification on every check.
    """
    if paused == _pause_event.is_set():
        return
    if paused:
        # Clear the resume event before advertising the pause so a waiter
        # cannot observe a stale "resumed" signal.
        _resume_event.clear()
        _pause_event.set()
        logger.warning(
            "⏸️ Processing paused "
            f"({reason}). db_monitor will stop watching database changes. "
            "Hive, Lightning, and other events will be processed after unpause. "
            "Changes already in progress will finish.",
            extra={"notification": True, "processing_paused": True},
        )
        return
    _pause_event.clear()
    _resume_event.set()
    logger.info(
        f"▶️ Processing unpaused ({reason}). db_monitor will resume database changes.",
        extra={"notification": True, "processing_paused": False},
    )


def reset_processing_pause_for_tests() -> None:
    """Clear the in-process pause events. Tests only."""
    _pause_event.clear()
    _resume_event.clear()


async def get_processing_pause() -> dict[str, Any]:
    """Read the pause flag. A missing key means processing is running."""
    raw = await InternalConfig.redis_async.get(pause_redis_key())
    if raw:
        return {"paused": True, "paused_at": str(raw)}
    return {"paused": False, "paused_at": None}


async def set_processing_paused(paused: bool) -> dict[str, Any]:
    """Write or remove the Redis flag, then publish the matching event.

    The key is updated before the publish so a subscriber that reads the flag
    after it receives the event sees the new state.
    """
    redis = InternalConfig.redis_async
    key = pause_redis_key()
    channel = pause_channel()
    if paused:
        paused_at = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        await redis.set(key, paused_at)
        await redis.publish(channel, PAUSE_MESSAGE)
        logger.warning(
            f"⏸️ Processing pause requested. Flag {key} set at {paused_at}.",
            extra={"notification": True, "processing_paused": True},
        )
        return {"paused": True, "paused_at": paused_at}

    await redis.delete(key)
    await redis.publish(channel, UNPAUSE_MESSAGE)
    logger.info(
        f"▶️ Processing unpause requested. Flag {key} cleared.",
        extra={"notification": True, "processing_paused": False},
    )
    return {"paused": False, "paused_at": None}


async def apply_pause_flag_from_redis(reason: str) -> None:
    """Copy the Redis flag into the in-process pause event.

    A failed read leaves the current state alone so a Redis blip does not
    pause a running monitor or resume a paused one.
    """
    try:
        state = await get_processing_pause()
    except Exception as exc:
        logger.error(
            f"⏸️ Could not read the processing pause flag ({reason}): {exc}",
            extra={"notification": False},
        )
        return
    set_local_pause(bool(state["paused"]), reason=reason)


def _control_reason(message: dict[str, Any] | None) -> str:
    if not message:
        return "redis flag"
    data = message.get("data")
    if isinstance(data, bytes):
        data = data.decode()
    if data == UNPAUSE_MESSAGE:
        return "unpause event"
    if data == PAUSE_MESSAGE:
        return "pause event"
    return "redis flag"


async def watch_processing_pause(shutdown_event: asyncio.Event) -> None:
    """Listen for pause and unpause events until shutdown.

    Pub/sub delivers the event immediately. A one-second read of the flag
    covers a missed message, which Redis does not keep for a disconnected
    subscriber.
    """
    while not shutdown_event.is_set():
        pubsub = None
        try:
            pubsub = InternalConfig.redis_async.pubsub()
            await pubsub.subscribe(pause_channel())
            while not shutdown_event.is_set():
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=POLL_SECONDS,
                )
                await apply_pause_flag_from_redis(_control_reason(message))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                f"⏸️ Processing pause listener failed: {exc}",
                extra={"notification": False},
            )
            with suppress(TimeoutError):
                await asyncio.wait_for(shutdown_event.wait(), timeout=POLL_SECONDS)
        finally:
            if pubsub is not None:
                with suppress(Exception):
                    await pubsub.aclose()


async def _wait_for_pause_or_shutdown(shutdown_event: asyncio.Event) -> None:
    """Block until the local pause or shutdown event is set, or the poll interval ends."""
    if is_processing_paused() or shutdown_event.is_set():
        return
    pause_task = asyncio.create_task(_pause_event.wait())
    stop_task = asyncio.create_task(shutdown_event.wait())
    try:
        await asyncio.wait(
            {pause_task, stop_task},
            timeout=POLL_SECONDS,
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        for task in (pause_task, stop_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(pause_task, stop_task, return_exceptions=True)


async def wait_until_paused_or_shutdown(shutdown_event: asyncio.Event) -> None:
    """Return when processing is paused or the process is shutting down.

    The local event is the fast path. The Redis flag is checked after each
    quiet interval so a paused flag still stops the watches if the pub/sub
    listener is down.
    """
    while not is_processing_paused() and not shutdown_event.is_set():
        await _wait_for_pause_or_shutdown(shutdown_event)
        if is_processing_paused() or shutdown_event.is_set():
            return
        try:
            state = await get_processing_pause()
        except Exception as exc:
            logger.warning(
                f"⏸️ Processing pause flag read failed: {exc}",
                extra={"notification": False},
            )
            continue
        if state["paused"]:
            set_local_pause(True, reason="pause flag")
            return


async def wait_while_paused(shutdown_event: asyncio.Event, collection_name: str) -> None:
    """Stay here until an unpause event arrives or shutdown is requested.

    The resume event is set by the pause listener. The Redis flag is checked
    on each quiet interval so a cleared flag still resumes the stream if the
    unpause publish was missed.
    """
    if not is_processing_paused() or shutdown_event.is_set():
        return
    logger.info(
        f"⏸️ {collection_name} change stream is paused. Waiting for an unpause event.",
        extra={"notification": False},
    )
    while is_processing_paused() and not shutdown_event.is_set():
        resume_task = asyncio.create_task(_resume_event.wait())
        stop_task = asyncio.create_task(shutdown_event.wait())
        try:
            await asyncio.wait(
                {resume_task, stop_task},
                timeout=POLL_SECONDS,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (resume_task, stop_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(resume_task, stop_task, return_exceptions=True)
        if shutdown_event.is_set() or not is_processing_paused():
            break
        try:
            state = await get_processing_pause()
        except Exception as exc:
            logger.warning(
                f"⏸️ Processing pause flag read failed: {exc}",
                extra={"notification": False},
            )
            continue
        if not state["paused"]:
            set_local_pause(False, reason="unpause event")
            break
    if shutdown_event.is_set():
        return
    logger.info(
        f"▶️ {collection_name} unpause event received. Resuming the change stream.",
        extra={"notification": False},
    )
