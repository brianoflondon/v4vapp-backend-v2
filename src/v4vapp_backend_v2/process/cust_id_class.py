import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

from pydantic import AfterValidator
from redis.asyncio.lock import Lock as RedisLock
from redis.exceptions import LockError, LockNotOwnedError

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName

LOCK_REPORTING_TIME = 10
ICON = "🔒"

# In-process tracking of outstanding lock waiters.
# cust_id -> request_id -> {"details": str, "started": float}
_OUTSTANDING_WAITERS: dict[str, dict[str, dict[str, float | str]]] = {}
_OUTSTANDING_LOCK = asyncio.Lock()

# Per-object rate-limit for wait warnings: cust_id -> next_allowed_epoch
_NEXT_ALLOWED_WARN: dict[str, float] = {}

_REPORTER_TASK: asyncio.Task | None = None
_REPORT_INTERVAL_SEC = 120


class CustIDLockException(Exception):
    """Custom exception for CustID lock acquisition failures."""

    pass


async def _register_waiter(cust_id: str, request_id: str, details: str) -> None:
    async with _OUTSTANDING_LOCK:
        bucket = _OUTSTANDING_WAITERS.setdefault(str(cust_id), {})
        bucket[request_id] = {"details": details, "started": time.time()}
        # Initialize throttle window if not present
        _NEXT_ALLOWED_WARN.setdefault(str(cust_id), 0.0)


async def _unregister_waiter(cust_id: str, request_id: str) -> None:
    async with _OUTSTANDING_LOCK:
        bucket = _OUTSTANDING_WAITERS.get(str(cust_id))
        if bucket and request_id in bucket:
            bucket.pop(request_id, None)
            if not bucket:
                _OUTSTANDING_WAITERS.pop(str(cust_id), None)
                _NEXT_ALLOWED_WARN.pop(str(cust_id), None)


async def _snapshot_waiters() -> dict[str, list[dict[str, float | str]]]:
    async with _OUTSTANDING_LOCK:
        return {
            cid: [
                {"details": info["details"], "started": info["started"]} for info in reqs.values()
            ]
            for cid, reqs in _OUTSTANDING_WAITERS.items()
        }


async def _log_outstanding_locks() -> None:
    # Local waiters (in-process)
    snap = await _snapshot_waiters()
    if snap:
        lines = []
        for cid, items in snap.items():
            ages = [int(time.time() - float(it["started"])) for it in items]
            oldest = max(ages) if ages else 0
            # show up to 3 details
            details = [str(it["details"]) for it in items if it.get("details")]
            preview = ", ".join(details[:3]) if details else ""
            extra_count = max(len(details) - 3, 0)
            suffix = f" (+{extra_count} more)" if extra_count > 0 else ""
            lines.append(
                f"- {cid}: {len(items)} waiter(s), oldest {oldest}s"
                f"{f' | {preview}{suffix}' if preview else ''}"
            )
        logger.info(
            f"{ICON} Outstanding lock waiters:\n" + "\n".join(lines), extra={"notification": False}
        )
    else:
        logger.info(f"{ICON} No outstanding lock waiters.", extra={"notification": False})

    # Active Redis locks (cross-process visibility)
    try:
        redis = InternalConfig.redis_async
        keys = await redis.keys("cust_id_lock:*")
        if keys:
            lines = []
            for key in keys:
                ttl_ms = await redis.pttl(key)  # -1 no expiry, -2 key does not exist
                if ttl_ms is None:
                    ttl_ms = -2
                ttl_str = (
                    "no-expiry"
                    if ttl_ms == -1
                    else (f"{ttl_ms / 1000:.0f}s" if ttl_ms >= 0 else "expired?")
                )
                lines.append(f"- {key} ttl={ttl_str}")
            logger.info(
                f"{ICON} Active Redis locks:\n" + "\n".join(lines), extra={"notification": False}
            )
        else:
            logger.info(f"{ICON} No active Redis locks.", extra={"notification": False})
    except Exception as e:
        logger.warning(f"{ICON} Failed to list Redis locks: {e}", extra={"notification": False})


async def _reporter_loop() -> None:
    while True:
        try:
            await asyncio.sleep(_REPORT_INTERVAL_SEC)
            await _log_outstanding_locks()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"{ICON} Lock reporter error: {e}", extra={"notification": False})


def start_lock_reporter() -> None:
    """Start the periodic reporter if not already started."""
    global _REPORTER_TASK
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop; caller can start later
        return
    if _REPORTER_TASK is None or _REPORTER_TASK.done():
        _REPORTER_TASK = loop.create_task(_reporter_loop(), name="custid-lock-reporter")


class CustID(AccName):
    """
    Customer ID class with simplified locking functionality.

    This class provides both context manager and manual methods for acquiring and
    releasing locks on object IDs using Redis. It extends str to allow using
    the object ID string directly.
    """

    async def acquire_lock(
        self,
        timeout: int | None = None,
        blocking_timeout: int | None = 60,
        request_details: str = "",
    ) -> bool:
        """
        Acquire a lock for this object ID.

        Args:
            timeout: Maximum life for the lock in seconds (None for no expiry)
            blocking_timeout: How long to wait before giving up (None waits indefinitely)
            request_details: Context string describing why the lock is requested.

        Returns:
            bool: True if lock was acquired successfully
        """
        # Ensure periodic reporter is running
        start_lock_reporter()

        lock_key = f"cust_id_lock:{self}"
        redis_instance = InternalConfig.redis_async

        # Track this waiter (multiple per cust_id supported)
        request_id = f"{uuid.uuid4()}"
        await _register_waiter(str(self), request_id, request_details or "")

        try:
            lock = RedisLock(
                redis_instance,
                name=lock_key,
                timeout=timeout,
                sleep=0.1,
                blocking=True,
                blocking_timeout=blocking_timeout,
            )

            start_time = time.time()
            last_log_time = start_time

            while True:
                try:
                    acquired = await asyncio.wait_for(
                        lock.acquire(),
                        timeout=LOCK_REPORTING_TIME
                        if blocking_timeout is None
                        else min(LOCK_REPORTING_TIME, blocking_timeout),
                    )
                    if acquired:
                        await _unregister_waiter(str(self), request_id)
                        logger.info(f"{ICON} Lock acquired for object {self}")
                        logger.info(f"{ICON} {request_details if request_details else ''}")
                        return True
                except asyncio.TimeoutError:
                    # Per-object deduped wait warning
                    should_log, preview, oldest = await CustID._should_log_wait(str(self))
                    if should_log:
                        # Header line once
                        logger.warning(
                            f"{ICON} Still waiting for lock on object {self} after {oldest}s...",
                            extra={"notification": False},
                        )
                        # Then one line per outstanding request
                        snap = await _snapshot_waiters()
                        waiters = snap.get(str(self), [])
                        for w in waiters:
                            details = str(w.get("details") or "").strip()
                            if details:
                                logger.warning(
                                    f"request: {details}",
                                    extra={"notification": False},
                                )

                    # Check blocking timeout
                    current_time = time.time()
                    if (
                        blocking_timeout is not None
                        and (current_time - start_time) >= blocking_timeout
                    ):
                        await _unregister_waiter(str(self), request_id)
                        raise CustIDLockException(
                            f"Failed to acquire lock for {self} after {blocking_timeout} seconds"
                        )
                    continue
                except Exception as e:
                    await _unregister_waiter(str(self), request_id)
                    raise CustIDLockException(f"{ICON} Error acquiring lock for {self}: {e}")

        except Exception as e:
            logger.error(f"{ICON} Error setting up lock for {self}: {e}")
            await _unregister_waiter(str(self), request_id)
            raise CustIDLockException(f"{ICON} Error setting up lock: {e}")

    @staticmethod
    async def release_lock(cust_id: str) -> bool:
        """
        Release a lock for a object ID.

        Args:
            cust_id: Customer ID string

        Returns:
            bool: True if lock was released successfully
        """
        redis_instance = InternalConfig.redis_async
        try:
            lock = RedisLock(redis_instance, name=f"cust_id_lock:{cust_id}", timeout=None)
        except Exception as e:
            logger.error(f"{ICON} Error creating lock for release {cust_id}: {e}")
            return False

        try:
            await lock.release()
            logger.info(f"{ICON} Lock released for {cust_id}")
            return True
        except LockNotOwnedError:
            logger.warning(f"{ICON} Lock for {cust_id} was not owned by this process")
            return False
        except LockError as e:
            if "Cannot release an unlocked lock" in str(e):
                await redis_instance.delete(f"cust_id_lock:{cust_id}")
                logger.info(f"{ICON} Release already expired lock for {cust_id}")
                return True
            else:
                logger.error(f"{ICON} Lock error for {cust_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"{ICON} Error releasing lock for {cust_id}: {e}")
            return False

    @staticmethod
    async def check_lock_exists(cust_id: str) -> bool:
        """
        Check if a lock exists for a object ID.
        """
        lock_key = f"cust_id_lock:{cust_id}"
        redis_instance = InternalConfig.redis_async
        try:
            exists = await redis_instance.exists(lock_key)
            return bool(exists)
        except Exception as e:
            logger.error(f"{ICON} Error checking lock existence for {cust_id}: {e}")
            return False

    @staticmethod
    async def clear_all_locks() -> None:
        """
        Clear all locks in Redis. Intended for testing.
        """
        redis_instance = InternalConfig.redis_async
        try:
            keys = await redis_instance.keys("cust_id_lock:*")
            if keys:
                await redis_instance.delete(*keys)
                logger.info(f"{ICON} All object ID locks cleared.")
            else:
                logger.info(f"{ICON} No object ID locks found to clear.")
        except Exception as e:
            logger.error(f"{ICON} Error clearing all locks: {e}")

    @asynccontextmanager
    async def locked(
        self,
        timeout: int | None = None,
        blocking_timeout: int | None = 60,
        request_details: str = "",
    ):
        """
        Async context manager for acquiring and releasing a lock.
        """
        acquired = False
        try:
            acquired = await self.acquire_lock(
                timeout=timeout,
                blocking_timeout=blocking_timeout,
                request_details=request_details,
            )
            yield
        finally:
            if acquired:
                await CustID.release_lock(self)

    @staticmethod
    async def _should_log_wait(cust_id: str) -> tuple[bool, str, int]:
        """
        Return (should_log, preview_details, oldest_age_s) for a given object id,
        enforcing a single log per LOCK_REPORTING_TIME across all waiters.
        """
        now = time.time()
        async with _OUTSTANDING_LOCK:
            next_allowed = _NEXT_ALLOWED_WARN.get(cust_id, 0.0)
            if now < next_allowed:
                return False, "", 0
            _NEXT_ALLOWED_WARN[cust_id] = now + LOCK_REPORTING_TIME

            reqs = list(_OUTSTANDING_WAITERS.get(cust_id, {}).values())
            ages = [int(now - float(it["started"])) for it in reqs]
            oldest = max(ages) if ages else 0
            details = [str(it.get("details") or "") for it in reqs if it.get("details")]
            preview = ", ".join(details[:3]) if details else ""
            if len(details) > 3:
                preview += f" (+{len(details) - 3} more)"
            return True, preview, oldest


# Annotated type with validator to cast to CustID
CustIDType = Annotated[str, AfterValidator(lambda x: CustID(x))]
