import asyncio
import time
from contextlib import asynccontextmanager
from typing import Annotated

from pydantic import AfterValidator
from redis.asyncio.lock import Lock as RedisLock
from redis.exceptions import LockError, LockNotOwnedError

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName

LOCK_REPORTING_TIME = 5


ICON = "🔒"  # Icon to represent the lock in logs and messages


class CustIDLockException(Exception):
    """Custom exception for CustID lock acquisition failures."""

    pass


class CustID(AccName):
    """
    Customer ID class with simplified locking functionality.

    This class provides both context manager and manual methods for acquiring and
    releasing locks on customer IDs using Redis. It extends str to allow using
    the customer ID string directly.
    """

    async def acquire_lock(
        self, timeout: int | None = None, blocking_timeout: int | None = 60
    ) -> bool:
        """
        Acquire a lock for this customer ID.

        Args:
            timeout: Maximum life for the lock in seconds (None for no expiry)
            blocking_timeout: How long to wait before giving up (None waits indefinitely)

        Returns:
            bool: True if lock was acquired successfully

        Raises:
            CustIDLockException: If lock acquisition fails
        """
        lock_key = f"cust_id_lock:{self}"
        redis_instance = InternalConfig.redis_async

        try:
            # Create a Redis lock
            lock = RedisLock(
                redis_instance,
                name=lock_key,
                timeout=timeout,
                sleep=0.1,  # Check more frequently for better responsiveness
                blocking=True,
                blocking_timeout=blocking_timeout,
            )

            start_time = time.time()
            last_log_time = start_time

            # Try to acquire lock, with periodic logging
            while True:
                try:
                    acquired = await asyncio.wait_for(
                        lock.acquire(),
                        timeout=LOCK_REPORTING_TIME
                        if blocking_timeout is None
                        else min(LOCK_REPORTING_TIME, blocking_timeout),
                    )
                    if acquired:
                        # Store the lock and return success
                        logger.info(f"{ICON} Lock acquired for customer {self}")
                        return True
                except asyncio.TimeoutError:
                    # Log that we're still waiting
                    current_time = time.time()
                    if current_time - last_log_time >= LOCK_REPORTING_TIME:
                        logger.warning(
                            f"{ICON} Still waiting for lock on customer {self} after {int(current_time - start_time)} seconds...",
                            extra={"notification": False},
                        )
                        last_log_time = current_time

                    # Check if we've exceeded blocking_timeout
                    if (
                        blocking_timeout is not None
                        and (current_time - start_time) >= blocking_timeout
                    ):
                        raise CustIDLockException(
                            f"Failed to acquire lock for {self} after {blocking_timeout} seconds"
                        )

                    # Continue trying
                    continue
                except Exception as e:
                    raise CustIDLockException(f"{ICON} Error acquiring lock for {self}: {e}")

        except Exception as e:
            logger.error(f"{ICON} Error setting up lock for {self}: {e}")
            raise CustIDLockException(f"{ICON} Error setting up lock: {e}")

    @staticmethod
    async def release_lock(cust_id: str) -> bool:
        """
        Release a lock for a customer ID.

        Args:
            cust_id: Customer ID string

        Returns:
            bool: True if lock was released successfully
        """

        # If not found in active locks, create a new lock to release it
        redis_instance = InternalConfig.redis_async
        try:
            lock = RedisLock(
                redis_instance,
                name=f"cust_id_lock:{cust_id}",
                timeout=None,  # Doesn't matter for release
            )
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
        Check if a lock exists for a customer ID.

        Args:
            cust_id: Customer ID string

        Returns:
            bool: True if lock exists
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
        Clear all locks in Redis.

        This method is intended for testing purposes to ensure no locks are left hanging.
        """
        redis_instance = InternalConfig.redis_async
        try:
            keys = await redis_instance.keys("cust_id_lock:*")
            if keys:
                await redis_instance.delete(*keys)
                logger.info(f"{ICON} All customer ID locks cleared.")
            else:
                logger.info(f"{ICON} No customer ID locks found to clear.")
        except Exception as e:
            logger.error(f"{ICON} Error clearing all locks: {e}")

    @asynccontextmanager
    async def locked(self, timeout: int | None = None, blocking_timeout: int | None = 60):
        """
        Async context manager for acquiring and releasing a lock.

        Args:
            timeout: Maximum life for the lock in seconds (None for no expiry)
            blocking_timeout: How long to wait before giving up (None waits indefinitely)

        Yields:
            None

        Raises:
            CustIDLockException: If lock acquisition fails
        """
        acquired = False
        try:
            acquired = await self.acquire_lock(timeout=timeout, blocking_timeout=blocking_timeout)
            yield
        finally:
            if acquired:
                await CustID.release_lock(self)


# Annotated type with validator to cast to CustID
CustIDType = Annotated[str, AfterValidator(lambda x: CustID(x))]
