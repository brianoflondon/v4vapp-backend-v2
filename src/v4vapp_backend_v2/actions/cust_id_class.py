import asyncio
from contextlib import asynccontextmanager
from random import random
from typing import Annotated

from pydantic import AfterValidator
from redis.asyncio.lock import Lock as RedisLock
from redis.exceptions import LockError, LockNotOwnedError

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class CustIDLockException(Exception):
    """Custom exception for CustID lock acquisition failures."""

    pass


class CustID(str):
    # Class-level storage for tracking locks across instances
    _locks: dict[str, object] = {}  # Format: {"{cust_id}:{group_id}": redis_lock_instance}

    async def wait_for_lock(
        self,
        group_id: str = "default",
        check_interval: float = 0.5,
        max_attempts: int | None = None,
        timeout: int | None = None,  # Lock TTL once acquired
    ) -> bool:
        """
        Blocks and waits for a lock to become free, then acquires it.

        Args:
            group_id: Unique identifier for the operation group
            check_interval: How long to wait between checks (seconds)
            max_attempts: Maximum number of attempts (None for unlimited)
            timeout: TTL for the lock once acquired

        Returns:
            bool: True when lock is acquired

        Usage:
            cust_id = CustID("user123")
            # This will block until the lock is available
            await cust_id.wait_for_lock(group_id="payment_123")
            try:
                # Do your protected operations here
                await process_payment()
            finally:
                # Always release the lock when done
                await CustID.release_lock("user123", "payment_123")
        """
        redis_instance = InternalConfig.redis_async
        lock_key = f"cust_id_lock:{self}:{group_id}"
        attempts = 0

        logger.info(f"Waiting for lock: {lock_key}")

        while max_attempts is None or attempts < max_attempts:
            try:
                async with redis_instance as redis:
                    # Try to acquire the lock
                    acquired = await redis.set(
                        lock_key,
                        "1",
                        nx=True,  # Only set if key doesn't exist
                        ex=timeout,
                    )

                    if acquired:
                        logger.info(f"Lock acquired after {attempts} attempts: {lock_key}")
                        # Store in class registry for later release
                        CustID._locks = getattr(CustID, "_locks", {})
                        CustID._locks[lock_key] = True
                        return True

                # Lock is taken, wait before retrying
                attempts += 1
                if attempts % 10 == 0:  # Log every 10 attempts
                    logger.info(f"Still waiting for lock after {attempts} attempts: {lock_key}")

                await asyncio.sleep(check_interval)

            except Exception as e:
                logger.error(f"Error while waiting for lock {lock_key}: {e}")
                await asyncio.sleep(check_interval)

        logger.error(f"Failed to acquire lock after {attempts} attempts: {lock_key}")
        return False

    async def acquire_lock(
        self,
        group_id: str,
        timeout: int | None = None,
        sleep: float = 0.5,
        blocking_timeout: int | None = None,
    ) -> bool:
        """
        Manually acquire a lock for this customer ID with a specific group_id.

        Args:
            group_id: Unique identifier for the operation group
            timeout: Maximum life for the lock in seconds (None for no expiry)
            sleep: How long to sleep between acquisition attempts
            blocking_timeout: How long to wait before giving up

        Returns:
            bool: True if lock was acquired successfully

        Raises:
            CustIDLockException: If lock acquisition fails
        """
        lock_key = f"cust_id_lock:{self}:{group_id}"
        redis_instance = InternalConfig.redis_async
        try:
            lock = RedisLock(
                redis_instance,
                name=lock_key,
                timeout=timeout,
                sleep=sleep,
                blocking=True,
                blocking_timeout=blocking_timeout,
            )

            acquired = await lock.acquire()
            if not acquired:
                raise CustIDLockException(
                    f"Failed to acquire lock for {self} with group {group_id}"
                )

            # Store the lock in class-level storage
            CustID._locks[lock_key] = lock
            logger.info(f"Lock acquired for {self} with group {group_id}")
            return True

        except Exception as e:
            logger.error(f"Error acquiring lock for {self} with group {group_id}: {e}")
            raise CustIDLockException(f"Error acquiring lock: {e}")
        finally:
            # Ensure we always close the Redis connection
            pass

    @staticmethod
    async def release_lock(cust_id: str, group_id: str) -> bool:
        """
        Release a lock for a customer ID and group_id. This static method
        allows releasing locks from different processes.

        Args:
            cust_id: Customer ID string
            group_id: The group_id used when acquiring the lock

        Returns:
            bool: True if lock was released successfully
        """
        lock_key = f"cust_id_lock:{cust_id}:{group_id}"

        # Check if we have this lock in our registry
        lock = CustID._locks.get(lock_key)

        # If not in registry or lock is not a RedisLock, create a new RedisLock object to release it
        if not isinstance(lock, RedisLock):
            redis_instance = InternalConfig.redis_async
            try:
                lock = RedisLock(
                    redis_instance,
                    name=lock_key,
                    timeout=None,  # Doesn't matter for release
                )
            except Exception as e:
                logger.error(
                    f"Error creating lock for release {cust_id} with group {group_id}: {e}"
                )
                return False

        try:
            await lock.release()
            # Remove from registry if present
            if lock_key in CustID._locks:
                del CustID._locks[lock_key]
            logger.info(f"Lock released for {cust_id} with group {group_id}")
            return True
        except LockNotOwnedError:
            logger.warning(
                f"Lock for {cust_id} with group {group_id} was not owned by this process"
            )
            return False
        except LockError as e:
            if "Cannot release an unlocked lock" in str(e):
                logger.debug(
                    f"Silently ignoring un-owned lock release for {cust_id} with group {group_id}"
                )
                return True
            else:
                logger.error(
                    f"Some other error releasing lock for {cust_id} with group {group_id}: {e}",
                    extra={"notification": False, "cust_id": cust_id, "group_id": group_id},
                )
            return False

        except Exception as e:
            logger.exception(e)
            logger.error(f"Error releasing lock for {cust_id} with group {group_id}: {e}")
            return False

    @staticmethod
    async def check_lock_exists(cust_id: str, group_id: str) -> bool:
        """
        Check if a lock exists for a customer ID and group_id.

        Args:
            cust_id: Customer ID string
            group_id: Group ID to check

        Returns:
            bool: True if lock exists
        """
        lock_key = f"cust_id_lock:{cust_id}:{group_id}"
        redis_instance = InternalConfig.redis_async

        try:
            exists = await redis_instance.exists(lock_key)
            return bool(exists)
        except Exception as e:
            logger.error(f"Error checking lock existence for {cust_id} with group {group_id}: {e}")
            return False

    @asynccontextmanager
    async def locked(
        self,
        group_id: str = "default",
        timeout: int | None = None,
        sleep: float = 0.5,
        blocking_timeout: int | None = None,
    ):
        """
        Asynchronous context manager for acquiring and releasing a lock.

        This method provides backward compatibility for locking mechanisms. It acquires a lock for the specified
        group and ensures the lock is released upon exiting the context. For cross-process locking, prefer using
        `acquire_lock` and `release_lock` directly.

        Args:
            group_id (str): Identifier for the lock group. Defaults to "default".
            timeout (int | None): Maximum time in seconds to wait for acquiring the lock. If None, wait indefinitely.
            sleep (float): Interval in seconds between lock acquisition attempts. Defaults to 0.5.
            blocking_timeout (int | None): Maximum time in seconds to block while waiting for the lock. If None, block indefinitely.

        Yields:
            None

        Raises:
            Any exceptions raised by `acquire_lock` or `release_lock`.

        """
        acquired = False
        try:
            acquired = await self.acquire_lock(
                group_id=group_id, timeout=timeout, sleep=sleep, blocking_timeout=blocking_timeout
            )
            yield
        finally:
            if acquired:
                await CustID.release_lock(self, group_id)


# Annotated type with validator to cast to CustID
CustIDType = Annotated[str, AfterValidator(lambda x: CustID(x))]


# Example usage in an async function
async def process_customer(customer_id: CustID, comment: str = ""):
    try:
        logger.info(f"Starting processing for customer {customer_id} {comment}")

        async with customer_id.locked(timeout=None, blocking_timeout=None, group_id="processing"):
            # This is the critical section where the lock is held
            logger.info(
                f"Lock acquired for {customer_id}. Performing exclusive operations... {comment}"
            )
            # Simulate some work that requires exclusive access
            await asyncio.sleep(2 + random() * 10)  # Replace with actual async operations
            logger.info(f"{comment} for {customer_id}")
            logger.info(f"Operations completed for {customer_id}. Releasing lock... {comment}")

        logger.info(f"Processing finished for {customer_id}")
    except CustIDLockException as e:
        logger.error(f"Could not acquire lock for {customer_id}: {e}")
