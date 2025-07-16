import asyncio
from contextlib import asynccontextmanager
from random import random
from typing import Annotated, Optional

from pydantic import AfterValidator
from redis.asyncio.lock import Lock as RedisLock
from redis.exceptions import LockNotOwnedError

from v4vapp_backend_v2.config.setup import (
    logger,  # Assuming this is needed if not already imported
)
from v4vapp_backend_v2.database.async_redis import (
    V4VAsyncRedis,  # Assuming the import path; adjust if necessary
)


class CustIDLockException(Exception):
    """Custom exception for CustID lock acquisition failures."""

    pass


class CustID(str):
    @asynccontextmanager
    async def locked(
        self,
        timeout: Optional[float] = 60,
        sleep: float = 0.1,
        blocking_timeout: Optional[float] = 5,
    ):
        """
        Asynchronous context manager to acquire a lock for this customer ID using Redis.
        Waits for the lock to be available if it's held by another process.

        Args:
            timeout (Optional[float]): The maximum life for the lock in seconds. Defaults to 60.
                                       If None, the lock does not expire.
            sleep (float): How long to sleep in seconds between attempts to acquire the lock. Defaults to 0.1.
            blocking_timeout (Optional[float]): How long to wait in seconds before giving up on acquiring the lock.
                                                If None, wait indefinitely.

        Raises:
            RuntimeError: If the lock could not be acquired within the blocking_timeout.

        Usage:
            cust = CustID("customer123")
            async with cust.locked():
                # Critical section code here
        """
        redis_instance = V4VAsyncRedis()
        try:
            # Manually enter: ping to ensure connection
            redis = redis_instance.redis  # Access the async Redis client

            lock = RedisLock(
                redis,
                name=f"cust_id_lock:{self}",
                timeout=timeout,
                sleep=sleep,
                blocking=True,
                blocking_timeout=blocking_timeout,
            )
            acquired = await lock.acquire()
            if not acquired:
                raise CustIDLockException(f"Failed to acquire lock for {self}")
            try:
                yield
            finally:
                try:
                    await lock.release()
                except LockNotOwnedError:
                    logger.info(f"Lock for {self} was not owned, cannot release.")
                except Exception as e:
                    logger.error(f"Error releasing lock for {self}: {e}")
        except Exception as e:
            logger.error(f"Error acquiring lock for {self}: {e}")
            raise CustIDLockException(f"Error acquiring lock for {self}: {e}")


# Annotated type with validator to cast to CustID
CustIDType = Annotated[str, AfterValidator(lambda x: CustID(x))]


# Example usage in an async function
async def process_customer(customer_id: CustID, comment: str = ""):
    try:
        logger.info(f"Starting processing for customer {customer_id} {comment}")

        async with customer_id.locked(timeout=None, blocking_timeout=None):
            # This is the critical section where the lock is held
            logger.info(
                f"Lock acquired for {customer_id}. Performing exclusive operations... {comment}"
            )
            # Simulate some work that requires exclusive access
            await asyncio.sleep(random() * 3)  # Replace with actual async operations
            logger.info(f"{comment} for {customer_id}")
            logger.info(f"Operations completed for {customer_id}. Releasing lock... {comment}")

        logger.info(f"Processing finished for {customer_id}")
    except CustIDLockException as e:
        logger.error(f"Could not acquire lock for {customer_id}: {e}")


# Run the example
async def main():
    cust = CustID("customer123")
    cust2 = CustID("customer123")  # Same customer to test lock
    tasks = [
        process_customer(cust, "First processing"),
        process_customer(CustID("customer456"), "Only processing"),
        process_customer(cust2, "Second processing"),  # Same customer to test lock
        process_customer(CustID("customer789"), "Another customer processing"),
        process_customer(CustID("customer123"), "Third processing"),
    ]
    try:
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error occurred while processing customers: {e}")


# To execute the async code
if __name__ == "__main__":
    asyncio.run(main())
