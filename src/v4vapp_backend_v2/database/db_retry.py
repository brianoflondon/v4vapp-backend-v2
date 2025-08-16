import asyncio
import os
from pprint import pprint
import sys
from typing import Awaitable, Callable, TypeVar

from pymongo.errors import (
    ConnectionFailure,
    DuplicateKeyError,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from v4vapp_backend_v2.database.db_pymongo import DATABASE_ICON
from v4vapp_backend_v2.config.setup import logger

app_name = os.path.basename(sys.argv[0])

T = TypeVar("T")


def _is_retryable_op_failure(e: OperationFailure) -> bool:
    code = getattr(e, "code", None)
    details = getattr(e, "details", {}) or {}
    code_name = details.get("codeName")
    has_label = hasattr(e, "has_error_label") and e.has_error_label("RetryableWriteError")
    if has_label:
        return True
    if code in (189, 91):  # PrimarySteppedDown, ShutdownInProgress
        return True
    if code_name in ("PrimarySteppedDown", "NotWritablePrimary"):
        return True
    return False


async def mongo_call(
    op: Callable[[], Awaitable[T]],
    *,
    max_retries: int | None = None,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    error_code: str = "mongodb_op_error",
    notify_on_error: bool = True,
    context: str = "",
) -> T:
    """
    Execute an async MongoDB operation with retry/backoff.
    All retry/error/success logging happens here.
    """
    error_count = 0
    while True:
        try:
            result = await op()
            pprint(result)
            if error_count > 0:
                logger.info(
                    f"MongoDB reconnected after {error_count} errors"
                    f"{f' ({context})' if context else ''}",
                    extra={"error_code_clear": error_code},
                )
            return result

        except DuplicateKeyError:
            # Not retryable; bubble up
            raise

        except OperationFailure as e:
            if _is_retryable_op_failure(e):
                error_count += 1
                delay = min(base_delay * error_count, max_delay)
                logger.warning(
                    f"MongoDB retry {error_count}: {e}{f' ({context})' if context else ''}",
                    extra={"error_code": error_code, "notification": notify_on_error},
                )
                if max_retries is not None and error_count > max_retries:
                    raise
                await asyncio.sleep(delay)
                continue
            raise

        except (ServerSelectionTimeoutError, NetworkTimeout, ConnectionFailure) as e:
            error_count += 1
            delay = min(base_delay * error_count, max_delay)
            logger.warning(
                f"MongoDB retry {error_count}: {e}{f' ({context})' if context else ''}",
                extra={"error_code": error_code, "notification": notify_on_error},
            )
            if max_retries is not None and error_count > max_retries:
                raise
            await asyncio.sleep(delay)
            continue
