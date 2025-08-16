import asyncio
import os
import sys
from typing import Awaitable, Callable, TypeVar

from pymongo.errors import (
    ConnectionFailure,
    DuplicateKeyError,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.results import (
    BulkWriteResult,
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.db_pymongo import DATABASE_ICON

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
            logger.info(
                f"{DATABASE_ICON} {context} {summarize_write_result(result)}",
                extra={"db_result": result},
            )
            if error_count > 0:
                logger.info(
                    f"{DATABASE_ICON} reconnected after {error_count} errors"
                    f"{f' ({context})' if context else ''}",
                    extra={"notification": True, "error_code_clear": error_code},
                )
            return result

        except DuplicateKeyError:
            # Not retryable; bubble up
            raise

        except OperationFailure as e:
            if _is_retryable_op_failure(e):
                error_count += 1
                delay = min(base_delay * error_count, max_delay)

                notify_now = True if notify_on_error and error_count == 1 else False
                logger.warning(
                    f"{DATABASE_ICON} retry {error_count} error {f' ({context})' if context else ''}",
                    extra={"error_code": error_code, "notification": notify_now, "error": e},
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
                f"{DATABASE_ICON} retry {error_count}: {e}{f' ({context})' if context else ''}",
                extra={"error_code": error_code, "notification": notify_on_error},
            )
            if max_retries is not None and error_count > max_retries:
                raise
            await asyncio.sleep(delay)
            continue


def summarize_write_result(result) -> str:
    """
    Return a human-readable summary for common PyMongo write result objects.
    Supports: UpdateResult, InsertOneResult, InsertManyResult, DeleteResult, BulkWriteResult.
    """
    kind = type(result).__name__
    ack = getattr(result, "acknowledged", None)

    if isinstance(result, UpdateResult):
        matched = result.matched_count
        modified = result.modified_count
        upserted = result.upserted_id
        updated_existing = getattr(result, "raw_result", {}).get("updatedExisting")
        parts = [
            f"{kind}",
            f"matched={matched}",
            f"modified={modified}",
            f"upserted_id={upserted if upserted is not None else '-'}",
            f"acknowledged={ack}",
        ]
        if updated_existing is not None:
            parts.append(f"updatedExisting={updated_existing}")
        return ", ".join(parts)

    if isinstance(result, InsertOneResult):
        return f"{kind}, inserted_id={result.inserted_id}, acknowledged={ack}"

    if isinstance(result, InsertManyResult):
        count = len(result.inserted_ids or [])
        return f"{kind}, inserted_count={count}, acknowledged={ack}"

    if isinstance(result, DeleteResult):
        return f"{kind}, deleted_count={result.deleted_count}, acknowledged={ack}"

    if isinstance(result, BulkWriteResult):
        # BulkWriteResult exposes counters directly
        return (
            f"{kind}, "
            f"inserted={result.inserted_count}, "
            f"matched={result.matched_count}, "
            f"modified={result.modified_count}, "
            f"deleted={result.deleted_count}, "
            f"upserts={result.upserted_count}, "
            f"acknowledged={ack}"
        )

    # Fallback: dump raw_result if present
    raw = getattr(result, "raw_result", None)
    if raw is not None:
        return f"{kind}, raw_result={raw}, acknowledged={ack}"
    return f"{kind}, acknowledged={ack}"
