import inspect
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, AsyncIterable, Callable, Iterator, TypeVar

from asgiref.sync import sync_to_async as _sync_to_async

from v4vapp_backend_v2.config.setup import logger

thread_pool = ThreadPoolExecutor()

T = TypeVar("T")


# Async generator wrapper from https://github.com/django/asgiref/issues/142
def sync_to_async(
    sync_fn: Callable[..., Any], thread_sensitive: bool = True
) -> Callable[..., Any]:
    executor = thread_pool if not thread_sensitive else None
    is_gen = inspect.isgeneratorfunction(sync_fn)
    async_fn = _sync_to_async(
        sync_fn, thread_sensitive=thread_sensitive, executor=executor
    )

    if is_gen:

        @wraps(sync_fn)
        async def wrapper(*args: Any, **kwargs: Any) -> AsyncIterable[Any]:
            sync_iterable: Iterator[Any] = await async_fn(*args, **kwargs)
            async_iterable: AsyncIterable[Any] = sync_to_async_iterable(sync_iterable)
            async for item in async_iterable:
                yield item

    else:

        @wraps(sync_fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await async_fn(*args, **kwargs)

    return wrapper


async def sync_to_async_iterable(sync_iterable: Iterator[T]) -> AsyncIterable[T]:
    try:
        sync_iterator: Iterator[T] = await iter_async(sync_iterable)
        while True:
            try:
                yield await next_async(sync_iterator)
            except StopAsyncIteration:
                return
            except AttributeError as log_error:
                logger.error(
                    f"Logging error: {log_error} - problem with str in log level",
                    extra={"notification": False},
                )
                return
    except Exception as e:
        try:
            logger.warning(
                f"sync_to_async_iterable {e}", extra={"notification": False, "error": e}
            )
        except AttributeError as log_error:
            logger.error(
                f"Logging error: {log_error} - problem with str in log level",
                extra={"notification": False},
            )
        raise e


iter_async = sync_to_async(iter, thread_sensitive=False)


def _next(it: Iterator[T]) -> T:
    try:
        return next(it)
    except StopIteration:
        raise StopAsyncIteration
    except AttributeError as log_error:
        logger.error(
            f"Logging error: {log_error} - problem with str in log level",
            extra={"notification": False},
        )
        raise StopAsyncIteration
    except Exception as e:
        try:
            logger.warning(f"_next {e}", extra={"notification": False, "error": e})
        except AttributeError as log_error:
            logger.error(
                f"Logging error: {log_error} - problem with str in log level",
                extra={"notification": False},
            )
        raise e


next_async = sync_to_async(_next, thread_sensitive=False)
