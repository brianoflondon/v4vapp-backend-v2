import inspect
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Any, AsyncIterable, Callable, Iterator, TypeVar

from asgiref.sync import sync_to_async as _sync_to_async
from nectar.exceptions import NectarException

from v4vapp_backend_v2.config.setup import logger

thread_pool = ThreadPoolExecutor()

T = TypeVar("T")


# Async generator wrapper from https://github.com/django/asgiref/issues/142
def sync_to_async(
    sync_fn: Callable[..., Any], thread_sensitive: bool = True
) -> Callable[..., Any]:
    """
    Converts a synchronous function into an asynchronous function.

    This utility wraps a synchronous function and allows it to be called
    in an asynchronous context. It supports both regular functions and
    generator functions.

    Args:
        sync_fn (Callable[..., Any]): The synchronous function to be converted.
        thread_sensitive (bool, optional): If True, the function will run in the
            main thread. If False, it will run in a thread pool. Defaults to True.

    Returns:
        Callable[..., Any]: An asynchronous version of the provided synchronous function.
            If the input function is a generator function, the returned function will
            yield items asynchronously.
    """
    executor = thread_pool if not thread_sensitive else None
    is_gen = inspect.isgeneratorfunction(sync_fn)
    async_fn = _sync_to_async(sync_fn, thread_sensitive=thread_sensitive, executor=executor)

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
        return


iter_async = sync_to_async(iter, thread_sensitive=False)


def _next(it: Iterator[T]) -> T:
    try:
        return next(it)
    except StopIteration:
        raise StopAsyncIteration
    except NectarException as e:
        logger.warning(
            f"Nectar Error in _next {e}",
            extra={
                "notification": False,
                "error": e,
                "nectar_exception": True,
            },
        )
        raise StopAsyncIteration

    except AttributeError as e:
        logger.exception(
            f"Logging error: {e} - problem with str in log level",
            extra={"notification": False, "error": e},
        )
        raise StopAsyncIteration

    except TypeError as e:
        logger.warning(f"_next {e}", extra={"notification": False, "error": e})
        raise StopAsyncIteration

    except ValueError as e:
        if "last_irreversible_block_num is not in" in str(e):
            logger.warning("Recurrent Transfer list error", extra={"notification": False})
            raise StopAsyncIteration
        else:
            logger.warning(f"_next {e}", extra={"notification": False, "error": e})
            raise StopAsyncIteration
    except Exception as e:
        logger.warning(f"_next {e}", extra={"notification": False, "error": e})
        logger.exception(e, extra={"notification": False})
        raise e


next_async = sync_to_async(_next, thread_sensitive=False)
