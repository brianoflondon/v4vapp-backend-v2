import inspect
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from time import sleep
from typing import Any, AsyncIterable, Callable, Iterator, TypeVar

from asgiref.sync import sync_to_async as _sync_to_async
from nectar.exceptions import NectarException
from nectarapi.exceptions import NumRetriesReached, RPCError, WorkingNodeMissing

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
        async def wrapper_gen(*args: Any, **kwargs: Any) -> AsyncIterable[Any]:
            sync_iterable: Iterator[Any] = await async_fn(*args, **kwargs)
            async_iterable: AsyncIterable[Any] = sync_to_async_iterable(sync_iterable)
            async for item in async_iterable:
                yield item

        return wrapper_gen

    else:

        @wraps(sync_fn)
        async def wrapper_fn(*args: Any, **kwargs: Any) -> Any:
            return await async_fn(*args, **kwargs)

        return wrapper_fn


def _is_closed_client_error(error: BaseException) -> bool:
    """True when the Hive/RPC client was closed under a still-running stream worker."""
    text = str(error)
    name = type(error).__name__
    # hive-nectar 1.0.7+ hard-close
    if name == "RPCClosed":
        return True
    if "RPC client is closed" in text:
        return True
    if "Session must be initialized" in text:
        return True
    if "get_active_node" in text:
        return True
    if name == "AttributeError" and "pool_manager" in text:
        return True
    if name == "WorkingNodeMissing" and "pool manager is not available" in text:
        return True
    if name == "RPCConnection" and (
        "Session" in text or "not connected" in text.lower() or "closed" in text.lower()
    ):
        return True
    return False


def _should_propagate_stream_error(error: BaseException) -> bool:
    """Errors that stream_ops must see (do not swallow as clean end-of-stream)."""
    # Closed/discarded clients during rebuild or Ctrl-C: end stream quietly.
    if _is_closed_client_error(error):
        return False
    if isinstance(error, (NumRetriesReached, WorkingNodeMissing, NectarException)):
        # RPCConnection("Session must be initialized") is a NectarException subclass path
        # handled above via _is_closed_client_error when message matches.
        if "Session must be initialized" in str(error):
            return False
        return True
    text = str(error)
    name = type(error).__name__
    if "429" in text or "Too Many Requests" in text:
        return True
    if "batched calls" in text.lower():
        return True
    # Connection / pool failures: must not look like a clean EOS (silent stall).
    if any(
        token in text or token in name
        for token in (
            "PoolTimeout",
            "ConnectTimeout",
            "ReadTimeout",
            "ConnectError",
            "RemoteProtocolError",
        )
    ):
        return True
    return False


async def sync_to_async_iterable(sync_iterable: Iterator[T]) -> AsyncIterable[T]:
    try:
        sync_iterator: Iterator[T] = await iter_async(sync_iterable)
        while True:
            try:
                yield await next_async(sync_iterator)
            except StopAsyncIteration:
                return
            except Exception as e:
                if _is_closed_client_error(e):
                    return
                if _should_propagate_stream_error(e):
                    raise
                # Unexpected: log and end stream (legacy behaviour).
                logger.warning(
                    f"sync_to_async_iterable {type(e).__name__}: {e}",
                    extra={"notification": False},
                )
                return
    except Exception as e:
        if _is_closed_client_error(e):
            return
        if _should_propagate_stream_error(e):
            raise
        logger.warning(
            f"sync_to_async_iterable {type(e).__name__}: {e}",
            extra={"notification": False},
        )
        return


iter_async = sync_to_async(iter, thread_sensitive=False)


def _next(it: Iterator[T]) -> T:
    try:
        return next(it)
    except StopIteration:
        raise StopAsyncIteration
    except NumRetriesReached as e:
        logger.warning(
            f"_next {type(e).__name__}: {e}",
            extra={"notification": False, "error": e},
        )
        raise
    except WorkingNodeMissing as e:
        if _is_closed_client_error(e):
            # pool_manager gone after intentional close — clean EOS, not failover.
            raise StopAsyncIteration
        logger.warning(
            f"_next {e}",
            extra={"notification": False, "error": e},
        )
        raise
    except NectarException as e:
        if _is_closed_client_error(e):
            # Client closed under us (rebuild / Ctrl-C) — end stream quietly.
            raise StopAsyncIteration
        err = str(e)
        if "Block " in err and "does not exist" in err:
            # Let stream-level retry logic handle lagging nodes consistently.
            raise
        if "429" in err or "Too Many Requests" in err:
            # Propagate so stream_ops can cooldown the node and rebuild.
            raise
        if "batched calls" in err.lower() or "BatchedCallsNotSupported" in type(e).__name__:
            # Node cannot use max_batch_size — stream_ops should disable batching.
            raise
        logger.warning(
            f"Nectar Error in _next {type(e).__name__}: {err}",
            extra={
                "notification": False,
                "error": e,
                "nectar_exception": True,
            },
        )
        raise StopAsyncIteration

    except AttributeError as e:
        # Common on shutdown: pool_manager=None after close_hive_client, or closed RPC.
        if _is_closed_client_error(e):
            raise StopAsyncIteration
        logger.warning(
            f"_next AttributeError: {e}",
            extra={"notification": False},
        )
        raise StopAsyncIteration

    except TypeError as e:
        logger.warning(f"_next {e}", extra={"notification": False})
        raise StopAsyncIteration

    except ValueError as e:
        if "last_irreversible_block_num is not in" in str(e):
            logger.warning("Recurrent Transfer list error", extra={"notification": False})
            raise StopAsyncIteration
        else:
            logger.warning(f"_next {e}", extra={"notification": False})
            raise StopAsyncIteration
    except RPCError as e:
        if "Unable to acquire database lock" in str(e):
            logger.warning(
                "_next Hive server Unable to acquire database lock",
                extra={"notification": False, "error": e},
            )
            raise StopAsyncIteration
        raise
    except Exception as e:
        if _is_closed_client_error(e):
            raise StopAsyncIteration
        # Propagate rate-limits / pool failures so stream_ops can recover.
        if _should_propagate_stream_error(e):
            logger.warning(
                f"_next propagating {type(e).__name__}: {e}",
                extra={"notification": False},
            )
            raise
        logger.warning(
            f"_next {type(e).__name__}: {e}",
            extra={"notification": False},
        )
        sleep(0.1)  # Avoid tight loop on unexpected errors
        raise StopAsyncIteration


next_async = sync_to_async(_next, thread_sensitive=False)
