import inspect
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, AsyncIterator, Callable, Iterable, TypeVar, Union

from asgiref.sync import sync_to_async as _sync_to_async

T = TypeVar("T")


class AsyncConverter:
    """A context manager for converting sync functions to async with thread pool management."""

    class ConversionError(Exception):
        """Custom exception for async conversion issues"""

        pass

    def __init__(self, max_workers: int = None, thread_name_prefix: str = "async-"):
        """Initialize the converter with thread pool settings."""
        self._thread_pool = ThreadPoolExecutor(
            max_workers=max_workers or min(32, (os.cpu_count() or 1) * 5),
            thread_name_prefix=thread_name_prefix,
        )
        self._is_active = False

        # Define these after __init__ since they don't depend on instance state
        self._iter_async = _sync_to_async(self._safe_iter, thread_sensitive=False)
        self._next_async = _sync_to_async(self._next, thread_sensitive=False)

    async def __aenter__(self) -> "AsyncConverter":
        """Start the context manager."""
        self._is_active = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up resources on context exit."""
        self._thread_pool.shutdown(wait=False)
        self._is_active = False

    def sync_to_async(
        self,
        sync_fn: Callable[..., Any],
        thread_sensitive: bool = True,
        executor: Union[ThreadPoolExecutor, None] = None,
    ) -> Callable[..., Any]:
        """Convert a synchronous function or generator to an async equivalent."""
        if not self._is_active:
            raise RuntimeError("AsyncConverter must be used within an async context")

        effective_executor = (
            executor
            if executor is not None
            else (self._thread_pool if not thread_sensitive else None)
        )
        is_generator = inspect.isgeneratorfunction(sync_fn)
        async_fn = _sync_to_async(
            sync_fn, thread_sensitive=thread_sensitive, executor=effective_executor
        )

        if is_generator:

            @wraps(sync_fn)
            async def wrapper(*args, **kwargs) -> AsyncIterator[T]:
                try:
                    sync_iterable = await async_fn(*args, **kwargs)
                    if not isinstance(sync_iterable, Iterable):
                        raise self.ConversionError(
                            f"Generator function returned non-iterable: {type(sync_iterable)}"
                        )
                    async for item in self._sync_to_async_iterable(sync_iterable):
                        yield item
                except Exception as e:
                    raise self.ConversionError(
                        f"Generator conversion failed: {str(e)}"
                    ) from e

        else:

            @wraps(sync_fn)
            async def wrapper(*args, **kwargs) -> Any:
                try:
                    return await async_fn(*args, **kwargs)
                except Exception as e:
                    raise self.ConversionError(
                        f"Function conversion failed: {str(e)}"
                    ) from e

        return wrapper

    async def _sync_to_async_iterable(self, sync_iterable: Any) -> AsyncIterator[T]:
        """Convert a synchronous iterable to an asynchronous iterator."""
        try:
            if not hasattr(sync_iterable, "__iter__") and not hasattr(
                sync_iterable, "__aiter__"
            ):
                raise self.ConversionError(
                    f"Object is not iterable: {type(sync_iterable)}"
                )
            sync_iterator = await self._iter_async(sync_iterable)
            while True:
                try:
                    yield await self._next_async(sync_iterator)
                except StopAsyncIteration:
                    break
                except Exception as e:
                    raise self.ConversionError(f"Iteration failed: {str(e)}") from e
        except Exception as e:
            raise self.ConversionError(f"Iterable conversion failed: {str(e)}") from e

    # Static helper methods
    @staticmethod
    def _safe_iter(obj: Any) -> Any:
        """Safely create an iterator from an object."""
        try:
            return iter(obj)
        except TypeError as e:
            raise TypeError(f"Cannot create iterator from {type(obj)}: {str(e)}")

    @staticmethod
    def _next(it: Any) -> Any:
        try:
            return next(it)
        except StopIteration:
            raise StopAsyncIteration


# Corrected usage example
async def example_usage():
    async with AsyncConverter() as converter:
        # Convert a regular function
        async_add = converter.sync_to_async(lambda x: x + 1)

        # Convert a generator
        def sync_gen(n):
            for i in range(n):
                yield i

        async_gen = converter.sync_to_async(sync_gen)

        # Use them
        result = await async_add(5)  # returns 6
        print(f"Result: {result}")

        async for num in async_gen(3):  # yields 0, 1, 2
            print(f"Number: {num}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(example_usage())
