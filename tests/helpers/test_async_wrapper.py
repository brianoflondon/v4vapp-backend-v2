import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Generator

import pytest

# Assuming the code is in a file called async_wrapper.py
from v4vapp_backend_v2.helpers.async_wrapper import (
    sync_to_async,
    sync_to_async_iterable,
)


# Test fixtures and sample functions
def sync_normal_function(x: int, y: int) -> int:
    return x + y


def sync_generator_function(n: int) -> Generator[int, None, None]:
    for i in range(n):
        yield i


@pytest.fixture
def event_loop():
    """Create an event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Test cases
@pytest.mark.asyncio
async def test_sync_to_async_normal_function():
    """Test wrapping a normal synchronous function."""
    async_func = sync_to_async(sync_normal_function)

    # Test basic functionality
    result = await async_func(3, 4)
    assert result == 7

    # Test with different inputs
    result = await async_func(10, -5)
    assert result == 5


@pytest.mark.asyncio
async def test_sync_to_async_generator_function():
    """Test wrapping a synchronous generator function."""
    async_gen = sync_to_async(sync_generator_function)

    # Test collecting results from async generator
    results = [x async for x in async_gen(3)]
    assert results == [0, 1, 2]

    # Test with different input
    results = [x async for x in async_gen(5)]
    assert results == [0, 1, 2, 3, 4]


@pytest.mark.asyncio
async def test_sync_to_async_empty_generator():
    """Test wrapping a generator that yields nothing."""
    async_gen = sync_to_async(sync_generator_function)
    results = [x async for x in async_gen(0)]
    assert results == []


@pytest.mark.asyncio
async def test_sync_to_async_thread_sensitivity():
    """Test thread_sensitive parameter."""
    # Test with thread_sensitive=True (uses default executor)
    async_func_ts = sync_to_async(sync_normal_function, thread_sensitive=True)
    result_ts = await async_func_ts(1, 2)
    assert result_ts == 3

    # Test with thread_sensitive=False (uses thread pool)
    async_func_nts = sync_to_async(sync_normal_function, thread_sensitive=False)
    result_nts = await async_func_nts(1, 2)
    assert result_nts == 3


@pytest.mark.asyncio
async def test_sync_to_async_iterable():
    """Test the sync_to_async_iterable helper function directly."""
    sync_iter = iter([1, 2, 3])
    async_iter = sync_to_async_iterable(sync_iter)

    results = [x async for x in async_iter]
    assert results == [1, 2, 3]


def test_type_hints():
    """Test that type hints are preserved (manual inspection)."""
    # This is more of a compile-time check, but we can verify the function signatures
    async_func = sync_to_async(sync_normal_function)
    async_gen = sync_to_async(sync_generator_function)

    # Check if the wrapper preserves the original function's metadata
    assert async_func.__name__ == sync_normal_function.__name__
    assert async_gen.__name__ == sync_generator_function.__name__


@pytest.mark.asyncio
async def test_multiple_concurrent_calls():
    """Test multiple concurrent calls to wrapped functions."""
    async_func = sync_to_async(sync_normal_function)

    results = await asyncio.gather(async_func(1, 2), async_func(3, 4), async_func(5, 6))
    assert results == [3, 7, 11]


if __name__ == "__main__":
    pytest.main([__file__])
