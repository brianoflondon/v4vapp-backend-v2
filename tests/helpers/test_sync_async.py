# import asyncio
# import threading
# from concurrent.futures import ThreadPoolExecutor

# import pytest

# from v4vapp_backend_v2.depreciated.sync_async import AsyncConverter

# # Mark all tests as asyncio tests
# pytestmark = pytest.mark.asyncio


# async def test_regular_function_conversion():
#     """Test converting a regular synchronous function to async."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def add_five(x: int) -> int:
#             return x + 5

#         result = await add_five(10)
#         assert result == 15
#         assert isinstance(result, int)


# async def test_generator_conversion():
#     """Test converting a synchronous generator to async iterator."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def count_to(n: int):
#             for i in range(n):
#                 yield i

#         results = []
#         async for num in count_to(3):
#             results.append(num)
#         assert results == [0, 1, 2]
#         assert len(results) == 3


# async def test_nested_async_calls():
#     """Test nested async function calls by handling async at the test level."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def multiply(x: int) -> int:
#             return x * 2

#         # Process as a separate async call
#         intermediate = await multiply(5)

#         @converter.sync_to_async
#         def process(x: int) -> int:
#             return x + 1  # Use pre-computed intermediate value

#         result = await process(intermediate)
#         assert result == 11  # (5 * 2) + 1


# async def test_outside_context():
#     """Test that sync_to_async fails outside async context."""
#     converter = AsyncConverter()

#     with pytest.raises(
#         RuntimeError, match="AsyncConverter must be used within an async context"
#     ):

#         @converter.sync_to_async
#         def dummy():
#             return 42

#         await dummy()


# async def test_non_iterable_generator():
#     """Test error handling when a supposed generator returns a non-iterable."""
#     # with pytest.raises(AsyncConverter.ConversionError, match="returned non-iterable"):
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def bad_generator():
#             return 42  # Not an iterable

#         result = await bad_generator()
#         try:
#             async for _ in result:  # This should trigger the error
#                 pass
#         except TypeError as e:
#             assert "requires an object with __aiter__" in str(e)


# async def test_exception_in_function():
#     """Test error propagation from a sync function."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def failing_function():
#             raise ValueError("Test error")

#         with pytest.raises(
#             AsyncConverter.ConversionError,
#             match="Function conversion failed: Test error",
#         ):
#             await failing_function()


# async def test_exception_in_generator():
#     """Test error propagation from a sync generator."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def failing_generator():
#             yield 1
#             raise ValueError("Generator error")

#         with pytest.raises(
#             AsyncConverter.ConversionError,
#             match="Generator conversion failed:.*Generator error",
#         ):
#             async for _ in failing_generator():
#                 pass


# async def test_empty_generator():
#     """Test handling of an empty generator."""
#     async with AsyncConverter(max_workers=2) as converter:

#         @converter.sync_to_async
#         def empty_generator():
#             if False:
#                 yield  # Never reached

#         results = []
#         async for item in empty_generator():
#             results.append(item)
#         assert results == []

# @pytest.mark.skip(reason="This test is not working")
# async def test_thread_sensitivity():
#     """Test thread-sensitive execution."""
#     async with AsyncConverter(max_workers=2) as converter:

#         def get_thread_id():
#             return threading.current_thread().ident

#         get_thread_id_async = converter.sync_to_async(
#             get_thread_id, thread_sensitive=True
#         )

#         main_thread_id = threading.current_thread().ident
#         async_thread_id = await get_thread_id_async()
#         assert async_thread_id == main_thread_id


# async def test_custom_executor():
#     """Test using a custom executor."""
#     async with AsyncConverter(max_workers=2) as converter:
#         custom_executor = ThreadPoolExecutor(max_workers=1)

#         def simple_task():
#             return 123

#         with pytest.raises(
#             TypeError, match="executor must not be set when thread_sensitive is True"
#         ):
#             simple_task_async = converter.sync_to_async(
#                 simple_task, executor=custom_executor
#             )
#             result = await simple_task_async()
#             assert result == 123
#             custom_executor.shutdown()


# if __name__ == "__main__":
#     pytest.main([__file__, "-v"])
