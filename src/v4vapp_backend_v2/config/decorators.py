import functools
import time
from timeit import default_timer as timer

from v4vapp_backend_v2.config.setup import logger

"""
General purpose functions
"""
ICON = "⏰"


def async_time_decorator(func):
    """
    A decorator that wraps an asynchronous function to log its execution
    time and handle exceptions.

    Args:
        func (coroutine function): The asynchronous function to be wrapped.

    Returns:
        coroutine function: The wrapped asynchronous function.

    The wrapper logs the execution time of the function and, in case of an exception,
    logs the error along with the time taken before the exception occurred.
    """

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.time()
        extra_info = ""
        if "account" in kwargs:
            extra_info = f" for '{kwargs['account']}'"
        elif "cust_id" in kwargs:
            extra_info = f" for cust_id '{kwargs['cust_id']}'"
        try:
            result = await func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"{ICON} Function '{func.__qualname__[:26]:<26}' took {execution_time:.4f}s{extra_info}",
                extra={
                    "func_name": func.__qualname__,
                    "call_kwargs": kwargs,
                    "execution_time": execution_time,
                },
            )
            return result
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.warning(
                f"{ICON} Function '{func.__qualname__[:26]:<26}' failed after {execution_time:.4f}s: {str(e)}",
                extra={"notification": False, "error": e},
            )
            raise

    return wrapper


def async_time_stats_decorator(runs=1, notes: str = ""):
    """
    A decorator to measure and log the execution time of an asynchronous function.

    This decorator logs the execution time of the decorated function and maintains
    a list of execution times for a specified number of runs. Once the number of
    runs is reached, it logs the average execution time and the standard deviation
    (if applicable), then resets the timings list.

    Args:
        func (Callable): The asynchronous function to be decorated.

    Returns:
        Callable: The wrapped function with timing and logging functionality.

    Raises:
        Exception: Re-raises any exception encountered during the execution of the
        decorated function, after logging the failure and execution time.
    """
    ICON = "⏰"

    def decorator(func):
        import time  # Local import to ensure availability
        from statistics import mean, stdev  # Also import these locally

        timings = []

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal timings
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                end_time = time.time()
                execution_time = end_time - start_time
                timings.append(execution_time)

                if len(timings) >= runs:
                    avg_time = mean(timings)
                    logger.info(
                        f"{ICON} Last: {execution_time * 1000:>4.0f}ms, Avg: {avg_time * 1000:>4.0f}ms, Runs: {len(timings)} {func.__qualname__[:34]:<38}",
                        extra={
                            "func_name": func.__qualname__,
                            "call_kwargs": kwargs,
                            "avg_time": avg_time,
                            "timings": timings,
                        },
                    )
                    if len(timings) > 1:
                        logger.debug(f"{ICON} Std Dev: {stdev(timings) * 1000:>4.0f}ms")
                    timings = []  # Reset after reporting

                return result
            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time
                logger.warning(
                    f"{ICON} Function '{func.__qualname__[:26]}' failed after {execution_time:.4f}s: {str(e)}"
                )
                raise

        return wrapper

    return decorator


def time_decorator(func):
    """
    Synchronous version: logs execution time and handles exceptions like the async decorator.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = timer()
        extra_info = ""
        if "account" in kwargs:
            extra_info = f" for '{kwargs['account']}'"
        elif "cust_id" in kwargs:
            extra_info = f" for cust_id '{kwargs['cust_id']}'"
        try:
            result = func(*args, **kwargs)
            execution_time = timer() - start_time
            logger.info(
                f"{ICON} Function '{func.__qualname__[:26]:<26}' took {execution_time:.6f}s{extra_info}",
                extra={
                    "func_name": func.__qualname__,
                    "call_kwargs": kwargs,
                    "execution_time": execution_time,
                },
            )
            return result
        except Exception as e:
            execution_time = timer() - start_time
            logger.warning(
                f"{ICON} Function '{func.__qualname__[:26]:<26}' failed after {execution_time:.6f}s: {str(e)}",
                extra={"notification": False, "error": e},
            )
            raise

    return wrapper
