import asyncio
from typing import List

from v4vapp_backend_v2.config.setup import logger


def handle_tasks(tasks: List[asyncio.Task]) -> None:
    """
    Handles a list of asyncio tasks, waiting for them to complete and logging any exceptions.

    Args:
        tasks (List[asyncio.Task]): A list of asyncio tasks to handle.
    """
    for task in tasks:
        try:
            logger.info(
                f"Waiting for task {task.get_name()} to complete", extra={"notification": False}
            )
            result = task.result()  # Wait for the task to complete
            if result is not None:
                logger.info(
                    f"Task {task.get_name()} completed with a result",
                    extra={"notification": False, "result": result},
                )
                logger.info(f"{result}", extra={"notification": False})
            else:
                logger.info(
                    f"Task {task.get_name()} completed successfully", extra={"notification": False}
                )
        except Exception as e:
            logger.error(
                f"Task {task.get_name()} failed with exception: {e}", extra={"notification": False}
            )
