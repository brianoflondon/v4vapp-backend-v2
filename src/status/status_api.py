import asyncio
import logging
import socket
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict

import uvicorn
from colorama import Fore, Style
from fastapi import FastAPI, HTTPException
from single_source import get_version

# Import your logger and config paths from setup.py
from v4vapp_backend_v2.config.setup import (
    InternalConfig,
    logger,  # Use this logger for any custom logging in this file
)

STATUS_API_VERSION = (
    get_version(__name__, Path(__file__).parent, default_return="1.0.0") or "1.0.0"
)

DISABLE_ROUTINE_LOGGING = True  # Set to True to disable routine logs from the status API


class StatusAPIException(Exception):
    """
    Custom exception for status API errors.

    Attributes:
        message (str): The error message.
        extra (Any): Additional information about the error.
    """

    def __init__(self, message: str, extra: Any = None):
        super().__init__(message)
        self.message = message
        self.extra = extra


class StatusAPIPortInUseException(Exception):
    pass


class StatusAPIStartupException(StatusAPIException):
    """Raised when the Status API fails to start."""

    pass


class StatusAPI:
    """
    A simple FastAPI-based status API server.

    - Runs on a specified port.
    - Exposes a /status endpoint that calls a provided async health check function.
    - Returns "OK" (200) on success or an error (500) on failure.
    - Integrates with asyncio shutdown events for graceful stopping.

    Start up with the `start` method, which runs until the provided shutdown event is set.
    Pass in a `health_check_func` that performs necessary health checks.
    Also takes in a `shutdown_event` to listen for shutdown signals.

    Attributes:
        port (int): The port to run the server on.
        health_check_func (Callable[[], Awaitable[Dict[str, Any]]]): An async function to run for health checks.
        shutdown_event (asyncio.Event): The event to wait for shutdown.
        app (FastAPI): The FastAPI application instance.



    """

    def __init__(
        self,
        port: int,
        health_check_func: Callable[[], Awaitable[Dict[str, Any]]],
        shutdown_event: asyncio.Event,
        process_name: str = "status_api",
        version: str = STATUS_API_VERSION,
    ):
        """
        Initialize the StatusAPI.

        Args:
            port (int): The port to run the server on.
            health_check_func (Callable[[], Awaitable[None]]): An async function to run for health checks.
                It should raise an exception if unhealthy.
            shutdown_event (asyncio.Event): The event to wait for shutdown.
        """
        self.port = port
        self.health_check_func = health_check_func
        self.shutdown_event = shutdown_event
        self.app = FastAPI(
            title=f"{process_name} Status API",
            version=version,
            description=f"Status API for {process_name}",
        )

        @self.app.get("/")
        @self.app.get("/status")
        async def status() -> Dict[str, Any]:
            log_func = logger.debug
            try:
                check_answer = await self.health_check_func()
                error_codes_dict = InternalConfig().error_codes_to_dict()
                if error_codes_dict:
                    check_answer["error_codes"] = error_codes_dict
                    log_func = logger.info
                    # We don't need to notify because the underlying issues are already being notified elsewhere
                    if not DISABLE_ROUTINE_LOGGING:
                        log_func(
                            f"Status API health check passed {process_name} {'no error' if not error_codes_dict else 'with errors'}",
                            extra={"notification": False, "check_answer": check_answer},
                        )
                return {"status": "OK", **check_answer}
            except Exception as e:
                # Use your imported logger for consistent logging
                logger.error(
                    f"Health check failed {process_name}: {str(e)}",
                    extra={**getattr(e, "extra", {})},
                )
                raise HTTPException(
                    status_code=500, detail=f"Health check failed {process_name}: {str(e)}"
                )

    def _is_port_available(self, port: int) -> bool:
        """Check if the port is available by attempting to bind a socket."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("0.0.0.0", port))
                return True
            except OSError:
                return False

    async def start(self, shutdown_timeout: float = 5.0):
        """
        Asynchronously start the FastAPI server and run it in the background until the shutdown event is set.

        This method first checks if the specified port is available. If not, it logs an error and returns without starting the server.
        If the port is available, it configures and starts a Uvicorn server with the FastAPI app, using minimal logging.
        The server runs as an asynchronous task, and the method waits for the shutdown event to be set.
        It handles various exceptions, including cancellation, OS errors, and general exceptions, logging errors appropriately.
        In the finally block, it ensures the server task is properly shut down.

        Args:
            shutdown_timeout (float): Timeout in seconds to wait for server shutdown. Default is 5.0.

        Raises:
            No exceptions are raised directly; errors are logged internally.
        """
        # Check if port is available before starting
        logger.info(f"Checking availability of port {self.port} for Status API...")
        server_task = None
        server = None
        try:
            for port in range(self.port, self.port + 10):
                self.port = port
                if self._is_port_available(self.port):
                    break
            if not self._is_port_available(self.port):
                logger.error(f"Port {self.port} is already in use. Cannot start Status API.")
                raise StatusAPIPortInUseException(f"Port {self.port} is already in use.")

            logger.info(
                f"{Fore.WHITE}Starting Status API for {self.app.title} on port {self.port}{Style.RESET_ALL}"
            )

            config = uvicorn.Config(
                self.app,
                host="0.0.0.0",
                port=self.port,
                log_level="warning",  # Only log critical errors (effectively disables most logs)
                access_log=False,
                # log_config=None,  # Disable Uvicorn's default logging configuration
            )
            server = uvicorn.Server(config)
            # Run the server in a task, but allow shutdown
            server_task = asyncio.create_task(server.serve())

            # Give the server a moment to start and surface immediate failures
            await asyncio.sleep(0)  # yield to event loop
            done, _ = await asyncio.wait({server_task}, timeout=0.1)
            if done:
                exc = server_task.exception()
                # If the task completed immediately (with or without exception), treat as startup failure
                logger.error(
                    f"Status API server task finished immediately during startup: {exc or 'no exception'}"
                )
                raise StatusAPIStartupException(
                    f"Status API failed to start on port {self.port}"
                ) from exc

            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        except (OSError, SystemExit, StatusAPIPortInUseException) as e:
            # Trap binding errors (e.g., address already in use) and propagate as startup failure
            logger.error(f"Failed to start Status API on port {self.port}: {str(e)}")
            raise StatusAPIStartupException(
                f"Failed to start Status API on port {self.port}"
            ) from e
        except Exception as e:
            logger.error(f"Error while running Status API: {str(e)}")
            raise StatusAPIStartupException("Error while running Status API") from e
        finally:
            logger.info(
                f"{Fore.WHITE}Shutting down Status API for {self.app.title} on port {self.port}{Style.RESET_ALL}"
            )
            if server_task and server:
                # Suppress uvicorn/starlette CancelledError tracebacks during shutdown
                logging.getLogger("uvicorn.error").setLevel(logging.CRITICAL)
                server.should_exit = True
                try:
                    await asyncio.wait_for(server_task, timeout=shutdown_timeout)
                except asyncio.TimeoutError:
                    logger.warning(
                        f"Status API shutdown timed out after {shutdown_timeout}s, cancelling task"
                    )
                    server_task.cancel()
                    try:
                        await server_task
                    except asyncio.CancelledError:
                        pass
                except (OSError, SystemExit):
                    # Suppress re-raising of binding errors in finally
                    pass
