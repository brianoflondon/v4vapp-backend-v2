import asyncio
import socket
from typing import Any, Awaitable, Callable, Dict

import uvicorn
from colorama import Fore, Style
from fastapi import FastAPI, HTTPException

# Import your logger and config paths from setup.py
from v4vapp_backend_v2.config.setup import (
    InternalConfig,
    logger,  # Use this logger for any custom logging in this file
)


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
        version: str = "1.0.0",
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
            try:
                ans = await self.health_check_func()
                error_codes_dict = InternalConfig().error_codes_to_dict()
                if error_codes_dict:
                    ans["error_codes"] = error_codes_dict
                return {"status": "OK", **ans}
            except Exception as e:
                # Use your imported logger for consistent logging
                logger.error(f"Health check failed: {str(e)}", extra={**getattr(e, "extra", {})})
                raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

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
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.port,
            log_level="critical",  # Only log critical errors (effectively disables most logs)
            access_log=False,
        )
        server = uvicorn.Server(config)
        server_task = None
        try:
            if not self._is_port_available(self.port):
                raise StatusAPIPortInUseException(f"Port {self.port} is already in use.")

            logger.info(
                f"{Fore.WHITE}Starting Status API for {self.app.title} on port {self.port}{Style.RESET_ALL}"
            )

            # Run the server in a task, but allow shutdown
            server_task = asyncio.create_task(server.serve())

            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        except (OSError, SystemExit, StatusAPIPortInUseException) as e:
            # Trap binding errors (e.g., address already in use)
            logger.error(f"Failed to start Status API on port {self.port}: {str(e)}")
        except Exception as e:
            logger.error(f"Error while running Status API: {str(e)}")
        finally:
            if server_task:
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
