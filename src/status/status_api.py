import asyncio
from typing import Any, Awaitable, Callable, Dict

import uvicorn
from fastapi import FastAPI, HTTPException


class StatusAPI:
    """
    A simple FastAPI-based status API server.

    - Runs on a specified port.
    - Exposes a /status endpoint that calls a provided async health check function.
    - Returns "OK" (200) on success or an error (500) on failure.
    - Integrates with asyncio shutdown events for graceful stopping.
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
            title="Status API", version=version, description=f"Status API for {process_name}"
        )

        @self.app.get("/")
        @self.app.get("/status")
        async def status():
            try:
                ans = await self.health_check_func()
                return {"status": "OK", **ans}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

    async def start(self):
        """
        Start the FastAPI server asynchronously.
        Runs in the background until shutdown_event is set.
        """
        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.port, log_level="info")
        server = uvicorn.Server(config)

        # Run the server in a task, but allow shutdown
        server_task = asyncio.create_task(server.serve())

        # Wait for shutdown signal
        try:
            await self.shutdown_event.wait()
        except asyncio.CancelledError:
            pass
        finally:
            server.should_exit = True
            await server_task
