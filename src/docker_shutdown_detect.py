import signal
import asyncio
import logging
import sys

logger = logging.getLogger(__name__)

# Define a global flag to track shutdown
shutdown_event = asyncio.Event()

def handle_shutdown_signal(signum, frame):
    """
    Signal handler to set the shutdown event.
    """
    logger.info(f"Received shutdown signal: {signum}")
    shutdown_event.set()

async def main():
    """
    Main function to run the application.
    """
    try:
        logger.info("Application started. Running...")
        while not shutdown_event.is_set():
            # Simulate some work
            await asyncio.sleep(1)
            logger.info("Working...")
    except asyncio.CancelledError:
        logger.info("Main task cancelled.")
    finally:
        logger.info("Cleaning up resources...")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)

    # Register signal handlers for SIGTERM and SIGINT
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    signal.signal(signal.SIGINT, handle_shutdown_signal)

    # Run the main event loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        logger.info("Application shutting down.")
        sys.exit(0)