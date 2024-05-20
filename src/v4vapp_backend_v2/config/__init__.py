import atexit
import json
import logging.config
import logging.handlers
import pathlib
import sys

import colorlog

logger = logging.getLogger("backend")  # __name__ is a common choice


def setup_logging():
    config_file = pathlib.Path("logging_configs/2-stderr-json-file.json")
    with open(config_file) as f_in:
        config = json.load(f_in)

    logging.config.dictConfig(config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

    handler = colorlog.StreamHandler()
    handler.setFormatter(
        colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s %(levelname)-8s %(name)-14s %(module)-14s %(lineno) 5d : %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
            log_colors={
                "DEBUG": "cyan",
                "INFO": "blue",  # change this to the color you want
                "WARNING": "yellow",
                "ERROR": "red",
                "CRITICAL": "red,bg_white",
            },
            stream=sys.stdout,
        )
    )
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # Get the gRPC logger and add the same handler
    grpc_logger = logging.getLogger("grpc")
    grpc_logger.addHandler(handler)
    grpc_logger.setLevel(logging.WARNING)

    logger.info("Starting LND gRPC client")
    logger.debug("Debug message")
    logger.warning("Warning message")
    logger.error("Error message")
    critical = {"json": "data"}
    logger.critical("Critical message", extra=critical)
