import atexit
import json
import logging.config
import logging.handlers
import sys
from pathlib import Path

import colorlog
from single_source import get_version

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.config.mylogger import NonErrorFilter

logger = logging.getLogger("backend")  # __name__ is a common choice


def setup_logging():
    config_file = Path("logging_configs/5-queued-stderr-json-file.json")
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
            "%(log_color)s%(asctime)s.%(msecs)03d %(levelname)-8s %(name)-14s %(module)-14s %(lineno) 5d : %(message)s",
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
    # handler.addFilter(NonErrorFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # Get the gRPC logger and add the same handler
    grpc_logger = logging.getLogger("grpc")
    grpc_logger.addHandler(handler)
    grpc_logger.setLevel(logging.WARNING)
    logger.info(f"Starting LND gRPC client v{__version__}", extra={"telegram": True})
