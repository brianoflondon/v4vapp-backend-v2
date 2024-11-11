import atexit
import json
import logging.config
import logging.handlers
import sys
from pathlib import Path
from typing import Any

import colorlog
from pydantic import BaseModel
from single_source import get_version
from yaml import safe_load

from v4vapp_backend_v2 import __version__

logger = logging.getLogger("backend")  # __name__ is a common choice


BASE_CONFIG_PATH = Path("config/")
BASE_LOGGING_CONFIG_PATH = Path(BASE_CONFIG_PATH, "logging/")


class LoggingConfig(BaseModel):
    log_config_file: str = ""
    default_log_level: str = "DEBUG"
    handlers: Any
    log_folder: Path = Path("logs/")


class Config(BaseModel):
    version: str = "1"
    logging: LoggingConfig


class InternalConfig:
    config: Config

    def __init__(self):
        super().__init__()
        self.setup_config()
        self.setup_logging()

    def setup_config(self) -> None:
        try:
            config_file = Path(BASE_CONFIG_PATH, "config.yaml")
            with open(config_file) as f_in:
                config = safe_load(f_in)
        except FileNotFoundError as ex:
            logger.error(f"Config file not found: {ex}")
            raise ex

        try:
            self.config = Config.model_validate(config)
        except ValueError as ex:
            logger.error(f"Invalid configuration: {ex}")
            raise ex

    def setup_logging(self):
        try:
            config_file = Path(
                BASE_LOGGING_CONFIG_PATH, self.config.logging.log_config_file
            )
            with open(config_file) as f_in:
                config = json.load(f_in)
        except FileNotFoundError as ex:
            logger.error(f"Logging config file not found: {ex}")
            raise ex

        # if folder for logs doesn't exist create it
        log_folder = self.config.logging.log_folder
        log_folder.mkdir(exist_ok=True)

        logging.config.dictConfig(config)
        queue_handler = logging.getHandlerByName("queue_handler")
        if queue_handler is not None:
            queue_handler.listener.start()
            atexit.register(queue_handler.listener.stop)

        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s%(asctime)s.%(msecs)03d %(levelname)-8s %(name)-14s %(module)-16s %(lineno) 5d : %(message)s",
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
        logger.setLevel(self.config.logging.default_log_level)

        # # Get the gRPC logger and add the same handler
        # grpc_logger = logging.getLogger("grpc")
        # grpc_logger.addHandler(handler)
        # grpc_logger.setLevel(logging.WARNING)

        # # set the level of the logger for asyncio to WARNING
        # logging.getLogger("asyncio").setLevel(logging.WARNING)

        for handler, level in self.config.logging.handlers.items():
            logging.getLogger(handler).addHandler(handler)
            logging.getLogger(handler).setLevel(level)

        logger.info(
            f"Starting LND gRPC client v{__version__}", extra={"telegram": True}
        )
