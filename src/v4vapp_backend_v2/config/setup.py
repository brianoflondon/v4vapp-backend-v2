import asyncio
import atexit
import json
import logging.config
import logging.handlers
import sys
from pathlib import Path
from typing import Any, List, Protocol, override

import colorlog
from pydantic import BaseModel
from yaml import safe_load

from v4vapp_backend_v2 import __version__

logger = logging.getLogger("backend")  # __name__ is a common choice


BASE_CONFIG_PATH = Path("config/")
BASE_LOGGING_CONFIG_PATH = Path(BASE_CONFIG_PATH, "logging/")

BASE_DISPLAY_LOG_LEVEL = logging.INFO  # Default log level for stdout

"""
These classes need to match the structure of the config.yaml file

"""


class LoggingConfig(BaseModel):
    log_config_file: str = ""
    default_log_level: str = "DEBUG"
    handlers: Any
    log_folder: Path = Path("logs/")


class LndConnectionConfig(BaseModel):
    name: str = ""
    address: str = ""
    options: list = []
    certs_path: Path = Path(".certs/")
    macaroon_filename: str = ""
    cert_filename: str = ""
    use_proxy: str = ""


class TailscaleConfig(BaseModel):
    tailnet_name: str = ""
    notification_server: str = ""
    notification_server_port: int = 0


class TelegramConfig(BaseModel):
    chat_id: int = 0


class Config(BaseModel):
    version: str = "1"
    logging: LoggingConfig
    lnd_connections: List[LndConnectionConfig]
    tailscale: TailscaleConfig
    telegram: TelegramConfig

    def connection(self, connection_name: str) -> LndConnectionConfig:
        for connection in self.lnd_connections:
            if connection.name == connection_name:
                return connection
        raise ValueError(f"Connection {connection_name} not found in config")

class ConsoleLogFilter(logging.Filter):
    """
    A logging filter that allows only log records with a level greater than DEBUG.

    This is referenced in the logging configuration json file.

    Methods:
        filter(record: logging.LogRecord) -> bool | logging.LogRecord:
            Determines if the given log record should be logged. Returns True
            if the log level is more than DEBUG, otherwise False.
    """

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno >= BASE_DISPLAY_LOG_LEVEL


class LoggerFunction(Protocol):
    def __call__(self, msg: object, *args: Any, **kwargs: Any) -> None: ...


class InternalConfig:
    """
    Singleton class to manage internal configuration and logging setup.

    Attributes:
        _instance (InternalConfig): Singleton instance of the class.
        config (Config): Configuration object validated from the config file.

    Methods:
        __new__(cls, *args, **kwargs):
            Ensures only one instance of the class is created (Singleton pattern).

        __init__(self):
            Initializes the instance, sets up configuration and logging if not already
            initialized.

        setup_config(self) -> None:
            Loads and validates the configuration from a YAML file.

        setup_logging(self):
            Sets up logging configuration from a JSON file, initializes log handlers,
            and sets log levels.
    """

    _instance = None
    config: Config
    notification_loop: asyncio.AbstractEventLoop

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InternalConfig, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "_initialized"):
            super().__init__()
            self.setup_config()
            self.setup_logging()
            self._initialized = True

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

        # Configuration for the json log file is set in the external config.json file
        # The stdout log configuration is set in the code below

        # if folder for logs doesn't exist create it
        log_folder = self.config.logging.log_folder
        log_folder.mkdir(exist_ok=True)

        logging.config.dictConfig(config)
        queue_handler = logging.getHandlerByName("queue_handler")
        if queue_handler is not None:
            queue_handler.listener.start()
            try:
                self.notification_loop = asyncio.get_running_loop()
                logger.info("Found running loop for setup logging")
            except RuntimeError:  # No event loop in the current thread
                self.notification_loop = asyncio.new_event_loop()
                logger.info("Started new event loop for notification logging")
            atexit.register(self.notification_loop.close)

        try:
            if config["formatters"]["simple"]["format"]:
                format_str = config["formatters"]["simple"]["format"]
        except KeyError:
            format_str = (
                "%(asctime)s.%(msecs)03d %(levelname)-8s %(module)-22s "
                "%(lineno)6d : %(message)s"
            )

        def custom_log_namer(name):
            return name

            # full_base, ext = name.rsplit(".", 1)
            # # check if ext is an integer
            # try:
            #     int(ext)
            #     base, real_ext = full_base.rsplit(".", 1)
            #     return f"{base}.{ext}.{real_ext}"
            # except ValueError:
            #     return name

            # return name

        file_json_handler = logging.getHandlerByName("file_json")
        if file_json_handler is not None:
            file_json_handler.namer = custom_log_namer

        # Set up the colorlog handler
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s" + format_str,
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

        handler.addFilter(ConsoleLogFilter())

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
