import asyncio
import atexit
import functools
import json
import logging.config
import logging.handlers
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Protocol, override

import colorlog
from pydantic import BaseModel, field_validator, model_validator
from pymongo.operations import _IndexKeyHint
from yaml import safe_load

logger = logging.getLogger("backend")  # __name__ is a common choice


BASE_CONFIG_PATH = Path("config/")
BASE_LOGGING_CONFIG_PATH = Path(BASE_CONFIG_PATH, "logging/")

BASE_DISPLAY_LOG_LEVEL = logging.INFO  # Default log level for stdout

"""
These classes need to match the structure of the config.yaml file

"""


class StartupFailure(Exception):
    pass


class LoggingConfig(BaseModel):
    log_config_file: str = ""
    default_log_level: str = "DEBUG"
    handlers: Any
    log_folder: Path = Path("logs/")


class LndConnectionConfig(BaseModel):
    name: str = ""
    icon: str = ""
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


class IndexConfig(BaseModel):
    index_key: _IndexKeyHint | None = None
    unique: Optional[bool] = None


class CollectionConfig(BaseModel):
    indexes: Dict[str, IndexConfig] | None = None


class DatabaseUserConfig(BaseModel):
    password: str
    roles: List[str]


class DatabaseDetailsConfig(BaseModel):
    db_users: Dict[str, DatabaseUserConfig]
    collections: Optional[Dict[str, CollectionConfig | None]] = None


class DatabaseConnectionConfig(BaseModel):
    db_hosts: List[str]
    db_replica_set: Optional[str] = None
    dbs: Dict[str, DatabaseDetailsConfig]


class Config(BaseModel):
    """
    Config class for application configuration.

    Attributes:
        version (str): The version of the configuration. Default is "1".
        logging (LoggingConfig): Configuration for logging.
        default_connection (str): The default connection name.
        lnd_connections (List[LndConnectionConfig]):
            List of LND connection configurations.
        tailscale (TailscaleConfig): Configuration for Tailscale.
        telegram (TelegramConfig): Configuration for Telegram.

    Methods:
        unique_names(cls, v):
            Validates that all LND connections have unique names.

        check_default_connection(cls, v):
            Validates that the default connection is present in the
            list of LND connections.

        list_lnd_connections(self) -> List[str]:
            Returns a list of names of all LND connections.

        connection(self, connection_name: str) -> LndConnectionConfig:
            Returns the LND connection configuration for the given connection name.
            Raises a ValueError if the connection name is not found.
    """

    version: str = "1"
    logging: LoggingConfig
    default_connection: str = ""
    default_database_connection: str = ""
    lnd_connections: List[LndConnectionConfig]
    tailscale: TailscaleConfig
    telegram: TelegramConfig
    database: Dict[str, DatabaseConnectionConfig]

    @field_validator("lnd_connections")
    def unique_names(cls, v):
        names = [conn.name for conn in v]
        if len(names) != len(set(names)):
            raise ValueError("Duplicate names found in lnd_connections")
        return v

    @model_validator(mode="after")
    def check_default_connection(cls, v):
        # Check that the default connection is in the list of connections
        # if it is given.
        if v.default_connection and v.default_connection not in [
            conn.name for conn in v.lnd_connections
        ]:
            raise ValueError("Default connection not found in lnd_connections")
        return v

    @model_validator(mode="after")
    def check_default_database_connection(cls, v):
        # Check that the default connection is in the list of connections
        # if it is given.
        if (
            v.default_database_connection
            and v.default_database_connection not in v.database.keys()
        ):
            raise ValueError("Default database connection not found in database")
        return v

    def list_connection_names(self) -> List[str]:
        return [connection.name for connection in self.lnd_connections]

    @property
    def connection_names(self) -> str:
        """
        Retrieve a list of connection names from the lnd_connections attribute.

        Returns:
            str: A list containing the names of all connections separated by ,.
        """
        return ", ".join([name for name in self.list_connection_names()])

    @property
    def database_names(self) -> str:
        """
        Retrieve a list of database names from the database attribute.

        Returns:
            str: A list containing the names of all databases separated by ,.
        """
        return ", ".join(self.database[self.default_database_connection].dbs.keys())

    def connection(self, connection_name: str) -> LndConnectionConfig:
        """
        Retrieve the LndConnectionConfig for a given connection name.

        Args:
            connection_name (str): The name of the connection to retrieve.

        Returns:
            LndConnectionConfig: The configuration for the specified connection.

        Raises:
            ValueError: If the connection with the specified name is not found.
        """
        for connection in self.lnd_connections:
            if connection.name == connection_name:
                return connection
        raise ValueError(f"Connection {connection_name} not found in config")

    def icon(self, connection_name: str) -> str:
        """
        Retrieves the icon associated with a given connection name.

        Args:
            connection_name (str): The name of the connection for which
            to retrieve the icon.

        Returns:
            str: The icon associated with the specified connection name.
        """
        return self.connection(connection_name).icon


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

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self, "notification_loop"):
            if self.notification_loop is not None:
                self.notification_loop.close()

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
            print("Invalid configuration:")
            print(ex)
            # exit the program with an error but no stack trace
            raise StartupFailure(ex)

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


"""
General purpose functions
"""


def format_time_delta(delta: timedelta, fractions: bool = False) -> str:
    """
    Formats a timedelta object as a string.
    If Days are present, the format is "X days, Y hours".
    Otherwise, the format is "HH:MM:SS".
    Args:
        delta (timedelta): The timedelta object to format.

    Returns:
        str: The formatted string.
    """
    if delta.days:
        return f"{delta.days} days, {delta.seconds // 3600} hours"
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if fractions:
        return f"{hours:02}:{minutes:02}:{seconds:02}.{delta.microseconds // 1000:03}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_in_flight_time(creation_date: datetime) -> str:
    """
    Calculate the time in flight for a given datetime object.
    Args:
        creation_date (datetime): The datetime object to calculate
        the time in flight for.

    Returns:
        str: The formatted string representing the timedelta.
    """

    current_time = datetime.now(tz=timezone.utc)

    if current_time < creation_date:
        in_flight_time = format_time_delta(timedelta(seconds=0.1))
    else:
        in_flight_time = format_time_delta(current_time - creation_date)

    return in_flight_time


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
        try:
            result = await func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"Function '{func.__name__[:16]}' "
                f"took {execution_time:.4f} seconds to execute"
            )
            return result
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(
                f"Function '{func.__name__[:16]}' "
                f"failed after {execution_time:.4f} seconds with error: {str(e)}"
            )
            raise

    return wrapper


def async_time_stats_decorator(runs=1):
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

    def decorator(func):
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
                        f"Function '{func.__name__[:16]}' stats - "
                        f"Last: {execution_time:.4f}s, "
                        f"Avg: {avg_time:.4f}s, "
                        f"Runs: {len(timings)}"
                    )
                    if len(timings) > 1:
                        logger.info(f"Std Dev: {stdev(timings):.4f}s")
                    timings = []  # Reset after reporting

                return result
            except Exception as e:
                end_time = time.time()
                execution_time = end_time - start_time
                logger.warning(
                    f"Function '{func.__name__[:16]}' failed after "
                    f"{execution_time:.4f}s with error: {str(e)}"
                )
                raise

        return wrapper

    return decorator
