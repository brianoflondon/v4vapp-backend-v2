import asyncio
import atexit
import functools
import json
import logging.config
import logging.handlers
import os
import sys
import time
from enum import StrEnum
from pathlib import Path
from statistics import mean, stdev
from typing import Any, Dict, List, Optional, Protocol, override

import colorlog
from pydantic import BaseModel, model_validator
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


class BaseConfig(BaseModel):
    pass


class LoggingConfig(BaseConfig):
    log_config_file: str = ""
    default_log_level: str = "DEBUG"
    log_levels: Dict[str, str] = {}
    log_folder: Path = Path("logs/")
    log_notification_silent: List[str] = []


class LndConnectionConfig(BaseConfig):
    icon: str = ""
    address: str = ""
    options: list = []
    certs_path: Path = Path(".certs/")
    macaroon_filename: str = ""
    cert_filename: str = ""
    use_proxy: str = ""


class TailscaleConfig(BaseConfig):
    tailnet_name: str = ""
    notification_server: str = ""
    notification_server_port: int = 0


class TelegramConfig(BaseConfig):
    chat_id: int = 0


class NotificationBotConfig(BaseConfig):
    name: str = ""
    token: str = ""
    chat_id: int = 0


class ApiKeys(BaseConfig):
    binance_api_key: str = os.getenv("BINANCE_API_KEY", "")
    binance_api_secret: str = os.getenv("BINANCE_API_SECRET", "")
    binance_testnet_api_key: str = os.getenv("BINANCE_TESTNET_API_KEY", "")
    binance_testnet_api_secret: str = os.getenv("BINANCE_TESTNET_API_SECRET", "")
    coinmarketcap: str = os.getenv("COINMARKETCAP_API_KEY", "")


class IndexConfig(BaseConfig):
    index_key: _IndexKeyHint | None = None
    unique: Optional[bool] = None


class CollectionConfig(BaseConfig):
    indexes: Dict[str, IndexConfig] | None = None


class DatabaseUserConfig(BaseConfig):
    password: str
    roles: List[str]


class DatabaseDetailsConfig(BaseConfig):
    db_users: Dict[str, DatabaseUserConfig]
    collections: Optional[Dict[str, CollectionConfig | None]] = None


class DatabaseConnectionConfig(BaseConfig):
    hosts: List[str]
    replica_set: str | None = None
    admin_dbs: Dict[str, DatabaseDetailsConfig] | None = None
    icon: str | None = None


class RedisConnectionConfig(BaseConfig):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    kwargs: Dict[str, Any] = {}


class HiveRoles(StrEnum):
    """
    HiveRoles is an enumeration that defines different roles within the Hive system.

    Attributes:
        server (str): Represents the server role.
        treasury (str): Represents the treasury role.
    """

    server = "server"
    treasury = "treasury"


class HiveAccountConfig(BaseConfig):
    """
    HiveAccountConfig is a configuration class for Hive account settings.

    Attributes:
        role (HiveRoles): The role assigned to the Hive account. Default is HiveRoles.server.
        posting_key (str): The posting key for the Hive account. Default is an empty string.
        active_key (str): The active key for the Hive account. Default is an empty string.
        memo_key (str): The memo key for the Hive account. Default is an empty string.
    """

    name: str = ""
    role: HiveRoles = HiveRoles.server
    posting_key: str = ""
    active_key: str = ""
    memo_key: str = ""
    hbd_balance: str = ""  # HBD balance of the account
    hive_balance: str = ""  # HIVE balance of the account

    @property
    def keys(self) -> List[str]:
        """
        Retrieve the keys of the Hive account.

        Returns:
            List[str]]: A list of the private keys for the account.
        """
        return [key for key in [self.posting_key, self.active_key, self.memo_key] if key]


class HiveConfig(BaseConfig):
    hive_accs: Dict[str, HiveAccountConfig] = {"_none": HiveAccountConfig()}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, acc in self.hive_accs.items():
            acc.name = name

    @property
    def memo_keys(self) -> List[str]:
        """
        Retrieve the memo keys of all Hive accounts.

        Returns:
            List[str]: A list containing the memo keys of all Hive accounts.
        """
        return [acc.memo_key for acc in self.hive_accs.values() if acc.memo_key]

    @property
    def hive_acc_names(self) -> List[str]:
        """
        Retrieve the names of all Hive accounts.

        Returns:
            List[str]: A list containing the names of all Hive accounts.
        """
        return list(self.hive_accs.keys())

    @property
    def server_accounts(self) -> List[HiveAccountConfig]:
        """
        Retrieve the server accounts from the Hive account configurations.

        Returns:
            List[HiveAccountConfig]: A list of Hive accounts with the role HiveRoles.server.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.server]

    @property
    def server_account_names(self) -> List[str]:
        """
        Retrieve the names of the server accounts.

        Returns:
            List[str]: A list containing the names of all server accounts.
        """
        return [acc.name for acc in self.server_accounts]

    @property
    def treasury_accounts(self) -> List[HiveAccountConfig]:
        """
        Retrieve the treasury accounts from the Hive account configurations.

        Returns:
            List[HiveAccountConfig]: A list of Hive accounts with the role HiveRoles.treasury.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.treasury]

    @property
    def treasury_account_names(self) -> List[str]:
        """
        Retrieve the names of the Treasury accounts.

        Returns:
            List[str]: A list containing the names of all treasury accounts.
        """
        return [acc.name for acc in self.treasury_accounts]


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

    # Defaults
    default_lnd_connection: str = ""
    default_db_connection: str = ""
    default_db_name: str = ""

    # Connections and DB configs
    lnd_connections: Dict[str, LndConnectionConfig]
    db_connections: Dict[str, DatabaseConnectionConfig]
    dbs: Dict[str, DatabaseDetailsConfig]
    redis: RedisConnectionConfig = RedisConnectionConfig()

    tailscale: TailscaleConfig = TailscaleConfig()

    telegram: TelegramConfig = TelegramConfig()
    notification_bots: Dict[str, NotificationBotConfig] = {}

    api_keys: ApiKeys = ApiKeys()
    hive: HiveConfig = HiveConfig()

    @model_validator(mode="after")
    def check_all_defaults(cls, v: Any):
        # Check that the default connection is in the list of connections
        # if it is given.
        print("Checking all defaults")
        if v.default_lnd_connection and v.default_lnd_connection not in v.lnd_connections.keys():
            raise ValueError("Default connection not found in lnd_connections")
        if v.default_db_connection and v.default_db_connection not in v.db_connections.keys():
            raise ValueError("Default database connection not found in database")
        if v.default_db_name and v.default_db_name not in v.dbs.keys():
            raise ValueError("Default database name not found in databases")

        # check if two notification bots have the same token
        tokens = [bot.token for bot in v.notification_bots.values()]
        if len(tokens) != len(set(tokens)):
            raise ValueError("Two notification bots have the same token")

        return v

    @property
    def lnd_connections_names(self) -> str:
        """
        Retrieve a list of connection names from the lnd_connections attribute.

        Returns:
            str: A list containing the names of all connections separated by ,.
        """
        return ", ".join(self.lnd_connections.keys())

    @property
    def db_connections_names(self) -> str:
        """
        Retrieve a list of connection names from the db_connections attribute.

        Returns:
            str: A list containing the names of all connections separated by ,.
        """
        return ", ".join(self.db_connections.keys())

    @property
    def dbs_names(self) -> str:
        """
        Retrieve a list of database names from the database attribute.

        Returns:
            str: A list containing the names of all databases separated by ,.
        """
        return ", ".join(self.dbs.keys())

    def find_notification_bot_name(self, token: str) -> str:
        """
        Retrieve a list of bot tokens from the telegram_bots attribute.

        Returns:
            str: A list containing the bot tokens separated by ,.
        """
        [bot_name] = [name for name, bot in self.notification_bots.items() if bot.token == token]
        if not bot_name:
            raise ValueError("Bot name not found in the configuration")
        return bot_name


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
    notification_lock = False
    base_config_path: Path = BASE_CONFIG_PATH
    base_logging_config_path: Path = BASE_LOGGING_CONFIG_PATH

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
                self.shutdown()

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
            config_file = Path(BASE_LOGGING_CONFIG_PATH, self.config.logging.log_config_file)
            with open(config_file) as f_in:
                config = json.load(f_in)
        except FileNotFoundError as ex:
            logger.error(f"Logging config file not found: {ex}")
            raise ex

        # Ensure log folder exists
        log_folder = self.config.logging.log_folder
        log_folder.mkdir(exist_ok=True)

        # Apply the logging configuration from the JSON file
        logging.config.dictConfig(config)

        # Adjust logging levels dynamically if
        # specified in self.config.logging.log_levels
        for logger_name, level in self.config.logging.log_levels.items():
            logger_object = logging.getLogger(logger_name)
            if logger_object:
                logger_object.setLevel(level)

        # Start the queue handler listener if it exists
        queue_handler = logging.getHandlerByName("queue_handler")
        if queue_handler is not None:
            queue_handler.listener.start()
            try:
                self.notification_loop = asyncio.get_running_loop()
                logger.info("Found running loop for setup logging")
            except RuntimeError:  # No event loop in the current thread
                self.notification_loop = asyncio.new_event_loop()
                logger.info(
                    "Started new event loop for notification logging",
                    extra={"loop": self.notification_loop.__dict__},
                )
            atexit.register(lambda: asyncio.run(self.async_shutdown()))

        # Set up the simple format string
        try:
            format_str = config["formatters"]["simple"]["format"]
        except KeyError:
            format_str = (
                "%(asctime)s.%(msecs)03d %(levelname)-8s %(module)-22s %(lineno)6d : %(message)s"
            )

        # Custom namer for file_json handler
        def custom_log_namer(name):
            return name

        file_json_handler = logging.getHandlerByName("file_json")
        if file_json_handler is not None:
            file_json_handler.namer = custom_log_namer

        # Set up the colorlog handler for stdout
        handler = colorlog.StreamHandler()
        handler.setFormatter(
            colorlog.ColoredFormatter(
                "%(log_color)s" + format_str,
                datefmt="%Y-%m-%dT%H:%M:%S%z",
                log_colors={
                    "DEBUG": "cyan",
                    "INFO": "blue",
                    "WARNING": "yellow",
                    "ERROR": "red",
                    "CRITICAL": "red,bg_white",
                },
                stream=sys.stdout,
            )
        )
        # Add the colorlog handler to the root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        root_logger.setLevel(self.config.logging.default_log_level)

        # Optional: Add filters if needed
        handler.addFilter(ConsoleLogFilter())

    async def async_shutdown(self):
        await asyncio.sleep(0.1)
        self.shutdown()

    def shutdown(self):
        if hasattr(self, "notification_loop") and self.notification_loop is not None:
            if self.notification_loop.is_running():
                logger.info("Closing notification loop")

                # Wait for all pending tasks to complete
                pending_tasks = asyncio.all_tasks(loop=self.notification_loop)
                if pending_tasks:
                    logger.info("Waiting for pending tasks to complete")
                    self.notification_loop.run_until_complete(
                        asyncio.gather(*pending_tasks, return_exceptions=True)
                    )

                # Schedule the loop to stop from the current thread
                self.notification_loop.call_soon_threadsafe(self.notification_loop.stop)

                # Wait for the loop to stop by polling (non-blocking)
                while self.notification_loop.is_running():
                    logger.info("Waiting for loop to stop")
                    time.sleep(0.1)

                # Stop the loop before shutting down async generators
                self.notification_loop.stop()

                # Shut down async generators and close the loop
                try:
                    self.notification_loop.run_until_complete(
                        self.notification_loop.shutdown_asyncgens()
                    )
                except RuntimeError:
                    logger.warning("Event loop already closed or not running async generators")
                finally:
                    self.notification_loop.close()
                print("InternalConfig Shutdown Notification loop closed")
            else:
                # If the loop isn’t running, just close it
                self.notification_loop.close()
                print("InternalConfig Shutdown Notification loop closed (was not running)")


"""
General purpose functions
"""


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
                f"Function '{func.__name__[:16]}' took {execution_time:.4f} seconds to execute"
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
