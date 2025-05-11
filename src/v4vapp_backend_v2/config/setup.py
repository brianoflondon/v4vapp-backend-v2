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
from typing import Any, ClassVar, Dict, List, Optional, Protocol, override

import colorlog
from packaging import version
from pydantic import BaseModel, model_validator
from pymongo.operations import _IndexKeyHint
from yaml import safe_load

logger = logging.getLogger("backend")  # __name__ is a common choice


BASE_CONFIG_PATH = Path("config/")
BASE_LOGGING_CONFIG_PATH = Path(BASE_CONFIG_PATH, "logging/")
DEFAULT_CONFIG_FILENAME = "config.yaml"

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
    log_filename: Path = Path("v4vapp-backend-v2.log.jsonl")
    log_notification_silent: List[str] = []
    default_notification_bot_name: str = ""


class LndConnectionConfig(BaseConfig):
    icon: str = ""
    address: str = ""
    options: list = []
    certs_path: Path = Path(".certs/")
    macaroon_filename: str = ""
    cert_filename: str = ""
    use_proxy: str = ""


class LndConfig(BaseConfig):
    default: str = ""
    connections: Dict[str, LndConnectionConfig] = {}


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


class TimeseriesConfig(BaseConfig):
    timeField: str = ""
    metaField: str = ""
    granularity: str = "seconds"


class IndexConfig(BaseConfig):
    index_key: _IndexKeyHint | None = None
    unique: Optional[bool] = None


class CollectionConfig(BaseConfig):
    indexes: Dict[str, IndexConfig] | None = None
    timeseries: TimeseriesConfig | None = None

    @model_validator(mode="after")
    def validate_timeseries_and_indexes(self):
        if self.timeseries and self.indexes:
            raise ValueError("Indexes cannot be defined for a time-series collection.")
        return self


class DatabaseUserConfig(BaseConfig):
    password: str = ""
    roles: List[str]


class DatabaseDetailsConfig(BaseConfig):
    db_users: Dict[str, DatabaseUserConfig]
    collections: Optional[Dict[str, CollectionConfig | TimeseriesConfig | None]] = None


class DatabaseConnectionConfig(BaseConfig):
    hosts: List[str]
    replica_set: str | None = None
    admin_dbs: Dict[str, DatabaseDetailsConfig] | None = None
    icon: str | None = None


class DbsConfig(BaseConfig):
    default_connection: str = ""
    default_name: str = ""
    default_user: str = ""
    connections: Dict[str, DatabaseConnectionConfig] = {}
    dbs: Dict[str, DatabaseDetailsConfig] = {}


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
        funding (str): Represents the funding role: this account is recognized when moving
            Owner's equity funds into the treasury account.
    """

    server = "server"
    treasury = "treasury"
    funding = "funding"
    exchange = "exchange"


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
    watch_users: List[str] = []
    proposals_tracked: List[int] = []
    watch_witnesses: List[str] = []
    custom_json_ids_tracked: List[str] = []

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
    def server_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first server account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.server.
        """
        return self.server_accounts[0] if self.server_accounts else None

    @property
    def treasury_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first treasury account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.treasury.
        """
        return self.treasury_accounts[0] if self.treasury_accounts else None

    @property
    def funding_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first funding account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.funding.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.funding][0]

    @property
    def exchange_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first exchange account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.exchange.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.exchange][0]

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
    version (str): The version of the configuration. Default is an empty string.
    lnd_config (LndConfig): Configuration for LND connections.
    dbs_config (DbsConfig): Configuration for database connections.
    redis (RedisConnectionConfig): Configuration for Redis connection.
    notification_bots (Dict[str, NotificationBotConfig]): Dictionary of notification bot configurations.
    api_keys (ApiKeys): Configuration for API keys.
    hive (HiveConfig): Configuration for Hive.
    min_config_version (ClassVar[str]): Minimum required configuration version.

    check_all_defaults(cls, v: Config) -> Config:
        Validates the configuration after initialization to ensure all defaults are properly set.
        Raises ValueError if any validation fails.

    lnd_connections_names(self) -> str:
        Retrieves a comma-separated list of LND connection names.

    db_connections_names(self) -> str:
        Retrieves a comma-separated list of database connection names.

    dbs_names(self) -> str:
        Retrieves a comma-separated list of database names.

    find_notification_bot_name(self, token: str) -> str:
        Finds the name of a notification bot based on its token.
        Raises ValueError if the token is not found.
    """

    version: str = ""
    logging: LoggingConfig

    lnd_config: LndConfig = LndConfig()
    dbs_config: DbsConfig = DbsConfig()

    redis: RedisConnectionConfig = RedisConnectionConfig()

    tailscale: TailscaleConfig = TailscaleConfig()

    notification_bots: Dict[str, NotificationBotConfig] = {}

    api_keys: ApiKeys = ApiKeys()
    hive: HiveConfig = HiveConfig()

    min_config_version: ClassVar[str] = "0.2.0"

    @model_validator(mode="after")
    def check_all_defaults(cls, v: "Config") -> "Config":
        # Check that the default connection is in the list of connections
        # if it is given.
        logger.info("Validating the Config file and defaults....")
        config_version = version.parse(v.version)
        min_version = version.parse(cls.min_config_version)
        if config_version < min_version:
            raise ValueError(
                f"Config version {v.version} is less than the minimum required version {cls.min_config_version}"
            )

        if v.lnd_config.default and v.lnd_config.default not in v.lnd_config.connections.keys():
            raise ValueError(
                f"Default lnd connection: {v.lnd_config.default} not found in lnd_connections"
            )

        if (
            v.dbs_config.default_connection
            and v.dbs_config.default_connection not in v.dbs_config.connections.keys()
        ):
            raise ValueError(
                f"Default database connection: {v.dbs_config.default_connection} not found in database"
            )
        if v.dbs_config.default_name and v.dbs_config.default_name not in v.dbs_config.dbs.keys():
            raise ValueError(
                f"Default database name: {v.dbs_config.default_name} not found in dbs"
            )

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
        return ", ".join(self.lnd_config.connections.keys())

    @property
    def db_connections_names(self) -> str:
        """
        Retrieve a list of connection names from the db_connections attribute.

        Returns:
            str: A list containing the names of all connections separated by ,.
        """
        return ", ".join(self.dbs_config.connections.keys())

    @property
    def dbs_names(self) -> str:
        """
        Retrieve a list of database names from the database attribute.

        Returns:
            str: A list containing the names of all databases separated by ,.
        """
        return ", ".join(self.dbs_config.dbs.keys())

    def find_notification_bot_name(self, token: str) -> str:
        """
        Retrieve a list of bot tokens from the notification_bots attribute.

        Returns:
            str: A list containing the bot tokens separated by ,.
        """
        bot_names = [name for name, bot in self.notification_bots.items() if bot.token == token]
        if not bot_names:
            raise ValueError("Bot name not found in the configuration")
        return bot_names[0]


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
    config_filename: str = DEFAULT_CONFIG_FILENAME
    base_config_path: Path = BASE_CONFIG_PATH
    base_logging_config_path: Path = BASE_LOGGING_CONFIG_PATH
    notification_loop: ClassVar[asyncio.AbstractEventLoop | None] = None
    notification_lock: ClassVar[bool] = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InternalConfig, cls).__new__(cls)
        return cls._instance

    def __init__(
        self, bot_name: str = "", config_filename: str = DEFAULT_CONFIG_FILENAME, *args, **kwargs
    ):
        if not hasattr(self, "_initialized"):
            logger.info(f"Config filename: {config_filename}")
            super().__init__()
            InternalConfig.notification_loop = None  # Initialize notification_loop
            InternalConfig.notification_lock = False
            self.setup_config(config_filename)
            self.setup_logging()
            self._initialized = True
            atexit.register(self.shutdown)

    def __exit__(self, exc_type, exc_value, traceback):
        self.shutdown()

    def setup_config(self, config_filename: str = DEFAULT_CONFIG_FILENAME) -> None:
        try:
            # test if config_filename already has the default BASE_CONFIG_PATH
            if config_filename.startswith(str(f"{BASE_CONFIG_PATH}/")):
                config_file = str(Path(config_filename))
            else:
                # if not, prepend the base config path
                config_file = str(Path(BASE_CONFIG_PATH, config_filename))
            with open(config_file) as f_in:
                config = safe_load(f_in)
            self.config_filename = config_filename
            print(f"Config file found: {config_file}")
        except FileNotFoundError as ex:
            print(f"Config file not found: {ex}")
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
                try:
                    if self.config.logging.log_folder and self.config.logging.log_filename:
                        config["handlers"]["file_json"]["filename"] = str(
                            Path(self.config.logging.log_folder, self.config.logging.log_filename)
                        )
                except KeyError as ex:
                    print(f"KeyError in logging config no logfile set: {ex}")
                    raise ex
        except FileNotFoundError as ex:
            print(f"Logging config file not found: {ex}")
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

    def check_notifications(self):
        """
        Monitors the state of the notification loop and lock.

        This method continuously checks the status of the `notification_loop` and
        `notification_lock` attributes, printing their states at regular intervals
        until the notification loop is no longer running and the lock is released.

        Returns:
            None
        """
        if getattr(self, "notification_loop") and self.notification_loop is not None:
            while self.notification_loop.is_running() or self.notification_lock:
                print(
                    f"Notification loop: {self.notification_loop.is_running()} "
                    f"Notification lock: {self.notification_lock}"
                )
                time.sleep(0.5)
        return

    def shutdown_logging(self):
        for handler in logging.root.handlers[:]:
            handler.close()
            logging.root.removeHandler(handler)

    def shutdown(self):
        """
        Gracefully shuts down the notification loop and ensures all pending tasks are completed.

        This method performs the following steps:
        1. Checks if the `notification_loop` attribute exists and is not `None`.
        2. If the loop is running:
            - Logs the intention to close the notification loop.
            - Retrieves the current task running the shutdown logic.
            - Gathers all other pending tasks in the loop and waits for their completion.
            - Schedules the loop to stop from the current thread.
            - Polls until the loop stops running.
            - Attempts to shut down asynchronous generators and closes the loop.
        3. If the loop is not running, it simply closes the loop.
        4. Logs the status of the shutdown process.

        Notes:
        - Handles exceptions if the event loop is already closed or no asynchronous generators are running.
        - Ensures proper cleanup of resources associated with the notification loop.

        Raises:
            RuntimeError: If the event loop is already closed or cannot shut down async generators.
        """
        self.shutdown_logging()
        logger.info("InternalConfig Shutdown: Waiting for notifications")
        self.check_notifications()
        if hasattr(self, "notification_loop") and self.notification_loop is not None:
            if self.notification_loop.is_running():
                logger.info("InternalConfig Shutdown: Closing notification loop")

                # # Get the current task (the one running the shutdown logic)
                # current_task = asyncio.current_task(loop=self.notification_loop)
                current_task = []
                # Gather all tasks except the current one
                pending_tasks = [
                    task
                    for task in asyncio.all_tasks(loop=self.notification_loop)
                    if task is not current_task
                ]

                # Wait for all pending tasks to complete
                if pending_tasks:
                    logger.info(f"Waiting for {len(pending_tasks)} pending tasks to complete")
                    self.notification_loop.run_until_complete(
                        asyncio.gather(*pending_tasks, return_exceptions=True)
                    )

                # Schedule the loop to stop from the current thread
                self.notification_loop.call_soon_threadsafe(self.notification_loop.stop)

                # Wait for the loop to stop by polling (non-blocking)
                while self.notification_loop.is_running():
                    logger.info("Waiting for loop to stop")
                    time.sleep(0.1)

                # Shut down async generators and close the loop
                try:
                    self.notification_loop.run_until_complete(
                        self.notification_loop.shutdown_asyncgens()
                    )
                except RuntimeError:
                    logger.warning("Event loop already closed or not running async generators")
                finally:
                    self.notification_loop.close()
                logger.info("InternalConfig Shutdown: Notification loop closed")
            else:
                # If the loop isn’t running, just close it
                self.notification_loop.close()
                logger.info("InternalConfig Shutdown: Notification loop closed (was not running)")


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
            logger.debug(
                f"Function '{func.__qualname__[:26]}' took {execution_time:.4f} seconds to execute"
            )
            return result
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.warning(
                f"Function '{func.__qualname__[:26]}' "
                f"failed after {execution_time:.4f} seconds with error: {str(e)}",
                extra={"notification": False, "error": e},
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
                        f"Function '{func.__qualname__[:26]}' stats - "
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
                    f"Function '{func.__qualname__[:26]}' failed after "
                    f"{execution_time:.4f}s with error: {str(e)}"
                )
                raise

        return wrapper

    return decorator
