import asyncio
import atexit
import json
import logging.config
import logging.handlers
import os
import sys
import time
from enum import StrEnum, auto
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Protocol

from dotenv import load_dotenv
from packaging import version
from pydantic import BaseModel, model_validator
from pymongo import AsyncMongoClient, MongoClient
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.database import Database
from pymongo.operations import _IndexKeyHint
from redis import Redis, RedisError
from redis.asyncio import Redis as AsyncRedis
from yaml import safe_load

from v4vapp_backend_v2.config.error_code_manager import ErrorCodeManager

load_dotenv()
logger = logging.getLogger("backend")  # __name__ is a common choice
ICON = "⚙️"

BASE_CONFIG_PATH = Path("config/")
BASE_LOGGING_CONFIG_PATH = Path(BASE_CONFIG_PATH, "logging/")
DEFAULT_CONFIG_FILENAME = "config.yaml"

BASE_DISPLAY_LOG_LEVEL = logging.INFO  # Default log level for stdout


def _parse_log_level(level: str | int | None, fallback: int) -> int:
    """Parse a logging level into its numeric value without using logging.getLevelName.

    Accepts ints, numeric strings (e.g. "20"), and named levels (case-insensitive).
    Falls back to `fallback` for unknown values.
    """
    if isinstance(level, int):
        return level
    if level is None:
        return fallback

    s = str(level).strip()
    try:
        return int(s)
    except (ValueError, TypeError):
        pass

    name = s.upper()
    if name == "WARN":
        name = "WARNING"

    name_to_level = getattr(logging, "_nameToLevel", None)
    if name_to_level and name in name_to_level:
        return name_to_level[name]

    return fallback


def make_rotation_namer(handler, rotation_folder: bool = False, min_width: int = 3):
    """Return a namer function for rotated log files.

    The namer will transform names like:
      logs/foo.jsonl.1 -> logs/foo.001.jsonl
    and if `rotation_folder` is True will place rotated files under
      logs/rotation/foo.001.jsonl

    Padding width is the greater of `min_width` and the number of digits in
    `handler.backupCount` (if present).
    """

    import os
    from pathlib import Path

    def namer(name: str) -> str:
        # Only process names that end with .<digits>
        base = str(name)
        head, sep, tail = base.rpartition(".")
        if not sep or not tail.isdigit():
            return name

        index = int(tail)
        rest = head
        rest_path = Path(rest)
        ext = rest_path.suffix
        prefix = rest[: -len(ext)] if ext else rest

        backup_count = getattr(handler, "backupCount", None) or 0
        width = max(min_width, len(str(int(backup_count))))
        index_str = f"{index:0{width}d}"

        new_filename = f"{Path(prefix).name}.{index_str}{ext}"
        parent = rest_path.parent
        if rotation_folder:
            rotation_dir = parent / "rotation"
            os.makedirs(rotation_dir, exist_ok=True)
            return str(rotation_dir / new_filename)
        return str(parent / new_filename)

    return namer


DB_RATES_COLLECTION = "rates_ts"

"""
These classes need to match the structure of the config.yaml file

"""


class StartupFailure(Exception):
    pass


class BaseConfig(BaseModel):
    pass


class AdminConfig(BaseConfig):
    highlight_users: List[str] = []
    public_api_host: str = ""


class LoggingConfig(BaseConfig):
    log_config_file: str = ""
    default_log_level: str = "DEBUG"
    console_log_level: str = "INFO"
    log_levels: Dict[str, str] = {}
    log_folder: Path = Path("logs/")
    log_notification_silent: List[str] = []
    default_notification_bot_name: str = ""
    # If True, place rotated files into a 'rotation/' subdirectory next to the
    # configured log files. Default: False (keep rotated files next to the
    # active log file with the rotation number before the extension).
    rotation_folder: bool = False

    def default_log_level_numeric(self) -> int:
        # Cache the numeric value after first parse so we don't re-parse on every call
        if not hasattr(self, "_default_log_level_numeric_cache"):
            self._default_log_level_numeric_cache = _parse_log_level(
                self.default_log_level, fallback=logging.DEBUG
            )
        return self._default_log_level_numeric_cache

    def console_log_level_numeric(self) -> int:
        # Cache the numeric value after first parse so we don't re-parse on every call
        if not hasattr(self, "_console_log_level_numeric_cache"):
            self._console_log_level_numeric_cache = _parse_log_level(
                self.console_log_level, fallback=logging.INFO
            )
        return self._console_log_level_numeric_cache


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
    lightning_fee_limit_ppm: int = 5000
    lightning_fee_estimate_ppm: int = 1000
    lightning_fee_base_msats: int = 50000


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
    coinmarketcap: str = os.getenv("COINMARKETCAP_API_KEY", "")

    @model_validator(mode="before")
    @classmethod
    def handle_none(cls, data: Any) -> dict:
        """Handle case where api_keys exists in YAML but is empty/None."""
        if data is None:
            return {}
        return data


class TimeseriesConfig(BaseConfig):
    timeField: str = ""
    metaField: str = ""
    granularity: str = "seconds"


class IndexConfig(BaseConfig):
    index_key: _IndexKeyHint
    unique: Optional[bool] = None


class CollectionConfig(BaseConfig):
    indexes: Dict[str, IndexConfig] | None = None

    # @model_validator(mode="after")
    # def validate_timeseries_and_indexes(self):
    #     if self.timeseries and self.indexes:
    #         raise ValueError("Indexes cannot be defined for a time-series collection.")
    #     return self


class DatabaseUserConfig(BaseConfig):
    password: str = ""
    roles: List[str]


class DatabaseDetailsConfig(BaseConfig):
    db_users: Dict[str, DatabaseUserConfig]
    collections: Dict[str, CollectionConfig] = {}
    timeseries: Dict[str, TimeseriesConfig] = {}


class DatabaseConnectionConfig(BaseConfig):
    hosts: List[str]
    replica_set: str | None = None
    admin_dbs: Dict[str, DatabaseDetailsConfig] | None = None
    icon: str | None = None

    @property
    def hosts_str(self) -> str:
        return ",".join(self.hosts)


class DbsConfig(BaseConfig):
    default_connection: str = ""
    default_name: str = ""
    default_user: str = ""
    connections: Dict[str, DatabaseConnectionConfig] = {}
    dbs: Dict[str, DatabaseDetailsConfig] = {}

    @property
    def default_db_connection(self) -> DatabaseConnectionConfig | None:
        if self.default_connection not in self.connections:
            return None
        return self.connections[self.default_connection]


class RedisConnectionConfig(BaseConfig):
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    kwargs: Dict[str, Any] = {}


class ExchangeMode(StrEnum):
    mainnet = auto()
    testnet = auto()


class ExchangeNetworkConfig(BaseConfig):
    """Configuration for a specific exchange network (testnet or mainnet).

    API credentials can be provided either:
    - Directly via api_key/api_secret fields
    - Via environment variable names in api_key_env_var/api_secret_env_var fields

    The resolved api_key and api_secret properties will return the actual values,
    checking env vars first if specified, then falling back to direct values.
    """

    base_url: str = ""
    api_url: str = ""  # Alternative to base_url for some exchanges

    # Direct API credentials (from config file)
    api_key_name: str = ""
    api_key: str = ""
    api_secret: str = ""

    # Environment variable names for API credentials
    api_key_env_var: str = ""
    api_secret_env_var: str = ""

    @property
    def resolved_api_key(self) -> str:
        """Get the API key, resolving from env var if specified."""
        if self.api_key_env_var:
            return os.getenv(self.api_key_env_var, "")
        return self.api_key

    @property
    def resolved_api_secret(self) -> str:
        """Get the API secret, resolving from env var if specified."""
        if self.api_secret_env_var:
            return os.getenv(self.api_secret_env_var, "")
        return self.api_secret


class ExchangeProviderConfig(BaseConfig):
    """Configuration for a single exchange provider (e.g., binance, vsc-exchange)."""

    exchange_mode: ExchangeMode = ExchangeMode.testnet
    testnet: ExchangeNetworkConfig = ExchangeNetworkConfig()
    mainnet: ExchangeNetworkConfig = ExchangeNetworkConfig()

    @property
    def use_testnet(self) -> bool:
        return self.exchange_mode == ExchangeMode.testnet

    @property
    def active_network(self) -> ExchangeNetworkConfig:
        """Get the currently active network config based on exchange_mode."""
        return self.testnet if self.use_testnet else self.mainnet


class ExchangeConfig(BaseConfig):
    """
    Configuration for exchange connections.

    Supports multiple exchanges with testnet/mainnet configurations.
    The default_exchange determines which exchange adapter to use.
    """

    default_exchange: str = "binance"
    binance: ExchangeProviderConfig = ExchangeProviderConfig()
    # vsc_exchange will be added when needed

    def get_provider(self, name: str | None = None) -> ExchangeProviderConfig:
        """Get provider config by name, defaults to default_exchange."""
        provider_name = name or self.default_exchange
        # Handle hyphenated names by converting to underscore for attribute access
        attr_name = provider_name.replace("-", "_")
        if hasattr(self, attr_name):
            return getattr(self, attr_name)
        raise ValueError(f"Unknown exchange provider: {provider_name}")


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
    customer = "customer"
    witness = "witness"


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
    role: HiveRoles = HiveRoles.customer
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


class WitnessMachineConfig(BaseConfig):
    name: str
    url: str
    signing_key: str
    priority: int = 0
    working: bool = True
    primary: bool = False
    execution_time: float = 0.0

    def __str__(self) -> str:
        return f"Witness {self.name:<18} {'(*)' if self.primary else '(b)'} {'is UP' if self.working else 'is DOWN'} Response time: {self.execution_time:.3f}s"


class WitnessConfig(BaseConfig):
    kuma_webhook_url: str = ""
    kuma_heartbeat_time: int = 60  # seconds
    witness_machines: List[WitnessMachineConfig]


class HiveConfig(BaseConfig):
    hive_accs: Dict[str, HiveAccountConfig] = {"_none": HiveAccountConfig()}
    watch_users: List[str] = []
    proposals_tracked: List[int] = []
    watch_witnesses: List[str] = []
    custom_json_ids_tracked: List[str] = []
    witness_configs: Dict[str, WitnessConfig] = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for name, acc in self.hive_accs.items():
            acc.name = name

    def witness_key_to_machine_name(self, witness_name: str, signing_key: str) -> str:
        """
        Given a witness name and signing key, return the corresponding machine name.

        Args:
            witness_name (str): The name of the witness.
            signing_key (str): The signing key of the witness machine.
        Returns:
            str: The name of the witness machine associated with the signing key.
        """
        witness_config = self.witness_configs.get(witness_name)
        if not witness_config:
            return "unknown"
        for machine in witness_config.witness_machines:
            if machine.signing_key == signing_key:
                return machine.name
        return "unknown"

    @property
    def valid_hive_config(self) -> bool:
        """
        Check if the Hive configuration is valid and complete.

        Returns:
            bool: True if the configuration is valid, False otherwise.
        """
        if not self.server_account or not self.server_account.name:
            return False
        if not self.treasury_account or not self.treasury_account.name:
            return False
        if not self.funding_account or not self.funding_account.name:
            return False
        if not self.exchange_account or not self.exchange_account.name:
            return False
        return True

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

    def get_hive_role_account(self, hive_role: HiveRoles) -> HiveAccountConfig | None:
        """
        Retrieve the first Hive account with the specified role.

        Args:
            hive_role (HiveRoles): The role of the Hive account to retrieve.

        Returns:
            HiveAccountConfig: The first Hive account with the specified role, or None if not found.
        """
        for acc in self.hive_accs.values():
            if acc.role == hive_role:
                return acc
        return None

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
    def treasury_accounts(self) -> List[HiveAccountConfig]:
        """
        Retrieve the treasury accounts from the Hive account configurations.

        Returns:
            List[HiveAccountConfig]: A list of Hive accounts with the role HiveRoles.treasury.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.treasury]

    @property
    def funding_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first funding account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.funding.
        """
        return self.funding_accounts[0] if self.funding_accounts else None

    @property
    def funding_accounts(self) -> List[HiveAccountConfig]:
        """
        Retrieve the funding accounts from the Hive account configurations.

        Returns:
            List[HiveAccountConfig]: A list of Hive accounts with the role HiveRoles.funding.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.funding]

    @property
    def exchange_account(self) -> HiveAccountConfig | None:
        """
        Retrieve the first exchange account from the Hive account configurations.

        Returns:
            HiveAccountConfig: The first Hive account with the role HiveRoles.exchange.
        """
        return self.exchange_accounts[0] if self.exchange_accounts else None

    @property
    def exchange_accounts(self) -> List[HiveAccountConfig]:
        """
        Retrieve the exchange accounts from the Hive account configurations.

        Returns:
            List[HiveAccountConfig]: A list of Hive accounts with the role HiveRoles.exchange.
        """
        return [acc for acc in self.hive_accs.values() if acc.role == HiveRoles.exchange]

    @property
    def treasury_account_names(self) -> List[str]:
        """
        Retrieve the names of the Treasury accounts.

        Returns:
            List[str]: A list containing the names of all treasury accounts.
        """
        return [acc.name for acc in self.treasury_accounts]

    @property
    def all_account_names(self) -> List[str]:
        """
        Retrieve the names of all accounts. All names must be set in order to receive any.

        Returns:
            List[str]: A list containing the names of all accounts.
        """
        if (
            self.server_account
            and self.treasury_account
            and self.funding_account
            and self.exchange_account
        ):
            return [
                self.server_account.name,
                self.treasury_account.name,
                self.funding_account.name,
                self.exchange_account.name,
            ]
        return []


class DevelopmentConfig(BaseModel):
    """
    DevelopmentConfig is a configuration class for development mode settings.

    Attributes:
        enabled (bool): Indicates whether development mode is enabled. Default is False.
        env_var (str): The name of the environment variable that will be set to True when running in development mode.
    """

    enabled: bool = False
    env_var: str = "V4VAPP_DEV_MODE"
    allowed_hive_accounts: List[str] = []


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

    version: str = "0.2.1"
    logging: LoggingConfig = LoggingConfig()
    development: DevelopmentConfig = DevelopmentConfig()

    lnd_config: LndConfig = LndConfig()
    dbs_config: DbsConfig = DbsConfig()

    redis: RedisConnectionConfig = RedisConnectionConfig()

    tailscale: TailscaleConfig = TailscaleConfig()

    notification_bots: Dict[str, NotificationBotConfig] = {}

    api_keys: ApiKeys = ApiKeys()
    hive: HiveConfig = HiveConfig()

    admin_config: AdminConfig = AdminConfig()

    exchange_config: ExchangeConfig = ExchangeConfig()

    min_config_version: ClassVar[str] = "0.2.1"

    @model_validator(mode="after")
    def check_all_defaults(self) -> "Config":
        """
        Validates the configuration after the model is initialized.
        """
        logger.info(f"{ICON} Validating the Config file and defaults....")

        # Check config version
        config_version = version.parse(self.version)
        min_version = version.parse(self.min_config_version)
        if config_version < min_version:
            raise ValueError(
                f"Config version {self.version} is less than the minimum required version {self.min_config_version}"
            )

        # Check default LND connection
        if self.lnd_config.default and self.lnd_config.default not in self.lnd_config.connections:
            raise ValueError(
                f"Default lnd connection: {self.lnd_config.default} not found in lnd_connections"
            )

        # Check default database connection
        if (
            self.dbs_config.default_connection
            and self.dbs_config.default_connection not in self.dbs_config.connections
        ):
            raise ValueError(
                f"Default database connection: {self.dbs_config.default_connection} not found in database"
            )

        # Check default database name
        if (
            self.dbs_config.default_name
            and self.dbs_config.default_name not in self.dbs_config.dbs
        ):
            raise ValueError(
                f"Default database name: {self.dbs_config.default_name} not found in dbs"
            )

        # Check for duplicate notification bot tokens
        tokens = [bot.token for bot in self.notification_bots.values()]
        if len(tokens) != len(set(tokens)):
            raise ValueError("Two notification bots have the same token")

        return self

    @property
    def valid_hive_config(self) -> bool:
        """
        Check if the Hive configuration is valid.

        Returns:
            bool: True if the configuration is valid, False otherwise.
        """
        if not self.hive:
            return False
        return self.hive.valid_hive_config

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


# MARK: Logging filters


class LoggerFunction(Protocol):
    def __call__(self, msg: object, *args: Any, **kwargs: Any) -> None: ...


# MARK: InternalConfig class
class InternalConfig:
    """
    InternalConfig is a singleton class responsible for managing the application's internal configuration, logging, database, and Redis setup.

        _instance (InternalConfig): The singleton instance of the class.
        config (Config): The validated configuration object loaded from a YAML file.
        config_filename (str): The name of the configuration file.
        base_config_path (Path): The base path for configuration files.
        base_logging_config_path (Path): The base path for logging configuration files.
        notification_loop (asyncio.AbstractEventLoop | None): The event loop used for notifications.
        notification_lock (bool): Lock indicating notification processing state.
        db_client (AsyncMongoClient): Asynchronous MongoDB client.
        db (AsyncDatabase): Asynchronous database instance.
        db_client_sync (MongoClient): Synchronous MongoDB client.
        db_uri (str): MongoDB connection URI.
        db_sync (Database): Synchronous database instance.
        redis_raw (Redis): Redis client with raw (bytes) responses.
        redis_decoded (Redis): Redis client with decoded (string) responses.


        __init__(self, bot_name: str = "", config_filename: str = DEFAULT_CONFIG_FILENAME, log_filename: str = "app.log.jsonl", *args, **kwargs):
            Initializes the singleton instance, sets up configuration, logging, and Redis clients.

        __exit__(self, exc_type, exc_value, traceback):
            Handles cleanup on context exit by calling shutdown.

        setup_config(self, config_filename: str = DEFAULT_CONFIG_FILENAME) -> None:

        setup_redis(self) -> None:
            Initializes Redis clients for both raw and decoded responses and tests connections.

        setup_logging(self, log_filename: str = "app.log") -> None:
            Configures logging using a JSON configuration file, sets up handlers, and applies log levels.

        check_notifications(self):
            Monitors the state of the notification loop and lock, printing their status at intervals.

        shutdown_logging(self):
            Closes and removes all handlers from the root logger.

        shutdown(self):
            Gracefully shuts down Redis clients, database clients, logging, and the notification event loop.

        close_db_clients_sync(self) -> None:
            Closes the synchronous database client.

        close_db_clients_async(self) -> None:
            Asynchronously closes the asynchronous database client.

    """

    _instance = None
    config: Config
    config_filename: str = DEFAULT_CONFIG_FILENAME
    base_config_path: Path = BASE_CONFIG_PATH
    base_logging_config_path: Path = BASE_LOGGING_CONFIG_PATH
    notification_loop: ClassVar[asyncio.AbstractEventLoop | None] = None
    notification_lock: ClassVar[bool] = False
    db_client: ClassVar[AsyncMongoClient] = AsyncMongoClient()
    db: ClassVar[AsyncDatabase] = AsyncDatabase(client=db_client, name="default_db")
    db_client_sync: ClassVar[MongoClient] = MongoClient()
    db_uri: ClassVar[str] = "mongodb://localhost:27017"
    db_sync: ClassVar[Database] = Database(client=db_client_sync, name="default_db")

    redis: ClassVar[Redis] = Redis()
    redis_decoded: ClassVar[Redis] = Redis(decode_responses=True)
    redis_async: ClassVar[AsyncRedis] = AsyncRedis()

    # Error code manager - singleton that handles in-memory tracking + MongoDB persistence
    error_code_manager: ClassVar[ErrorCodeManager] = ErrorCodeManager(db_enabled=False)

    local_machine_name: str = "unknown"

    @property
    def error_codes(self) -> ErrorCodeManager:
        """
        Backward-compatible property that returns the ErrorCodeManager.
        The manager provides dict-like interface for compatibility.
        """
        return InternalConfig.error_code_manager

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(InternalConfig, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        bot_name: str = "",
        config_filename: str = DEFAULT_CONFIG_FILENAME,
        log_filename: str = "",
        *args,
        **kwargs,
    ):
        if not hasattr(self, "_initialized"):
            if not log_filename:
                from v4vapp_backend_v2.helpers.general_purpose_funcs import get_entrypoint_path

                log_filepath = get_entrypoint_path()
                log_filename = log_filepath.stem
            if not log_filename.endswith(".jsonl"):
                log_filename += ".jsonl"

            self.local_machine_name = os.getenv("LOCAL_MACHINE_NAME", "unknown")
            print(f"Starting initialization... {config_filename} {log_filename}")
            self._initialized = True
            super().__init__()
            InternalConfig.notification_loop = None
            InternalConfig.notification_lock = False
            self.setup_config(config_filename)
            self.setup_logging(log_filename)
            self.setup_redis()
            # Configure error code manager with server/node info and enable DB persistence
            InternalConfig.error_code_manager.configure(
                server_id=self.server_id,
                node_name=self.node_name,
                local_machine_name=self.local_machine_name,
                db_enabled=True,
            )
            logger.info(f"{ICON} Config filename: {config_filename}")
            logger.info(f"{ICON} Log filename: {log_filename}")
            if self.config.dbs_config.default_db_connection:
                logger.info(
                    f"{ICON} Database URI: {self.config.dbs_config.default_db_connection.hosts_str}"
                )
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
            logger.info(f"{ICON} Config file found: {config_file}")
        except FileNotFoundError as ex:
            logger.error(f"{ICON} Config file not found: {ex}")
            self.config = Config()
            return
            # raise ex

        try:
            self.config = Config.model_validate(config)
        except ValueError as ex:
            logger.error(f"{ICON} Invalid configuration:")
            logger.error(ex)
            raise StartupFailure(ex)

    def setup_redis(self) -> None:
        try:
            logger.info(f"Setting up redis: {self.config.redis.host}")
            InternalConfig.redis = Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                decode_responses=False,
                **self.config.redis.kwargs,
            )
            InternalConfig.redis_decoded = Redis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                decode_responses=True,
                **self.config.redis.kwargs,
            )
            InternalConfig.redis_async = AsyncRedis(
                host=self.config.redis.host,
                port=self.config.redis.port,
                db=self.config.redis.db,
                decode_responses=True,
                **self.config.redis.kwargs,
            )
            # Optional: Test connections during startup
            InternalConfig.redis.ping()
            InternalConfig.redis_decoded.ping()
            logger.info(f"{ICON} Redis clients initialized successfully")
        except RedisError as ex:
            logger.error(f"{ICON} Failed to connect to {self.config.redis.host} Redis: {ex}")
            raise StartupFailure(f"Redis connection failure: {ex}")

    def setup_logging(self, log_filename: str = "app.log") -> None:
        try:
            config_file = Path(BASE_LOGGING_CONFIG_PATH, self.config.logging.log_config_file)
            with open(config_file) as f_in:
                config = json.load(f_in)
                try:
                    if self.config.logging.log_folder and log_filename:
                        config["handlers"]["file_json"]["filename"] = str(
                            Path(self.config.logging.log_folder, log_filename)
                        )
                except KeyError as ex:
                    print(f"KeyError in logging config no logfile set: {ex}")
                    raise ex
        except (FileNotFoundError, IsADirectoryError) as ex:
            print(f"Logging config file not found: {ex}")
            return
            raise ex

        # Ensure log folder exists
        log_folder = self.config.logging.log_folder
        log_folder.mkdir(parents=True, exist_ok=True)

        # Apply the logging configuration from the JSON file
        logging.config.dictConfig(config)

        # Adjust logging levels dynamically if
        # specified in self.config.logging.log_levels
        for logger_name, level in self.config.logging.log_levels.items():
            logger_object = logging.getLogger(logger_name)
            if logger_object:
                logger_object.setLevel(level)

        # Start the queue handler listener if it exists and has a listener attribute
        queue_handler = logging.getHandlerByName("queue_handler")
        if queue_handler is not None:
            logger.info(
                f"{ICON} Queue handler found; ensure QueueListener is started elsewhere if needed."
            )
            queue_handler.listener.start()  # type: ignore[attr-defined]
            atexit.register(queue_handler.listener.stop)  # type: ignore[attr-defined]
            try:
                InternalConfig.notification_loop = asyncio.get_running_loop()
                logger.info(f"{ICON} Found running loop for setup logging")
            except RuntimeError:  # No event loop in the current thread
                InternalConfig.notification_loop = asyncio.new_event_loop()
                logger.info(
                    f"{ICON} Started new event loop for notification logging",
                    extra={"loop": InternalConfig.notification_loop.__dict__},
                )

        # Set up the simple format string
        try:
            format_str = config["formatters"]["simple"]["format"]
        except KeyError:
            format_str = (
                "%(asctime)s.%(msecs)03d %(levelname)-8s %(module)-22s %(lineno)6d : %(message)s"
            )

        # Assign custom rotation namer to RotatingFileHandler instances.
        rotation_folder_flag = getattr(self.config.logging, "rotation_folder", False)

        file_json_handler = logging.getHandlerByName("file_json")
        if file_json_handler is not None and isinstance(
            file_json_handler, logging.handlers.RotatingFileHandler
        ):
            file_json_handler.namer = make_rotation_namer(
                file_json_handler, rotation_folder=rotation_folder_flag, min_width=3
            )

        # Also attach to any RotatingFileHandler on configured loggers (covers other handlers)
        for logger_name, logger_obj in logging.root.manager.loggerDict.items():
            if not isinstance(logger_obj, logging.Logger):
                continue
            for h in logger_obj.handlers:
                if isinstance(h, logging.handlers.RotatingFileHandler):
                    h.namer = make_rotation_namer(
                        h, rotation_folder=rotation_folder_flag, min_width=3
                    )

        # Finally, ensure any handlers attached to the root logger are handled too
        for h in logging.getLogger().handlers:
            if isinstance(h, logging.handlers.RotatingFileHandler):
                h.namer = make_rotation_namer(h, rotation_folder=rotation_folder_flag, min_width=3)

        # Helper: does root already have a console stream handler?
        def _root_has_console_handler() -> bool:
            root = logging.getLogger()
            for h in root.handlers:
                if isinstance(h, logging.StreamHandler):
                    stream = getattr(h, "stream", None)
                    if stream in (sys.stdout, sys.stderr):
                        return True
            return False

        root_logger = logging.getLogger()
        root_logger.setLevel(self.config.logging.default_log_level)

        # Independent console handler install (avoid duplicates if pytest/live logging already added one)
        STDOUT_HANDLER_NAME = "stdout_color"
        force_console = os.getenv("V4VAPP_FORCE_CONSOLE_LOG") == "1"

        already_named = any(
            getattr(h, "name", "") == STDOUT_HANDLER_NAME for h in root_logger.handlers
        )
        if (force_console or not _root_has_console_handler()) and not already_named:
            try:
                import colorlog  # ensure available

                handler = colorlog.StreamHandler(stream=sys.stdout)
                handler.set_name(STDOUT_HANDLER_NAME)
                handler.setFormatter(
                    colorlog.ColoredFormatter(
                        "%(log_color)s" + format_str,
                        datefmt="%m-%dT%H:%M:%S",
                        log_colors={
                            "DEBUG": "cyan",
                            "INFO": "blue",
                            "WARNING": "yellow",
                            "ERROR": "red",
                            "CRITICAL": "red,bg_white",
                        },
                    )
                )
            except Exception:
                # Fallback to plain StreamHandler if colorlog not available
                handler = logging.StreamHandler(stream=sys.stdout)
                handler.set_name(STDOUT_HANDLER_NAME)
                handler.setFormatter(logging.Formatter(format_str, datefmt="%Y-%m-%dT%H:%M:%S%z"))

            # Optional: keep any existing filter behavior
            try:
                # Lazy import to avoid circular import
                from v4vapp_backend_v2.config.mylogger import (
                    AddJsonDataIndicatorFilter,
                    AddNotificationBellFilter,
                    ConsoleLogFilter,
                    ErrorTrackingFilter,
                )

                handler.addFilter(ErrorTrackingFilter())
                handler.addFilter(ConsoleLogFilter())
                handler.addFilter(AddJsonDataIndicatorFilter())
                handler.addFilter(AddNotificationBellFilter())
            except Exception:
                pass

            root_logger.addHandler(handler)

        # Let app loggers propagate to root; we rely on a single root console handler
        # (prevents duplicates while keeping debug console output alive)
        logging.getLogger("v4vapp_backend_v2").propagate = True
        logging.getLogger().propagate = True

    def check_notifications(self):
        """
        Monitors the state of the notification loop and lock.
        """
        max_wait_s = 2.0
        start = time.time()
        loop = getattr(self, "notification_loop", None)
        if loop is None:
            InternalConfig.notification_lock = False
            return
        while (loop.is_running() or InternalConfig.notification_lock) and (
            time.time() - start
        ) < max_wait_s:
            print(
                f"Notification loop: {loop.is_running()} "
                f"Notification lock: {InternalConfig.notification_lock}"
            )
            time.sleep(0.2)
        # Ensure we don't stick on lock forever
        InternalConfig.notification_lock = False
        return

    def error_codes_to_dict(self) -> dict[Any, dict[str, Any]]:
        """
        Convert the error_codes dictionary to a dictionary of dictionaries.

        Returns:
            dict[Any, dict[str, Any]]: A dictionary where each key is an error code and
            each value is a dictionary representation of the corresponding ErrorCode object.
        """
        return InternalConfig.error_code_manager.to_dict()

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
        if hasattr(InternalConfig, "redis") and InternalConfig.redis is not None:
            InternalConfig.redis.close()
            logger.info(f"{ICON} Closed raw Redis client.")
        if hasattr(InternalConfig, "redis_decoded") and InternalConfig.redis_decoded is not None:
            InternalConfig.redis_decoded.close()
            logger.info(f"{ICON} Closed decoded Redis client.")

        self.close_db_clients_sync()
        try:
            loop = asyncio.get_running_loop()
            if loop is not self.notification_loop:
                loop.create_task(self.close_db_clients_async())
                if (
                    hasattr(InternalConfig, "redis_async")
                    and InternalConfig.redis_async is not None
                ):
                    loop.create_task(InternalConfig.redis_async.close())
                    logger.info(f"{ICON} Closed async Redis client.")
        except RuntimeError:
            # If there is no running loop, we can safely close the db client
            pass

        self.shutdown_logging()
        logger.info(f"{ICON} InternalConfig Shutdown: Waiting for notifications")
        self.check_notifications()
        if hasattr(self, "notification_loop") and self.notification_loop is not None:
            if self.notification_loop.is_running():
                logger.info(f"{ICON} InternalConfig Shutdown: Closing notification loop")

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
                    logger.info(
                        f"{ICON} Waiting for {len(pending_tasks)} pending tasks to complete"
                    )
                    self.notification_loop.run_until_complete(
                        asyncio.gather(*pending_tasks, return_exceptions=True)
                    )

                # Schedule the loop to stop from the current thread
                self.notification_loop.call_soon_threadsafe(self.notification_loop.stop)

                # Wait for the loop to stop by polling (non-blocking)
                while self.notification_loop.is_running():
                    logger.info(f"{ICON} Waiting for loop to stop")
                    time.sleep(0.1)

                # Shut down async generators and close the loop
                try:
                    self.notification_loop.run_until_complete(
                        self.notification_loop.shutdown_asyncgens()
                    )
                except RuntimeError:
                    logger.warning(
                        f"{ICON} Event loop already closed or not running async generators"
                    )
                finally:
                    self.notification_loop.close()
                logger.info(f"{ICON} InternalConfig Shutdown: Notification loop closed")
            else:
                # If the loop isn’t running, just close it
                self.notification_loop.close()
                logger.info(
                    f"{ICON} InternalConfig Shutdown: Notification loop closed (was not running)"
                )

    def close_db_clients_sync(self) -> None:
        """
        Manually close both synchronous and asynchronous database clients if they exist.
        """
        if hasattr(InternalConfig, "db_client_sync") and InternalConfig.db_client_sync:
            InternalConfig.db_client_sync.close()
            logger.info(f"{ICON} Closed synchronous database client.")

    async def close_db_clients_async(self) -> None:
        """
        Asynchronously close the database client if it exists.
        This method is intended to be used in an asynchronous context.
        """
        if hasattr(InternalConfig, "db_client") and InternalConfig.db_client:
            await InternalConfig.db_client.close()
            logger.info(f"{ICON} Closed asynchronous database client.")

    # MARK: Special Properties
    @property
    def server_id(self) -> str:
        """
        Retrieve the server ID from the configuration.

        Returns:
            str: The server ID, which is the name of the server account.
        """
        if self.config.hive.server_account:
            return self.config.hive.server_account.name
        return ""

    @property
    def node_name(self) -> str:
        """
        Retrieve the default Lightning node name from the configuration.

        Returns:
            str: The node name, which is the name of the LND node.
        """
        if self.config.lnd_config.default:
            return self.config.lnd_config.default
        return ""

    @property
    def binance_config(self) -> ExchangeProviderConfig:
        """
        Shortcut to get the binance exchange provider config.

        Returns:
            ExchangeProviderConfig: The Binance exchange provider configuration.
        """
        return self.config.exchange_config.get_provider("binance")
