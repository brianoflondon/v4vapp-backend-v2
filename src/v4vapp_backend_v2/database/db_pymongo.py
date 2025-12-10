import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict
from urllib.parse import quote_plus

from pymongo import AsyncMongoClient, MongoClient, timeout
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.database import Database
from pymongo.errors import CollectionInvalid, OperationFailure, ServerSelectionTimeoutError

from v4vapp_backend_v2.config.setup import CollectionConfig, InternalConfig, logger

app_name = os.path.basename(sys.argv[0])

DATABASE_ICON = "📁"


class DBConnConnectionException(Exception):
    """Custom exception for database connection errors."""

    pass


class DBConn:
    db_conn: str = ""
    db_name: str = ""
    db_user: str = ""
    _setup: bool = False

    def __init__(self, db_conn: str = "", db_name: str = "", db_user: str = ""):
        config = InternalConfig().config
        dbs_config = config.dbs_config

        self.db_conn = db_conn if db_conn else dbs_config.default_connection
        self.db_name = db_name if db_name else dbs_config.default_name
        self.db_user = db_user if db_user else dbs_config.default_user

    def client(self) -> AsyncMongoClient[Dict[str, Any]]:
        """
        Returns an instance of AsyncMongoClient with the constructed URI.

        This method creates a MongoDB client using the URI built from the
        database connection, name, and user.

        Returns:
            AsyncMongoClient: An instance of AsyncMongoClient connected to the database.
        """

        client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(
            self.uri,  # Ensure URI is properly formatted (e.g., "mongodb://host:port")
            tz_aware=True,  # Enables timezone-aware datetime objects
            connectTimeoutMS=480_000,  # Timeout for establishing a connection (120 seconds)
            serverSelectionTimeoutMS=480_000,  # Timeout for selecting a server (120 seconds)
            retryWrites=True,  # Automatically retry write operations on failure
            retryReads=True,  # Automatically retry read operations on failure
            readPreference="primaryPreferred",  # Prefer primary for reads
            w=1,  # Ensure write operations are acknowledged by the majority of nodes
            journal=True,  # If True block until write operations have been committed to the journal
            appName=app_name,  # Optional: for MongoDB monitoring
        )

        return client

    def admin_client(self) -> AsyncMongoClient[Dict[str, Any]]:
        """
        Returns an instance of AsyncMongoClient for the admin database.

        This method creates a MongoDB client specifically for the admin database
        using the admin URI.

        Returns:
            AsyncMongoClient: An instance of AsyncMongoClient connected to the admin database.
        """
        return AsyncMongoClient(self.admin_uri, tz_aware=True)

    def db(self) -> AsyncDatabase[Dict[str, Any]]:
        """
        Returns the database instance for the specified database name.

        This method retrieves the database instance using the constructed URI.

        Returns:
            AsyncDatabase[Dict[str, Any]]: The database instance for the specified database name.
        """
        return self.client()[self.db_name]

    def _build_uri_from_config(
        self, db_conn: str = "", db_name: str = "", db_user: str = ""
    ) -> str:
        """
        Constructs a MongoDB URI from the database configuration.

        This method builds a MongoDB connection URI using the database configuration
        provided in the `db_config` attribute. It supports both single-host and
        replica set configurations.

        Returns:
            str: The constructed MongoDB URI.
        """
        db_conn = self.db_conn if not db_conn else db_conn
        db_name = self.db_name if not db_name else db_name
        db_user = self.db_user if not db_user else db_user

        config = InternalConfig().config
        db_connection = config.dbs_config.connections[db_conn]

        # Special case for github tests with no Admin auth

        auth_source = f"?authSource={db_name}"
        db_password = ""

        if db_connection.replica_set == "rsPytest":
            db_password = ""
            db_user = ""
            auth_source = ""
        elif db_name == "admin" and db_connection.admin_dbs:
            admin_db_password = db_connection.admin_dbs["admin"].db_users["admin"].password
            db_password = ":" + quote_plus(admin_db_password) + "@"
            db_user = quote_plus(db_user)
        else:
            db_password = ":" + quote_plus(self.db_password) + "@"
            db_user = quote_plus(db_user)

        if db_connection.replica_set:
            replica_set = f"&replicaSet={db_connection.replica_set}"
        else:
            replica_set = ""

        hosts = ",".join(db_connection.hosts) if db_conn else "localhost"

        return f"mongodb://{db_user}{db_password}{hosts}/{auth_source}{replica_set}"

    @property
    def db_password(self) -> str:
        """
        Retrieves the database password from the configuration.

        This property fetches the password for the database user from the
        configuration settings.

        Returns:
            str: The password for the database user.
        """
        # if InternalConfig().config.dbs_config.connections[self.db_conn].replica_set == "rsPytest":
        #     return ""
        return InternalConfig().config.dbs_config.dbs[self.db_name].db_users[self.db_user].password

    @property
    def db_roles(self) -> list[str]:
        """
        Retrieves the roles assigned to the database user.

        This property fetches the roles for the database user from the
        configuration settings.

        Returns:
            list[str]: A list of roles assigned to the database user.
        """
        return InternalConfig().config.dbs_config.dbs[self.db_name].db_users[self.db_user].roles

    @property
    def uri(self) -> str:
        """
        Constructs a MongoDB URI using the database connection details.

        This property builds a MongoDB connection URI based on the provided
        database connection, name, and user.

        Returns:
            str: The constructed MongoDB URI.
        """
        return self._build_uri_from_config(
            db_conn=self.db_conn, db_name=self.db_name, db_user=self.db_user
        )

    @property
    def admin_uri(self) -> str:
        """
        Constructs a MongoDB URI for the admin database.

        This method builds a MongoDB connection URI specifically for the admin database,
        using the database configuration provided in the `db_config` attribute.

        Returns:
            str: The constructed MongoDB URI for the admin database.
        """
        return self._build_uri_from_config(db_conn=self.db_conn, db_name="admin", db_user="admin")

    async def test_connection(self, timeout_seconds: float = 10, admin: bool = False) -> None:
        """
        Test the database connection by pinging the database.

        This method attempts to connect to the database and execute a ping command
        to verify that the connection is successful.

        Parameters:
            timeout_seconds (float): The timeout in seconds for the connection attempt.
            admin (bool): If True, use the admin URI for the connection.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        uri = self.admin_uri if admin else self.uri
        client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(uri, tz_aware=True)
        try:
            with timeout(timeout_seconds):
                async with client:
                    db = client[self.db_name]
                    ans = await db.command("ping")
                    assert ans.get("ok", None)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to the database: {e}") from e

    # MARK: Database setup methods Async
    def client_sync(self) -> MongoClient[Dict[str, Any]]:  # pragma: no cover
        """
        Returns an instance of MongoClient with the constructed URI.

        This method creates a MongoDB client using the URI built from the
        database connection, name, and user.

        Returns:
            MongoClient: An instance of MongoClient connected to the database.
        """
        timeout_ms = 600_000 if InternalConfig().config.development.enabled else 10_000
        return MongoClient(
            self.uri,  # Ensure URI is properly formatted (e.g., "mongodb://host:port")
            tz_aware=True,  # Enables timezone-aware datetime objects
            connectTimeoutMS=timeout_ms,  # Timeout for establishing a connection
            serverSelectionTimeoutMS=timeout_ms,  # Timeout for selecting a server
            retryWrites=True,  # Automatically retry write operations on failure
            retryReads=True,  # Automatically retry read operations on failure
            readPreference="primaryPreferred",  # Prefer primary for reads
            w=1,  # Ensure write operations are acknowledged by the majority of nodes
            journal=True,  # If True block until write operations have been committed to the journal
            appName=app_name,  # Optional: for MongoDB monitoring
        )

    def db_sync(self) -> Database[Dict[str, Any]]:  # pragma: no cover
        """
        Returns the database instance for the specified database name.
        This method retrieves the database instance using the constructed URI.
        Returns:
            Database[Dict[str, Any]]: The database instance for the specified database name.
        """
        return self.client_sync()[self.db_name]

    async def setup_database(self) -> None:
        """
        Set up the database connection and perform initial setup tasks.

        This method establishes a connection to the database, sets up the user,
        and prepares the collections and indexes as defined in the configuration.

        Raises:
            DBConnConnectionException: If there is an error connecting to the database.
        """
        InternalConfig.db_uri = self.uri
        if not self._setup:
            self._setup = True
            # Use a short timeout for the admin client setup if the database is not reachable
            admin_client: AsyncMongoClient[Dict[str, Any]] = AsyncMongoClient(
                self.admin_uri,
                tz_aware=True,
                connectTimeoutMS=600_000,
                serverSelectionTimeoutMS=5_000,
            )
            async with admin_client:
                await self.setup_user(admin_client=admin_client)
                await self.setup_collections_indexes(admin_client=admin_client)
                await self._create_timeseries(admin_client=admin_client)
            logger.info(
                f"{DATABASE_ICON} {logger.name} "
                f"Database {self.db_name} is set up with user {self.db_user}"
            )
            # if InternalConfig.db_client:
            #     await InternalConfig.db_client.close()
            InternalConfig.db_client = AsyncMongoClient(self.uri, tz_aware=True)
            InternalConfig.db = InternalConfig.db_client[self.db_name]
            logger.info(
                f"{DATABASE_ICON} Database {self.db_name} client is set up for InternalConfig"
            )
        logger.info(f"{DATABASE_ICON} {logger.name} Database {self.db_name} is already set up.")

    async def setup_user(self, admin_client: AsyncMongoClient[Dict[str, Any]]) -> None:
        """
        Set up the user.

        This method is intended to be called after the connection URI has been
        established. It can be used to perform any necessary setup tasks.
        """
        create_user = {}
        try:
            admin_db = admin_client[self.db_name]
            await admin_db["startup_collection"].insert_one(
                {"startup": "complete", "timestamp": datetime.now(tz=timezone.utc)}
            )
            users_info = await admin_client[self.db_name].command("usersInfo")
            users = [user["user"] for user in users_info.get("users", [])]
            if self.db_user not in users:
                create_user = {
                    "createUser": self.db_user,
                    "pwd": self.db_password,
                    "roles": [{"role": role, "db": self.db_name} for role in self.db_roles],
                    "comment": "Created by MongoDBClient",
                }
                ans = await admin_db.command(create_user)
                logger.info(
                    f"{DATABASE_ICON} {logger.name} "
                    f"Created user {self.db_user} with "
                    f"roles {self.db_roles} in {self.db_name}",
                    extra={
                        "user": self.db_user,
                        "roles": self.db_roles,
                        "db_name": self.db_name,
                        "ans": ans,
                    },
                )
        except OperationFailure as e:
            # If the user already exists, ignore the error
            if e.code not in [11000, 51003]:
                create_user = {} if not create_user else create_user
                logger.error(
                    f"{DATABASE_ICON} {logger.name} Failed to create user {self.db_user}: {e}",
                    extra={"error": str(e), "create_user": create_user},
                )
                raise DBConnConnectionException("Failed to create database user.") from e
            pass
        except ServerSelectionTimeoutError as e:
            logger.error(
                "Database server selection timed out. Can't proceed", extra={"error": str(e)}
            )
            raise DBConnConnectionException("Database server selection timed out.") from e

        except Exception as e:
            create_user = {} if not create_user else create_user
            logger.error(
                f"{DATABASE_ICON} {logger.name} Failed to create user {self.db_user}: {e}",
                extra={"error": e, "create_user": create_user},
            )
            pass

    async def setup_collections_indexes(
        self, admin_client: AsyncMongoClient[Dict[str, Any]]
    ) -> None:
        """
        Set up the collections in the database.

        This method is intended to be called after the user has been set up.
        It can be used to create any necessary collections and indexes.
        """
        """
        Asynchronously checks and creates indexes for the collections in the database.

        This method iterates through the collections defined in the database
        configuration and creates the specified indexes if they do not already exist.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
        """
        dbs = InternalConfig().config.dbs_config.dbs
        db = admin_client[self.db_name]
        if (
            dbs is None
            or dbs[self.db_name].collections is None
            or not dbs[self.db_name].collections
            or not dbs[self.db_name].collections.items()
        ):
            return
        for collection_name, config in dbs[self.db_name].collections.items():
            cursor = await db[collection_name].list_indexes()
            list_indexes = await cursor.to_list(length=None)
            if config and isinstance(config, CollectionConfig) and config.indexes:
                for index_name, index_value in config.indexes.items():
                    if not self._check_index_exists(list_indexes, index_name):
                        try:
                            await db[collection_name].create_index(
                                index_value.index_key,
                                unique=index_value.unique,
                                name=index_name,
                            )
                            logger.info(
                                f"{DATABASE_ICON} {logger.name} "
                                f"Created index {index_name} in {collection_name}"
                            )
                        except Exception as ex:
                            logger.error(ex)

    def _check_index_exists(self, indexes, index_name):
        for index in indexes:
            if index.get("name") == index_name:
                return True
        return False

    async def _create_timeseries(self, admin_client: AsyncMongoClient[Dict[str, Any]]) -> None:
        """
        Asynchronously creates a time series collection in the database.

        This method checks if the database client is connected and if the
        specified collection exists. If it does not exist, it creates a
        time series collection with the specified configuration.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
            OperationFailure: If there is an error creating the collection.
        """
        dbs = InternalConfig().config.dbs_config.dbs
        db = admin_client[self.db_name]
        if (
            dbs is None
            or dbs[self.db_name].timeseries is None
            or not dbs[self.db_name].timeseries
            or not dbs[self.db_name].timeseries.items()
        ):
            return

        for timeseries_name, config in dbs[self.db_name].timeseries.items():
            try:
                await db.create_collection(
                    timeseries_name,
                    timeseries=config.model_dump(exclude_unset=True, exclude_none=True),
                )
                logger.info(
                    f"{DATABASE_ICON} {logger.name} Created time series collection {timeseries_name}"
                )
            except CollectionInvalid:
                logger.debug(
                    f"{DATABASE_ICON} {logger.name} "
                    f"Collection {timeseries_name} already exists. "
                    f"Skipping creation."
                )
            except Exception as ex:
                message = f"{DATABASE_ICON} {logger.name} Failed to create time series collection {timeseries_name} {ex}"
                logger.error(message, extra={"notification": False})

    # MARK: Database setup methods Sync

    def test_connection_sync(self, timeout_seconds: float = 10, admin: bool = False) -> None:
        """
        Test the database connection by pinging the database.

        This method attempts to connect to the database and execute a ping command
        to verify that the connection is successful.

        Parameters:
            timeout_seconds (float): The timeout in seconds for the connection attempt.
            admin (bool): If True, use the admin URI for the connection.

        Raises:
            ConnectionError: If the connection to the database fails.
        """
        uri = self.admin_uri if admin else self.uri
        client: MongoClient[Dict[str, Any]] = MongoClient(uri, tz_aware=True)
        try:
            with timeout(timeout_seconds):
                with client:
                    db = client[self.db_name]
                    ans = db.command("ping")
                    assert ans.get("ok", None)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to the database: {e}") from e

    # MARK: Database setup methods
    def setup_database_sync(self) -> None:
        """
        Set up the database connection and perform initial setup tasks.

        This method establishes a connection to the database, sets up the user,
        and prepares the collections and indexes as defined in the configuration.
        """
        InternalConfig.db_uri = self.uri
        if not self._setup:
            self._setup = True
            admin_client: MongoClient[Dict[str, Any]] = MongoClient(self.admin_uri, tz_aware=True)
            with admin_client:
                self.setup_user_sync(admin_client=admin_client)
                self.setup_collections_indexes_sync(admin_client=admin_client)
                self._create_timeseries_sync(admin_client=admin_client)
            logger.info(
                f"{DATABASE_ICON} {logger.name} "
                f"Database {self.db_name} is set up with user {self.db_user}"
            )
            if InternalConfig.db_client_sync:
                InternalConfig.db_client_sync.close()
            InternalConfig.db_client_sync = MongoClient(self.uri, tz_aware=True)
            InternalConfig.db_sync = InternalConfig.db_client_sync[self.db_name]
            logger.info(
                f"{DATABASE_ICON} Database {self.db_name} client is set up for InternalConfig"
            )
        logger.info(f"{DATABASE_ICON} {logger.name} Database {self.db_name} is already set up.")

    def setup_user_sync(self, admin_client: MongoClient[Dict[str, Any]]) -> None:
        """
        Set up the user.

        This method is intended to be called after the connection URI has been
        established. It can be used to perform any necessary setup tasks.
        """
        create_user = {}
        try:
            admin_db = admin_client[self.db_name]
            admin_db["startup_collection"].insert_one(
                {"startup": "complete", "timestamp": datetime.now(tz=timezone.utc)}
            )
            users_info = admin_client[self.db_name].command("usersInfo")
            users = [user["user"] for user in users_info.get("users", [])]
            if self.db_user not in users:
                create_user = {
                    "createUser": self.db_user,
                    "pwd": self.db_password,
                    "roles": [{"role": role, "db": self.db_name} for role in self.db_roles],
                    "comment": "Created by MongoDBClient",
                }
                ans = admin_db.command(create_user)
                logger.info(
                    f"{DATABASE_ICON} {logger.name} "
                    f"Created user {self.db_user} with "
                    f"roles {self.db_roles} in {self.db_name}",
                    extra={
                        "user": self.db_user,
                        "roles": self.db_roles,
                        "db_name": self.db_name,
                        "ans": ans,
                    },
                )
        except OperationFailure as e:
            # If the user already exists, ignore the error
            if e.code not in [11000, 51003]:
                create_user = {} if not create_user else create_user
                logger.error(
                    f"{DATABASE_ICON} {logger.name} Failed to create user {self.db_user}: {e}",
                    extra={"error": str(e), "create_user": create_user},
                )
                raise e
            pass
        except Exception as e:
            create_user = {} if not create_user else create_user
            logger.error(
                f"{DATABASE_ICON} {logger.name} Failed to create user {self.db_user}: {e}",
                extra={"error": e, "create_user": create_user},
            )
            pass

    def setup_collections_indexes_sync(self, admin_client: MongoClient[Dict[str, Any]]) -> None:
        """
        Set up the collections in the database.

        This method is intended to be called after the user has been set up.
        It can be used to create any necessary collections and indexes.
        """
        """
        Checks and creates indexes for the collections in the database.

        This method iterates through the collections defined in the database
        configuration and creates the specified indexes if they do not already exist.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
        """
        dbs = InternalConfig().config.dbs_config.dbs
        db = admin_client[self.db_name]
        if (
            dbs is None
            or dbs[self.db_name].collections is None
            or not dbs[self.db_name].collections
            or not dbs[self.db_name].collections.items()
        ):
            return
        for collection_name, config in dbs[self.db_name].collections.items():
            list_indexes = list(db[collection_name].list_indexes())
            if config and isinstance(config, CollectionConfig) and config.indexes:
                for index_name, index_value in config.indexes.items():
                    if not self._check_index_exists(list_indexes, index_name):
                        try:
                            db[collection_name].create_index(
                                index_value.index_key,
                                unique=index_value.unique,
                                name=index_name,
                            )
                            logger.info(
                                f"{DATABASE_ICON} {logger.name} "
                                f"Created index {index_name} in {collection_name}"
                            )
                        except Exception as ex:
                            logger.error(ex)

    def _create_timeseries_sync(self, admin_client: MongoClient[Dict[str, Any]]) -> None:
        """
        Creates a time series collection in the database.

        This method checks if the database client is connected and if the
        specified collection exists. If it does not exist, it creates a
        time series collection with the specified configuration.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
            OperationFailure: If there is an error creating the collection.
        """
        dbs = InternalConfig().config.dbs_config.dbs
        db = admin_client[self.db_name]
        if (
            dbs is None
            or dbs[self.db_name].timeseries is None
            or not dbs[self.db_name].timeseries
            or not dbs[self.db_name].timeseries.items()
        ):
            return

        for timeseries_name, config in dbs[self.db_name].timeseries.items():
            try:
                db.create_collection(
                    timeseries_name,
                    timeseries=config.model_dump(exclude_unset=True, exclude_none=True),
                )
                logger.info(
                    f"{DATABASE_ICON} {logger.name} Created time series collection {timeseries_name}"
                )
            except CollectionInvalid:
                logger.debug(
                    f"{DATABASE_ICON} {logger.name} "
                    f"Collection {timeseries_name} already exists. "
                    f"Skipping creation."
                )
            except Exception as ex:
                message = f"{DATABASE_ICON} {logger.name} Failed to create time series collection {timeseries_name} {ex}"
                logger.error(message, extra={"notification": False})
                pass


# # End of DBConn class
