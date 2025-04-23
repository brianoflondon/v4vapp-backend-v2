import asyncio
from datetime import datetime, timezone
from enum import Enum, StrEnum
from timeit import default_timer as timer
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorCursor
from pymongo.errors import (
    ConnectionFailure,
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.results import DeleteResult, UpdateResult

from v4vapp_backend_v2.config.setup import InternalConfig, TimeseriesConfig, logger


class MongoDBStatus(StrEnum):
    UNKNOWN = "unknown"
    VALIDATED = "validated"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"


class DbErrorCode(Enum):
    NO_USER = 90001
    NO_DB = 90002
    NO_PASSWORD = 90003
    NO_CONNECTION = 90004
    BAD_URI = 90005


DATABASE_ICON = "📁"


# TODO: #56 Consider replacing this decorator with the tenacity module
def retry_on_failure(max_retries=5, initial_delay=1, backoff_factor=2):
    def decorator(func):
        """
        A decorator that retries a function upon encountering specific exceptions.

        This decorator retries the decorated asynchronous function when it raises
        either a `ConnectionFailure` or `OperationFailure` exception. The function
        will be retried up to `max_retries` times, with an initial delay of
        `initial_delay` seconds between attempts. The delay between retries will
        increase by a factor of `backoff_factor` after each attempt.

        Args:
            func (Callable): The asynchronous function to be decorated.

        Returns:
            Callable: The decorated function with retry logic.

        Raises:
            ConnectionFailure: If the maximum number of retries is reached.
            OperationFailure: If the maximum number of retries is reached.
        """

        async def wrapper(*args, **kwargs):
            error_code = None
            retries = 0
            delay = initial_delay
            while retries < max_retries:
                try:
                    ans = await func(*args, **kwargs)
                    if error_code:
                        logger.info(
                            f"Retry successful: {func.__name__}",
                            extra={"error_code_clear": error_code},
                        )
                    return ans
                except DuplicateKeyError as e:
                    extra = {
                        "error": str(e),
                        "retries": retries,
                    }
                    logger.debug(
                        f"DuplicateKeyError: {e}. Not retrying.",
                        extra=extra,
                    )
                    raise e
                except (ConnectionFailure, OperationFailure, InvalidOperation, Exception) as e:
                    retries += 1
                    error_code = "mongodb_error"
                    extra = {
                        "error": str(e),
                        "error_code": error_code,
                        "retries": retries,
                    }
                    if retries >= max_retries:
                        logger.error(
                            f"Failed to execute {func.__name__} after {retries} attempts: {e}",
                            extra=extra,
                        )
                        raise e
                    logger.warning(
                        f"Retrying {func.__name__} due to {e}. "
                        f"Attempt {retries}/{max_retries}. "
                        f"Retrying in {delay} s.",
                        extra=extra,
                    )
                    await asyncio.sleep(delay)
                    delay *= backoff_factor

        return wrapper

    return decorator


class MongoDBClient:
    def __init__(
        self,
        db_conn: str,
        db_name: str = "admin",
        db_user: str = "admin",
        uri: str | None = None,
        retry: bool = True,
        **kwargs,
    ) -> None:
        """
        Initializes the database connection and configuration.

        Args:
            db_conn (str): The database connection name from config.
            db_name (str, optional): The name of the database. Defaults to "admin".
            db_user (str, optional): The database user. Defaults to "admin".
            uri (str, optional): The URI for the database connection. Defaults to None.
            **kwargs: Additional keyword arguments.
            retry (bool, optional): Whether to retry the connection. Defaults to True.
        Returns:
            None
        """
        logger.info(f"{DATABASE_ICON} Initializing MongoDBClient {db_conn}")
        self.start_connection = timer()
        self.health_check: MongoDBStatus = MongoDBStatus.UNKNOWN
        self.first_health_check = MongoDBStatus.UNKNOWN
        self.db = None
        self.client = None
        self.error = None
        self.db_conn = db_conn
        # Sets up self.db_config here.
        self.db_name = db_name
        self.db_user = db_user
        self.validate_connection()
        self.hosts = ",".join(self.db_connection.hosts) if db_conn else "localhost"
        self.validate_user_db()
        self.db_password = self.dbs[self.db_name].db_users[self.db_user].password
        self.db_roles = self.dbs[self.db_name].db_users[self.db_user].roles
        self.collections = self.dbs[self.db_name].collections
        self.uri = uri if uri else self._build_uri_from_config()
        self.retry = retry
        self.kwargs = kwargs
        self.health_check = MongoDBStatus.VALIDATED

    def validate_connection(self):
        try:
            self.config = InternalConfig().config
            self.db_connection = self.config.dbs_config.connections[self.db_conn]

            if self.db_name == "admin":
                self.dbs = self.db_connection.admin_dbs
            else:
                self.dbs = self.config.dbs_config.dbs
        except KeyError:
            raise OperationFailure(
                error=f"Database Connection {self.db_conn} not found",
                code=DbErrorCode.NO_CONNECTION,
            )
        except Exception as e:
            raise OperationFailure(
                error=f"Error in database connection {self.db_conn}: {e}",
                code=DbErrorCode.NO_CONNECTION,
            )

    def validate_user_db(self):
        elapsed_time = timer() - self.start_connection
        logger.debug(
            f"Validating user {self.db_user} in database {self.db_name} {elapsed_time:.3f}s"
        )
        if self.db_name not in self.dbs:
            raise OperationFailure(
                error=f"User: {self.db_user} not in {self.db_name}",
                code=DbErrorCode.NO_DB,
            )
        if self.db_user not in self.dbs[self.db_name].db_users:
            raise OperationFailure(
                error=f"No database {self.db_name}",
                code=DbErrorCode.NO_USER,
            )
        if (
            not bool(self.dbs[self.db_name].db_users[self.db_user].password)
            and not self.db_connection.replica_set == "rsPytest"
        ):
            raise OperationFailure(
                error=f"No password for user {self.db_user} in {self.db_name}",
                code=DbErrorCode.NO_PASSWORD,
            )

    @property
    def hex_id(self):
        return hex(id(self))

    def __del__(self):
        if self.client:
            self.client.close()
            self.client = None
            self.health_check = MongoDBStatus.DISCONNECTED
            self.db = None
            time_connected = timer() - self.start_connection
            logger.debug(
                f"{DATABASE_ICON} "
                f"Deleted MongoDB Object {self.db_name} after {time_connected:.3f} s "
                f"{self.hex_id}",
                extra={
                    "client": self.client,
                    "db_name": self.db_name,
                    "db_user": self.db_user,
                    "db": self.db,
                    "time_connected": time_connected,
                },
            )

    @property
    def admin_uri(self):
        return self._build_uri_from_config("admin", "admin")

    def _build_uri_from_config(self, db_name: str = "", db_user: str = "") -> str:
        """
        Constructs a MongoDB URI from the database configuration.

        This method builds a MongoDB connection URI using the database configuration
        provided in the `db_config` attribute. It supports both single-host and
        replica set configurations.

        Returns:
            str: The constructed MongoDB URI.
        """
        db_name = self.db_name if not db_name else db_name
        db_user = self.db_user if not db_user else db_user
        auth_source = f"?authSource={db_name}"
        if self.db_connection.replica_set == "rsPytest":
            db_password = ""
            db_user = ""
            auth_source = ""
        elif db_name == "admin":
            db_password = (
                ":" + self.db_connection.admin_dbs["admin"].db_users["admin"].password + "@"
            )
        else:
            db_password = ":" + self.db_password + "@"

        if self.db_connection.replica_set:
            replica_set = f"&replicaSet={self.db_connection.replica_set}"
        else:
            replica_set = ""
        return f"mongodb://{db_user}{db_password}{self.hosts}/{auth_source}{replica_set}"

    async def _check_create_db(self):
        """
        Asynchronously checks if the MongoDB database exists and creates
        it if it does not.

        This method performs the following steps:
        1. Checks if the MongoDB client is connected.
        2. Uses the admin client to check if the database exists.
        3. Creates the database with the user configuration.
        4. Creates a user with the specified roles in the database.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
            OperationFailure: If there is an error creating the user,
                except for specific error codes (11000, 51003).

        Logs:
            Info: When a user is successfully created with roles in the database.
            Error: When there is a failure to create the user,
                with details of the error and user creation command.

        Note:
            This method will change the user's password if the user already exists.
        """
        if not self.client:
            raise ConnectionFailure("Not connected to MongoDB")
        try:
            admin_db = self.admin_client[self.db_name]
            await admin_db["startup_collection"].insert_one(
                {"startup": "complete", "timestamp": datetime.now(tz=timezone.utc)}
            )
            create_user = {
                "createUser": self.db_user,
                "pwd": self.db_password,
                "roles": [{"role": role, "db": self.db_name} for role in self.db_roles],
                "comment": "Created by MongoDBClient",
            }
            ans = await admin_db.command(create_user)
            logger.info(
                f"{DATABASE_ICON} "
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
                    f"Failed to create user {self.db_user}: {e}",
                    extra={"error": str(e), "create_user": create_user},
                )
                raise e
            pass
        except Exception as e:
            create_user = {} if not create_user else create_user
            logger.error(
                f"Failed to create user {self.db_user}: {e}",
                extra={"error": e, "create_user": create_user},
            )
            pass

    async def _check_indexes(self):
        """
        Asynchronously checks and creates indexes for the collections in the database.

        This method iterates through the collections defined in the database
        configuration and creates the specified indexes if they do not already exist.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
        """
        if not self.client:
            raise ConnectionFailure("Not connected to MongoDB")
        if self.dbs[self.db_name].collections is None or not self.dbs[self.db_name].collections:
            return
        for collection_name, collection_config in self.dbs[self.db_name].collections.items():
            list_indexes = await self.db[collection_name].list_indexes().to_list(length=None)
            if collection_config and collection_config.indexes:
                for index_name, index_value in collection_config.indexes.items():
                    if not self._check_index_exists(list_indexes, index_name):
                        try:
                            await self.db[collection_name].create_index(
                                index_value.index_key,
                                unique=index_value.unique,
                                name=index_name,
                            )
                            logger.info(
                                f"{DATABASE_ICON} Created index {index_name} in {collection_name}"
                            )
                        except Exception as ex:
                            logger.error(ex)
            elif collection_config and isinstance(collection_config, TimeseriesConfig):
                try:
                    await self.db.create_collection(
                        collection_name,
                        timeseries=collection_config.model_dump(),
                    )
                    logger.info(
                        f"{DATABASE_ICON} Created time series collection {collection_name}"
                    )
                except Exception as ex:
                    logger.error(ex)

    def _check_index_exists(self, indexes, index_name):
        for index in indexes:
            if index.get("name") == index_name:
                return True
        return False

    async def list_users(self) -> list:
        """
        List all users in the current database.

        Returns:
            list: A list of user names.
        """
        if self.client is None or self.db is None:
            return []
        users_info = await self.admin_client[self.db_name].command("usersInfo")
        return [user["user"] for user in users_info["users"]]

    async def connect(self):
        error_code = ""
        count = 0
        while True:
            if "serverSelectionTimeoutMS" not in self.kwargs:
                self.kwargs["serverSelectionTimeoutMS"] = 10000
            if "socketTimeoutMS" not in self.kwargs:
                self.kwargs["socketTimeoutMS"] = 10000
            first_time_check_or_recheck = (
                self.first_health_check == MongoDBStatus.UNKNOWN
                and not self.client
                and self.health_check != MongoDBStatus.CONNECTED
            )
            try:
                count += 1
                self.client = AsyncIOMotorClient(
                    self.uri,
                    tz_aware=True,
                    **self.kwargs,
                )
                self.admin_client = AsyncIOMotorClient(
                    self.admin_uri,
                    tz_aware=True,
                    **self.kwargs,
                )
                if first_time_check_or_recheck:
                    logger.info(
                        f"{DATABASE_ICON} Attempting to connect to MongoDB for the first time or after failure."
                    )
                    ans = await self.admin_client["admin"].command("ping")
                    assert ans.get("ok") == 1
                    if self.first_health_check == MongoDBStatus.UNKNOWN:
                        self.first_health_check = MongoDBStatus.VALIDATED
                self.db = self.client[self.db_name]
                database_names = await self.admin_client.list_database_names()
                database_users = await self.list_users()
                if first_time_check_or_recheck:
                    if self.db_name not in database_names or self.db_user not in database_users:
                        await self._check_create_db()
                    await self._check_indexes()
                logger.debug(
                    f"{DATABASE_ICON} "
                    f"Connected to MongoDB {self.db_name} "
                    f"after {timer() - self.start_connection:.3f}s "
                    f"{self.hex_id} {count}",
                    extra={
                        "client": self.client,
                        "db_name": self.db_name,
                        "db_user": self.db_user,
                        "db": self.db,
                        "id_self": self.hex_id,
                    },
                )

                self.health_check = MongoDBStatus.CONNECTED
                if count > 1:
                    logger.warning(
                        f"Reconnected to MongoDB {self.db_name} after {count} attempts",
                        extra={
                            "uri": self.uri,
                            "count": count,
                            "error_code_clear": error_code,
                        },
                    )
                return

            except (
                ConnectionFailure,
                OperationFailure,
                ServerSelectionTimeoutError,
                Exception,
            ) as e:
                error_code = e.code if hasattr(e, "code") else type(e).__name__
                logger.error(
                    f"Attempt {count} Failed to connect to MongoDB: {e}",
                    extra={
                        "uri": self.uri,
                        "error_code": error_code,
                        "error_details": str(e),
                    },
                )
                self.client = None
                self.db = None
                self.health_check = MongoDBStatus.ERROR
                # Only retry if we have ever connected to stop retrys
                if self.first_health_check != MongoDBStatus.VALIDATED:
                    raise e
                # give me a sleep time which is 1 + count * 2 or 30
                if not self.retry or count > 20:
                    raise e
                sleep_time = min(1 + count * 2, 30)  # Exponential backoff
                await asyncio.sleep(sleep_time)

    async def disconnect(self):
        if self.client:
            time_connected = timer() - self.start_connection
            logger.debug(
                f"{DATABASE_ICON} "
                f"Disconnected MongoDB {self.db_name} after {time_connected:.3f}s "
                f"{self.hex_id}",
                extra={
                    "client": self.client,
                    "db_name": self.db_name,
                    "db_user": self.db_user,
                    "db": self.db,
                    "time_connected": time_connected,
                    "id_self": self.hex_id,
                },
            )
            self.health_check = MongoDBStatus.DISCONNECTED
            self.client.close()

    async def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        if self.client is None or self.db is None or self.health_check != MongoDBStatus.CONNECTED:
            await self.connect()
        if self.db is None:
            raise ConnectionFailure("Not connected to MongoDB")
        return self.db[collection_name]

    @retry_on_failure()
    async def insert_one(self, collection_name: str, document: dict) -> ObjectId:
        collection = await self.get_collection(collection_name)
        result = await collection.insert_one(document)
        return result.inserted_id

    @retry_on_failure()
    async def insert_many(self, collection_name: str, documents: list) -> list[ObjectId]:
        """
        Insert multiple documents into a specified collection.
            ordered=False is used to continue inserting documents even if one fails.
        Args:
            collection_name (str): The name of the collection to insert documents into.
            documents (list): A list of documents to be inserted.
        Returns:
            list[ObjectId]: A list of ObjectIds of the inserted documents.
        """
        collection = await self.get_collection(collection_name)
        result = await collection.insert_many(documents, ordered=False)
        return result.inserted_ids

    @retry_on_failure()
    async def find_one(self, collection_name: str, query: dict, **kwargs) -> Any | None:
        """
        Asynchronously find a single document in the specified collection that
        matches the given query.

        Args:
            collection_name (str): The name of the collection to search in.
            query (dict): The query criteria to match the document.

        Returns:
            dict: The document that matches the query, or None if no document is found.
        """
        collection = await self.get_collection(collection_name)
        document = await collection.find_one(query, **kwargs)
        return document

    @retry_on_failure()
    async def find(self, collection_name: str, query: dict, *args, **kwargs) -> AsyncIOMotorCursor:
        """
        Asynchronously find multiple documents in a specified collection
        based on a query.

        Args:
            collection_name (str): The name of the collection to search in.
            query (dict): The query dictionary to filter the documents.

        Returns:
            AsyncIOMotorCollection: A cursor to the documents that match the query.
        """
        collection = await self.get_collection(collection_name)
        cursor = collection.find(query, *args, **kwargs)
        return cursor

    @retry_on_failure()
    async def update_one(
        self, collection_name: str, query: dict, update: dict, **kwargs
    ) -> UpdateResult:
        collection = await self.get_collection(collection_name)
        result = await collection.update_one(query, {"$set": update}, **kwargs)
        return result

    @retry_on_failure()
    async def update_many(self, collection_name: str, query: dict, update: dict) -> UpdateResult:
        collection = await self.get_collection(collection_name)
        result = await collection.update_many(query, {"$set": update}, upsert=True)
        return result

    @retry_on_failure()
    async def delete_one(self, collection_name: str, query: dict) -> DeleteResult:
        collection = await self.get_collection(collection_name)
        result = await collection.delete_one(query)
        return result

    @retry_on_failure()
    async def drop_user(self) -> dict:
        admin_db = self.admin_client[self.db_name]
        ans = await admin_db.command({"dropUser": self.db_user})
        return ans

    @retry_on_failure()
    async def drop_database(self, db_name) -> dict:
        ans = await self.admin_client.drop_database(db_name)
        return ans

    async def __aenter__(self):
        self.start_connection = timer()
        if self.client is None or self.db is None or self.health_check != MongoDBStatus.CONNECTED:
            await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()
        self.start_connection = 0
