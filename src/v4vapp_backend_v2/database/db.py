from datetime import datetime, timezone
from enum import Enum, StrEnum, auto
import json
import posixpath
import tempfile

from bson import ObjectId

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.events.async_event import async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

from v4vapp_backend_v2.config.setup import logger, InternalConfig
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import ConnectionFailure, OperationFailure
from pymongo.results import UpdateResult, DeleteResult
import logging

logger = logging.getLogger(__name__)


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


class MongoDBClient:
    def __init__(
        self,
        db_conn: str,
        db_name: str = "admin",
        db_user: str = "admin",
        uri: str = None,
        **kwargs,
    ) -> None:
        """
        Initializes the database connection and configuration.

        Args:
            db_conn (str): The database connection string.
            db_name (str, optional): The name of the database. Defaults to "admin".
            db_user (str, optional): The database user. Defaults to "admin".
            uri (str, optional): The URI for the database connection. Defaults to None.
            **kwargs: Additional keyword arguments.

        Returns:
            None
        """
        self.health_check: MongoDBStatus = MongoDBStatus.UNKNOWN
        self.db = None
        self.client = None
        self.error = None
        self.db_conn = db_conn
        # Sets up self.db_config here.
        self.validate_connection()
        self.hosts = ",".join(self.db_config.db_hosts) if db_conn else "localhost"
        self.db_name = db_name
        self.db_user = db_user
        self.validate_user_db()
        self.db_password = (
            self.db_config.dbs[self.db_name].db_users[self.db_user].password
        )
        self.db_roles = self.db_config.dbs[self.db_name].db_users[self.db_user].roles
        self.collections = self.db_config.dbs[db_name].collections
        self.uri = uri if uri else self._build_uri_from_config()
        self.health_check: MongoDBStatus = MongoDBStatus.VALIDATED
        self.kwargs = kwargs

    def validate_connection(self):
        try:
            config = InternalConfig().config
            self.db_config = config.database[self.db_conn]
        except KeyError as e:
            raise OperationFailure(
                error=f"Database Connection {self.db_conn} not found",
                code=DbErrorCode.NO_CONNECTION,
            )

    def validate_user_db(self):
        logger.info(f"Validating user {self.db_user} in database {self.db_name}")
        if not self.db_name in self.db_config.dbs:
            raise OperationFailure(
                error=f"User: {self.db_user} not in {self.db_name}",
                code=DbErrorCode.NO_DB,
            )
        if not self.db_user in self.db_config.dbs[self.db_name].db_users:
            raise OperationFailure(
                error=f"No database {self.db_name}",
                code=DbErrorCode.NO_USER,
            )
        if not bool(self.db_config.dbs[self.db_name].db_users[self.db_user].password):
            raise OperationFailure(
                error=f"No password for user {self.db_user} in {self.db_name}",
                code=DbErrorCode.NO_PASSWORD,
            )

    def __del__(self):
        if self.client:
            self.client.close()
            self.client = None
            self.health_check = MongoDBStatus.DISCONNECTED
            self.db = None
            logger.info("Disconnected from MongoDB")

    @property
    def admin_uri(self):
        return self._build_uri_from_config("admin", "admin")

    def _build_uri_from_config(self, db_name: str = None, db_user: str = None) -> str:
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
        db_password = self.db_config.dbs[db_name].db_users[db_user].password

        if self.db_config.db_replica_set:
            replica_set = f"&replicaSet={self.db_config.db_replica_set}"
        else:
            replica_set = ""
        auth_source = f"?authSource={db_name}"
        return (
            f"mongodb://{db_user}:{db_password}@{self.hosts}/"
            f"{auth_source}{replica_set}"
        )

    async def _check_create_db(self):
        """
        Asynchronously checks if the MongoDB database exists and creates it if it does not.

        This method performs the following steps:
        1. Checks if the MongoDB client is connected.
        2. Uses the admin client to check if the database exists.
        3. Creates the database with the user configuration.
        4. Creates a user with the specified roles in the database.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
            OperationFailure: If there is an error creating the user, except for specific error codes (11000, 51003).

        Logs:
            Info: When a user is successfully created with roles in the database.
            Error: When there is a failure to create the user, with details of the error and user creation command.

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
            logging.info(
                f"Created user {self.db_user} with roles {self.db_roles} in {self.db_name}",
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

        This method iterates through the collections defined in the database configuration
        and creates the specified indexes if they do not already exist.

        Raises:
            ConnectionFailure: If the MongoDB client is not connected.
        """
        if not self.client:
            raise ConnectionFailure("Not connected to MongoDB")
        if (
            self.db_config.dbs[self.db_name].collections is None
            or not self.db_config.dbs[self.db_name].collections
        ):
            return
        for collection_name, collection_config in self.db_config.dbs[
            self.db_name
        ].collections.items():
            if collection_config and collection_config.indexes:
                for index_name, index_value in collection_config.indexes.items():
                    try:
                        await self.db[collection_name].create_index(
                            index_value.index_key,
                            unique=index_value.unique,
                            name=index_name,
                        )
                    except Exception as ex:
                        print(ex)

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
        try:
            self.client = AsyncIOMotorClient(self.uri, tz_aware=True, **self.kwargs)
            self.admin_client = AsyncIOMotorClient(
                self.admin_uri, tz_aware=True, **self.kwargs
            )
            ans = await self.admin_client["admin"].command("ping")
            assert ans.get("ok") == 1
            self.db = self.client[self.db_name]
            database_names = await self.admin_client.list_database_names()
            database_users = await self.list_users()
            if self.db_name not in database_names or self.db_user not in database_users:
                await self._check_create_db()
                await self._check_indexes()
            logger.info(
                f"Connected to MongoDB {self.db}", extra={"client": self.client}
            )

        except (ConnectionFailure, OperationFailure, Exception) as e:
            logger.error(
                f"Failed to connect to MongoDB: {e}",
                extra={"uri": self.uri, "error": str(e)},
            )
            self.client = None
            self.db = None
            self.health_check = MongoDBStatus.ERROR
            raise e

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    async def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        if self.client is None or self.db is None:
            await self.connect()
        return self.db[collection_name]

    async def insert_one(self, collection_name: str, document: dict) -> ObjectId:
        collection = await self.get_collection(collection_name)
        result = await collection.insert_one(document)
        return result.inserted_id

    async def insert_many(
        self, collection_name: str, documents: list
    ) -> list[ObjectId]:
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

    async def find_one(self, collection_name: str, query: dict) -> dict:
        """
        Asynchronously find a single document in the specified collection that matches the given query.

        Args:
            collection_name (str): The name of the collection to search in.
            query (dict): The query criteria to match the document.

        Returns:
            dict: The document that matches the query, or None if no document is found.
        """
        collection = await self.get_collection(collection_name)
        document = await collection.find_one(query)
        return document

    async def update_one(
        self, collection_name: str, query: dict, update: dict, **kwargs
    ) -> UpdateResult:
        collection = await self.get_collection(collection_name)
        result = await collection.update_one(query, {"$set": update}, **kwargs)
        return result

    async def update_many(
        self, collection_name: str, query: dict, update: dict
    ) -> UpdateResult:
        collection = await self.get_collection(collection_name)
        result = await collection.update_many(query, {"$set": update}, upsert=True)
        return result

    async def delete_one(self, collection_name: str, query: dict) -> DeleteResult:
        collection = await self.get_collection(collection_name)
        result = await collection.delete_one(query)
        return result

    async def drop_user(self) -> dict:
        admin_db = self.admin_client[self.db_name]
        ans = await admin_db.command({"dropUser": self.db_user})
        return ans

    async def drop_database(self, db_name) -> dict:
        ans = await self.admin_client.drop_database(db_name)
        return ans

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class MyDBFlat:
    most_recent: LNDInvoice
    most_recent_settled: LNDInvoice

    def __init__(self):
        self.most_recent = LNDInvoice.model_construct()
        self.most_recent_settled = LNDInvoice.model_construct()
        async_subscribe(Events.LND_INVOICE, self.update_most_recent)

    async def update_most_recent(self, invoice: LNDInvoice):
        if invoice.settled:
            self.most_recent_settled = invoice
        else:
            self.most_recent = invoice


class MyDB:
    class LND:
        most_recent: LNDInvoice
        most_recent_settled: LNDInvoice

    def __init__(self):
        self._TEMP_FILE = posixpath.join(tempfile.gettempdir(), "database.json")
        self.LND.most_recent = LNDInvoice.model_construct()
        self.LND.most_recent_settled = LNDInvoice.model_construct()

        try:
            with open(self._TEMP_FILE, "r") as f:
                invoices_json = json.load(f)

                self.LND.most_recent = LNDInvoice.model_construct(
                    invoices_json["most_recent"]
                )
                self.LND.most_recent_settled = LNDInvoice.model_construct(
                    invoices_json["most_recent_settled"]
                )
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"File {self._TEMP_FILE} not found.")
            logger.warning(e, extra={"json": {"file": self._TEMP_FILE}})
            logger.warning(f"Most recent invoice: {self.LND.most_recent}")

    def update_most_recent(self, invoice: LNDInvoice):
        output = {}
        if invoice.settled:
            self.LND.most_recent_settled = invoice
            output = {
                "most_recent": self.LND.most_recent.model_dump(),
                "most_recent_settled": self.LND.most_recent_settled.model_dump(),
            }
        else:
            self.LND.most_recent = invoice
            output = {
                "most_recent": self.LND.most_recent.model_dump(),
                "most_recent_settled": self.LND.most_recent_settled.model_dump(),
            }

        with open(self._TEMP_FILE, "w") as f:
            json.dump(output, f, default=str)
            logger.debug(
                f"Updated most recent invoice: {invoice.add_index} {invoice.settled}",
                extra=output,
            )


# Create a temporary file
# db = MyDBFlat()

# subscribe(Events.LND_INVOICE_CREATED, db.update_most_recent)
