from enum import StrEnum
import json
import posixpath
import tempfile

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.events.async_event import async_subscribe
from v4vapp_backend_v2.events.event_models import Events
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

from v4vapp_backend_v2.config.setup import logger, InternalConfig
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import ConnectionFailure, OperationFailure
import logging

logger = logging.getLogger(__name__)


class MongoDBStatus(StrEnum):
    UNKNOWN = "unknown"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"


class MongoDBClient:
    """
    A client for connecting to and interacting with a MongoDB database using Motor, an async driver for MongoDB.

    Attributes:
        uri (str): The MongoDB URI.
        db_name (str): The name of the database to connect to.
        client (AsyncIOMotorClient): The Motor client instance.
        db (Database): The Motor database instance.

    Methods:
        __init__(uri: str = None, db_name: str = "admin"):
            Initializes the MongoDBClient with the given URI and database name.

        __del__():
            Closes the MongoDB client connection when the instance is deleted.

        _build_uri_from_config():
            Builds the MongoDB URI from the configuration.

        _check_db():
            Checks if the client is connected to the MongoDB database.

        async connect():
            Connects to the MongoDB database.

        async disconnect():
            Disconnects from the MongoDB database.

        async get_collection(collection_name: str):
            Retrieves a collection from the MongoDB database.

        async insert_one(collection_name: str, document: dict):
            Inserts a single document into a collection.

        async find_one(collection_name: str, query: dict):
            Finds a single document in a collection.

        async update_one(collection_name: str, query: dict, update: dict):
            Updates a single document in a collection.

        async delete_one(collection_name: str, query: dict):
            Deletes a single document from a collection.
    """

    def __init__(self, db_name: str = "admin", db_user: str = None) -> None:
        config = InternalConfig().config
        self.db_config = config.database
        self.db_name = db_name
        self.db_detail = self.db_config.get_db_detail(db_name, db_user)
        if not self.db_detail:
            self.db_detail = self.db_config.db_admin_detail
        self.db_user = self.db_detail.db_user if db_user is None else db_user
        self.uri = self._build_uri_from_config()
        self.client = None
        self.db = None
        self.health_check: MongoDBStatus = MongoDBStatus.UNKNOWN
        self.error = None

    @property
    def db_password(self) -> str:
        return (
            self.db_config.db_admin_password
            if self.db_user == self.db_config.db_admin_user
            else self.db_detail.db_password
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
        return self._build_uri_from_config(
            self.db_config.db_auth_source, self.db_config.db_admin_user
        )

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
        db_password = (
            self.db_config.db_admin_password
            if db_user == self.db_config.db_admin_user
            else self.db_detail.db_password
        )
        hosts = ",".join(self.db_config.db_hosts)
        if self.db_config.db_replica_set:
            replica_set = f"&replicaSet={self.db_config.db_replica_set}"
        else:
            replica_set = ""
        auth_source = f"?authSource={db_name}"
        return (
            f"mongodb://{db_user}:{db_password}@{hosts}/" f"{auth_source}{replica_set}"
        )

    async def _check_db(self):
        if not self.client:
            raise ConnectionFailure("Not connected to MongoDB")
        # Need an admin client to check if the database exists
        admin_client = AsyncIOMotorClient(self.admin_uri)
        database_names = await admin_client.list_database_names()
        admin_db = admin_client[self.db_config.db_auth_source]
        if self.db_name not in self.db_config.db_names:
            raise ConnectionFailure(
                f"Database Configuration for {self.db_name} not found"
            )
        # Create the database with the user configuration
        # Note: this will change the user's password if the user exists
        admin_client_db = admin_client[self.db_name]
        await admin_client_db["startup_collection"].insert_one({"startup": "complete"})
        create_user = {
            "createUser": self.db_user,
            "pwd": self.db_password,
            "roles": [
                {"role": role, "db": self.db_name} for role in self.db_detail.db_roles
            ],
            "comment": "Created by MongoDBClient",
        }
        try:
            users = await admin_db.command("usersInfo")
            if self.db_user in [user["user"] for user in users["users"]]:
                self.health_check = MongoDBStatus.CONNECTED
                return
            await admin_db.command(create_user)
            logger.info(
                f"Created database {self.db_name} "
                f"with user {self.db_user} "
                f"with roles {self.db_detail.db_roles}",
                extra=create_user,
            )
            self.health_check = MongoDBStatus.CONNECTED
        except OperationFailure as e:
            logger.warning(f"{e.details.get('errmsg')}")
            if e.code != 51003:
                self.health_check = MongoDBStatus.ERROR
                self.error = e
                raise e
            self.health_check = MongoDBStatus.CONNECTED
        except Exception as e:
            logger.error(
                f"Failed to create user {self.db_user} "
                f"with roles {self.db_detail.db_roles}: {e}",
                extra=create_user,
            )
            self.health_check = MongoDBStatus.ERROR
            self.error = e
            raise e

    async def connect(self):
        try:
            self.client = AsyncIOMotorClient(self.uri)
            # Test the connection
            if self.db_name != "admin":
                await self._check_db()
            await self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            logger.info(f"Connected to MongoDB {self.db}")

        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
            self.db = None
            raise e
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
            self.db = None
            raise e

    async def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    async def get_collection(self, collection_name: str) -> AsyncIOMotorCollection:
        if self.client is None or self.db is None:
            await self.connect()
        return self.db[collection_name]

    async def insert_one(self, collection_name: str, document: dict):
        collection = await self.get_collection(collection_name)
        result = await collection.insert_one(document)
        return result.inserted_id

    async def find_one(self, collection_name: str, query: dict):
        collection = await self.get_collection(collection_name)
        document = await collection.find_one(query)
        return document

    async def update_one(self, collection_name: str, query: dict, update: dict):
        collection = await self.get_collection(collection_name)
        result = await collection.update_one(query, {"$set": update})
        return result.modified_count

    async def delete_one(self, collection_name: str, query: dict):
        collection = await self.get_collection(collection_name)
        result = await collection.delete_one(query)
        return result.deleted_count

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
db = MyDBFlat()

# subscribe(Events.LND_INVOICE_CREATED, db.update_most_recent)
