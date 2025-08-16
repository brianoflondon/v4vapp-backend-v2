import asyncio
import re
from asyncio import get_event_loop
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, Dict, List

from pydantic import BaseModel, Field
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import (
    ConnectionFailure,
    DuplicateKeyError,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
)
from pymongo.results import UpdateResult

from v4vapp_backend_v2.config.setup import DB_RATES_COLLECTION, InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, HiveRatesDB, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd

ICON = "🔄"


class ReplyModel(BaseModel):
    """
    Base model for operations that can have replies.
    This model is used to track the reply ID, type, and any errors associated with the reply.
    """

    reply_id: str | None = Field("", description="Reply to the operation, if any", exclude=False)
    reply_type: str | None = Field(
        None,
        description="Transaction type of the reply, i.e. 'transfer' 'invoice' 'payment'",
        exclude=False,
    )
    reply_msat: int = Field(
        0,
        description="Msats amount of the reply, if any",
        exclude=False,
    )
    reply_message: str | None = Field(
        None,
        description="Message associated with the reply, if any",
        exclude=False,
    )
    reply_error: str | None = Field(None, description="Error in the reply, if any", exclude=False)

    def __init__(self, **data):
        """
        Initialize the ReplyModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)
        # TODO: We could automatically determine the reply_type based on the reply_id
        self.reply_id = data.get("reply_id", "")
        self.reply_type = data.get("reply_type", None)
        self.reply_msat = data.get("reply_msat", 0)
        self.reply_error = data.get("reply_error", None)
        self.reply_message = data.get("reply_message", None)

    # model_config = ConfigDict(use_enum_values=True)


class TrackedBaseModel(BaseModel):
    replies: List[ReplyModel] = Field(
        default_factory=list,
        description="List of replies to the operation",
        exclude=False,
    )

    conv: CryptoConv = Field(CryptoConv(), description="Conversion object for the transaction")
    fee_conv: CryptoConv | None = Field(
        CryptoConv(),
        description="Conversion object for fees associated with this transaction if any",
    )
    change_amount: AmountPyd | None = Field(
        None,
        description="Amount of change associated with this transaction if any",
    )
    change_conv: CryptoConv | None = Field(
        CryptoConv(),
        description="Conversion object for any returned change associated with this transaction if any",
    )
    change_memo: str | None = Field(
        None,
        description="Message associated with any change in this transaction if any",
    )
    process_time: float = Field(0, description="Time in (s) it took to process this transaction")
    last_quote: ClassVar[QuoteResponse] = QuoteResponse()

    def __init__(self, **data: Dict[str, Any]) -> None:
        """
        Initialize the TrackedBaseModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)

    @classmethod
    def short_id_query(cls, short_id: str) -> Dict[str, Any]:
        """
        Returns a query to find a document by its short_id.

        :param short_id: The short ID to search for.
        :return: A dictionary representing the query.
        """
        return {"group_id": {"$regex": f"^{re.escape(short_id)}$"}}

    # MARK: Reply Management

    def reply_ids(self) -> List[str]:
        """
        Returns a list of reply IDs from the replies.

        :return: A list of reply IDs.
        """
        return [reply.reply_id for reply in self.replies if reply.reply_id]

    def add_reply(
        self,
        reply_id: str,
        reply_type: str,
        reply_msat: int = 0,
        reply_message: str = "",
        reply_error: Any = None,
    ) -> None:
        """
        Adds a reply to the list of replies.

        :param reply_id: The ID of the reply.
        :param reply_type: The type of the reply (optional).
        :param reply_error: Any error associated with the reply (optional).
        """
        if reply_id and reply_id in self.reply_ids():
            logger.warning(
                f"{ICON} Reply with ID {reply_id} already exists in {self.name()}",
                extra={"notification": False},
            )
            raise ValueError(f"Reply with ID {reply_id} already exists")
        reply = ReplyModel(
            reply_id=reply_id,
            reply_type=reply_type,
            reply_msat=reply_msat,
            reply_message=reply_message,
            reply_error=reply_error,
        )
        self.replies.append(reply)

    def get_reply(self, reply_id: str) -> ReplyModel | None:
        """
        Retrieves a reply by its ID.

        :param reply_id: The ID of the reply to retrieve.
        :return: The ReplyModel instance if found, otherwise None.
        """
        for reply in self.replies:
            if reply.reply_id == reply_id:
                return reply
        return None

    def get_errors(self) -> list[ReplyModel]:
        """
        Retrieves all replies that have an error.

        :return: A list of ReplyModel instances that have an error.
        """
        return [reply for reply in self.replies if reply.reply_error is not None]

    def get_replies_by_type(self, reply_type: str) -> list[ReplyModel]:
        """
        Retrieves all replies of a specific type.

        :param reply_type: The type of replies to retrieve.
        :return: A list of ReplyModel instances matching the specified type.
        """
        return [reply for reply in self.replies if reply.reply_type == reply_type]

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.

        This method converts the class name to a snake_case string
        representation, which is typically used for naming operations
        or identifiers in a consistent and readable format.

        Returns:
            str: The snake_case representation of the class name.
        """
        return snake_case(cls.__name__)

    @property
    def collection_name(self) -> str:
        """
        Returns the name of the collection associated with this model.

        This method should be overridden in subclasses to provide the
        specific collection name.

        Returns:
            str: The name of the collection.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Returns the collection associated with this model.

        This method should be overridden in subclasses to provide the
        specific collection.

        Returns:
            AsyncCollection: The collection associated with this model.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @property
    def group_id_query(self) -> dict[str, Any]:
        """
        Returns the query used to identify the group ID in the database.

        This method should be overridden in subclasses to provide the
        specific query for the group ID.

        Returns:
            dict: The query used to identify the group ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @property
    def group_id_p(self) -> str:
        """
        Returns the group ID as a string.

        Note: @computed_field doesn't not work properly with mypy so the _p @property is used
        to fix this.

        This method should be overridden in subclasses to provide the
        specific query for the group ID.

        Returns:
            dict: The query used to identify the group ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    async def save(
        self,
        exclude_unset: bool = True,
        exclude_none: bool = True,
        mongo_kwargs: dict[str, Any] = {},
        **kwargs: Any,
    ) -> UpdateResult:
        """
        Asynchronously saves the current state of the model to the MongoDB database.

        This method serializes the model's data and performs an update operation on the
        corresponding MongoDB collection. It handles connection errors with automatic
        retries and logs relevant events. Subclasses may override this method to provide
        custom saving logic.

        Args:
            exclude_unset (bool, optional): If True, fields that were not explicitly set will be excluded from the update. Defaults to False.
            exclude_none (bool, optional): If True, fields with value None will be excluded from the update. Defaults to True.
            mongo_kwargs (dict[str, Any], optional): Additional keyword arguments for the MongoDB update operation. Defaults to {}.
            **kwargs (Any): Additional keyword arguments passed to the model serialization.

            UpdateResult: The result of the MongoDB update operation.

        Raises:
            DuplicateKeyError: If a duplicate key error occurs during the update.
            ServerSelectionTimeoutError: If the MongoDB server cannot be reached.
            NetworkTimeout: If a network timeout occurs.
            ConnectionFailure: If the connection to MongoDB fails.
            Exception: For any other exceptions encountered during the save operation.
        """
        if not mongo_kwargs:
            mongo_kwargs = {"upsert": True}
        update = self.model_dump(
            exclude_unset=exclude_unset, exclude_none=exclude_none, by_alias=True, **kwargs
        )
        if update.get("replies") == []:
            update.pop("replies", None)  # Remove empty replies list if it exists
        update = {
            "$set": update,
        }
        error_count = 0
        while True:
            try:
                db_ans = await InternalConfig.db[self.collection_name].update_one(
                    filter=self.group_id_query, update=update, **mongo_kwargs
                )
                if error_count > 0:
                    logger.info(
                        f"{ICON} Reconnected to MongoDB after {error_count} errors",
                        extra={"notification": True, "error_code_clear": "mongodb_save_error"},
                    )
                    logger.info(
                        f"{ICON} SAVED {self.group_id_p} to {self.collection_name}",
                    )
                    error_count = 0

                return db_ans

            except DuplicateKeyError:
                raise
            except (
                ServerSelectionTimeoutError,
                NetworkTimeout,
                ConnectionFailure,
                OperationFailure,
            ) as e:
                error_count += 1
                logger.error(
                    f"{ICON} Error {error_count} MongoDB connection error, while trying to save: {e}",
                    extra={"error_code": "mongodb_save_error", "notification": True},
                )
                # Wait before attempting to reconnect
                await asyncio.sleep(min(0.5 * error_count, 10))
                logger.info(f"{ICON} Attempting to reconnect to MongoDb")

            except Exception as e:
                logger.error(f"{ICON} Error occurred while saving to MongoDB: {e}")
                raise

    def tracked_type(self) -> str:
        """
        Returns the tracked type of the operation.

        This method should be overridden in subclasses to provide the
        specific tracked type.

        Returns:
            str: The tracked type of the operation.
        """
        return self.name()

    # MARK: Quote Management

    @classmethod
    def update_quote_sync(
        cls, quote: QuoteResponse | None = None, use_cache: bool = True, store_db: bool = True
    ) -> None:
        """
        Synchronously updates the last quote for the class.

        Args:
            quote (QuoteResponse | None): The quote to update.

        Returns:
            None
        """
        if quote:
            cls.last_quote = quote
            return

        try:
            loop = get_event_loop()
            if loop.is_running():
                # If the event loop is already running, schedule the coroutine
                raise RuntimeError(
                    "update_quote_sync cannot be called in an async context. Use update_quote instead."
                )
            else:
                loop.run_until_complete(cls.update_quote(use_cache=use_cache, store_db=store_db))
        except RuntimeError as e:
            # Handle cases where the event loop is already running
            # logger.error(f"Error in update_quote_sync: {e}")
            raise e

    @classmethod
    async def update_quote(
        cls, quote: QuoteResponse | None = None, use_cache: bool = True, store_db: bool = True
    ) -> None:
        """
        Asynchronously updates the last quote for the class.

        If a quote is provided, it sets the last quote to the provided quote.
        If no quote is provided, it fetches all quotes and sets the last quote
        to the fetched quote.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, fetches all quotes.

        Returns:
            None
        """
        if quote:
            cls.last_quote = quote
        else:
            all_quotes = AllQuotes()
            await all_quotes.get_all_quotes(use_cache=use_cache, store_db=store_db)
            cls.last_quote = all_quotes.quote

    async def update_quote_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Asynchronously updates the last quote for the class.

        If a quote is provided, it sets the last quote to the provided quote.
        If no quote is provided, it fetches all quotes and sets the last quote
        to the fetched quote.
        Uses the new quote to update a `conv` object.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, fetches all quotes.

        Returns:
            None
        """
        await TrackedBaseModel.update_quote(quote)
        await self.update_conv()

    async def update_conv(self, quote: QuoteResponse | None = None) -> None:
        """
        Updates the conversion for the transaction.

        If the subclass has a `conv` object, update it with the latest quote.
        If a quote is provided, it sets the conversion to the provided quote.
        If no quote is provided, it uses the last quote to set the conversion.

        Args:
            quote (QuoteResponse | None): The quote to update.
                If None, uses the last quote.
        """
        raise NotImplementedError("Subclasses must implement the update_conv method.")

    @classmethod
    async def nearest_quote(
        cls,
        timestamp: datetime,
    ) -> QuoteResponse:
        """
        Asynchronously finds the nearest quote in the database with a timestamp less than or equal to the provided timestamp.

        Args:
            timestamp (datetime): The reference datetime to find the nearest quote before or at this time.

        Returns:
            None

        Raises:
            ValueError: If the provided timestamp is not a datetime object.

        Side Effects:
            - Updates self.fetch_date with the timestamp of the found quote.
            - Updates self.quotes["HiveRatesDB"] with a QuoteResponse object containing the quote data.
            - Logs information about the found quote or warnings if an error occurs.
        """
        if not isinstance(timestamp, datetime):
            raise ValueError("timestamp must be a datetime object")

        if datetime.now(tz=timezone.utc) - timestamp < timedelta(seconds=600):
            await cls.update_quote()
            return cls.last_quote

        try:
            # Find the nearest quote by timestamp
            collection = InternalConfig.db[DB_RATES_COLLECTION]
            cursor = await collection.aggregate(
                [
                    {"$match": {"timestamp": {"$exists": True}}},
                    {
                        "$project": {
                            "originalDoc": "$$ROOT",
                            "time_diff_ms": {"$abs": {"$subtract": ["$timestamp", timestamp]}},
                        }
                    },
                    {"$sort": {"time_diff_ms": 1}},
                    {"$limit": 1},
                    {"$replaceRoot": {"newRoot": "$originalDoc"}},
                ]
            )
            nearest_quote = await cursor.to_list(length=1)

            if nearest_quote:
                quote = HiveRatesDB.model_validate(nearest_quote[0])
                quote_response = QuoteResponse(
                    hive_usd=quote.hive_usd,
                    hbd_usd=quote.hbd_usd,  # Assuming sats_hbd is used for hbd_us
                    btc_usd=quote.btc_usd,
                    hive_hbd=quote.hive_hbd,
                    raw_response={},
                    source="HiveRatesDB",
                    fetch_date=quote.timestamp,
                    error="",  # No error in this case
                    error_details={},
                )
                logger.info(
                    f"Found nearest quote delta from {timestamp}: {quote.timestamp - timestamp}",
                    extra={"notification": False, "quote": quote.model_dump()},
                )
                return quote_response
            else:
                logger.warning(
                    f"No quotes found for timestamp {timestamp}",
                    extra={"notification": False},
                )
                return cls.last_quote

        except ServerSelectionTimeoutError as e:
            logger.error(
                f"Failed to connect to the database: {e}",
                extra={"notification": False},
            )
            return cls.last_quote

        except Exception as e:
            logger.warning(f"Failed to find nearest quote: {e}", extra={"notification": False})
        return cls.last_quote
