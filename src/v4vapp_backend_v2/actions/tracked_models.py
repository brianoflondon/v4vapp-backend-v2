from asyncio import get_event_loop
from datetime import datetime, timedelta, timezone
from typing import Any, ClassVar, List

from pydantic import BaseModel, ConfigDict, Field
from pymongo.results import UpdateResult

from v4vapp_backend_v2.config.setup import DB_RATES_COLLECTION, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, HiveRatesDB, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case


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
    reply_error: Any | None = Field(None, description="Error in the reply, if any", exclude=False)

    def __init__(self, **data):
        """
        Initialize the ReplyModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)
        # TODO: We could automatically determine the reply_type based on the reply_id
        self.reply_id = data.get("reply_id", "")
        self.reply_type = data.get("reply_type", None)
        self.reply_msat = data.get("reply_msat", None)
        self.reply_error = data.get("reply_error", None)
        self.reply_message = data.get("reply_message", None)

    model_config = ConfigDict(use_enum_values=True)


class TrackedBaseModel(BaseModel):
    locked: bool = Field(
        default=False,
        description="Flag to indicate if the operation is locked or being processed",
        exclude=False,
    )
    replies: List[ReplyModel] = Field(
        default_factory=list,
        description="List of replies to the operation",
        exclude=False,
    )

    conv: CryptoConv | None = CryptoConv()

    last_quote: ClassVar[QuoteResponse] = QuoteResponse()
    db_client: ClassVar[MongoDBClient | None] = None

    def __init__(self, **data):
        """
        Initialize the TrackedBaseModel with the provided data.

        :param data: The data to initialize the model with.
        """
        super().__init__(**data)
        self.locked = data.get("locked", False)

    async def __aenter__(self) -> "TrackedBaseModel":
        """
        Asynchronously acquires a lock and returns the current instance.

        This method is intended to be used as part of an asynchronous context manager
        protocol. Upon entering the context, it ensures that the necessary lock is
        acquired before proceeding.

        Returns:
            TrackedBaseModel: The current instance with the lock acquired.
        """
        await self.lock_op()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        """
        Asynchronous context manager exit method.
        This method is called when exiting the async context. It ensures that any necessary cleanup is performed,
        such as unlocking operations by calling `self.unlock_op()`. It receives exception information if an exception
        was raised within the context.
        Args:
            exc_type (type[BaseException] | None): The type of exception raised, if any.
            exc_val (BaseException | None): The exception instance raised, if any.
            exc_tb (Any): The traceback object associated with the exception, if any.
        Returns:
            None
        """
        await self.unlock_op()

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
        if reply_id in self.reply_ids():
            logger.warning(
                f"Reply with ID {reply_id} already exists in {self.name()}",
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
    def collection(self) -> str:
        """
        Returns the name of the collection associated with this model.

        This method should be overridden in subclasses to provide the
        specific collection name.

        Returns:
            str: The name of the collection.
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

        This method should be overridden in subclasses to provide the
        specific query for the group ID.

        Returns:
            dict: The query used to identify the group ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    async def lock_op(self) -> UpdateResult | None:
        """
        Locks the operation to prevent concurrent processing.

        This method sets the `locked` attribute to True, indicating that
        the operation is currently being processed and should not be
        modified or accessed by other threads or processes.

        Returns:
            None
        """
        self.locked = True
        if TrackedBaseModel.db_client:
            ans = await TrackedBaseModel.db_client.update_one(
                collection_name=self.collection,
                query=self.group_id_query,
                update={"locked": self.locked},
            )
            return ans
        return None

    async def unlock_op(self) -> UpdateResult | None:
        """
        Unlocks the operation to allow concurrent processing.

        This method sets the `locked` attribute to False, indicating that
        the operation is no longer being processed and can be modified
        or accessed by other threads or processes.

        Returns:
            None
        """
        self.locked = False
        if self.db_client:
            # Remember my update_one already has the $set
            ans = await self.db_client.update_one(
                collection_name=self.collection,
                query=self.group_id_query,
                update={"$unset": {"locked": ""}},
                upsert=True,
            )
            return ans
        return None

    async def save(
        self, exclude_unset: bool = True, exclude_none: bool = True, **kwargs: Any
    ) -> UpdateResult | None:
        """
        Saves the current state of the operation to the database.

        This method should be overridden in subclasses to provide the
        specific saving logic for the operation.

        Returns:
            UpdateResult | None: The result of the update operation, or None if no database client is available.
        """
        if self.db_client:
            return await self.db_client.update_one(
                collection_name=self.collection,
                query=self.group_id_query,
                update=self.model_dump(
                    exclude_unset=exclude_unset, exclude_none=exclude_none, by_alias=True, **kwargs
                ),
                upsert=True,
            )
        logger.warning(
            "No database client available for saving the operation",
            extra={"notification": False},
        )
        return None

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
    def update_quote_sync(cls, quote: QuoteResponse | None = None) -> None:
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
                loop.run_until_complete(cls.update_quote())
        except RuntimeError as e:
            # Handle cases where the event loop is already running
            # logger.error(f"Error in update_quote_sync: {e}")
            raise e

    @classmethod
    async def update_quote(cls, quote: QuoteResponse | None = None) -> None:
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
            if cls.db_client and AllQuotes.db_client is None:
                AllQuotes.db_client = cls.db_client
            all_quotes = AllQuotes()
            await all_quotes.get_all_quotes()
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
        if not cls.db_client:
            logger.warning(
                "No database client available for HiveRatesDB", extra={"notification": False}
            )
            return cls.last_quote

        if not isinstance(timestamp, datetime):
            raise ValueError("timestamp must be a datetime object")

        if datetime.now(tz=timezone.utc) - timestamp < timedelta(seconds=600):
            await cls.update_quote()
            return cls.last_quote

        async with cls.db_client as db_client:
            try:
                # Find the nearest quote by timestamp
                collection = await db_client.get_collection(DB_RATES_COLLECTION)
                cursor = collection.aggregate(
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
            except Exception as e:
                logger.warning(f"Failed to find nearest quote: {e}", extra={"notification": False})
        return cls.last_quote
