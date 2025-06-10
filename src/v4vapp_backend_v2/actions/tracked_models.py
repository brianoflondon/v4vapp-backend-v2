import asyncio
from asyncio import get_event_loop
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import Any, ClassVar, Dict, List

from pydantic import BaseModel, ConfigDict, Field
from pymongo.results import UpdateResult

from v4vapp_backend_v2.config.setup import DB_RATES_COLLECTION, logger
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, HiveRatesDB, QuoteResponse
from v4vapp_backend_v2.helpers.general_purpose_funcs import snake_case
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


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
        self.reply_msat = data.get("reply_msat", 0)
        self.reply_error = data.get("reply_error", None)
        self.reply_message = data.get("reply_message", None)

    model_config = ConfigDict(use_enum_values=True)


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

    last_quote: ClassVar[QuoteResponse] = QuoteResponse()
    db_client: ClassVar[MongoDBClient | None] = None

    def __init__(self, **data):
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
        return {"group_id": {"$regex": f"^{short_id}"}}  # Match the full short_id

    async def __aenter__(self) -> "TrackedBaseModel":
        """
        Acquires an async lock and returns the current instance.

        This method is intended to be used as part of an async context manager
        protocol. Upon entering the context, it ensures that the necessary lock is
        acquired before proceeding, waiting up to 10 seconds for the lock.

        Returns:
            TrackedBaseModel: The current instance with the lock acquired.
        """
        # stack = inspect.stack()
        # print(stack[1].function, stack[1].filename, stack[1].lineno)
        logger.info(f"Locking   operation {self.name()} {self.group_id_p}")
        if await self.locked:
            logger.warning(
                f"Operation {self.name()} {self.group_id_p} is already locked, waiting for unlock",
                extra={"notification": False},
            )
            unlocked = await self.wait_for_lock(timeout=10)
            if not unlocked:
                logger.warning(
                    f"Timeout waiting for lock to be released for operation {self.name()} {self.group_id_p}",
                    extra={"notification": False},
                )
                await self.unlock_op()

                # raise TimeoutError("Timeout waiting for lock to be released.")
        await self.lock_op()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        """
        Async context manager exit method.
        This method is called when exiting the context. It ensures that any necessary cleanup is performed,
        such as unlocking operations by calling `self.unlock_op()`. It receives exception information if an exception
        was raised within the context.
        Args:
            exc_type (type[BaseException] | None): The type of exception raised, if any.
            exc_val (BaseException | None): The exception instance raised, if any.
            exc_tb (Any): The traceback object associated with the exception, if any.
        Returns:
            None
        """
        logger.info(f"Unlocking operation {self.name()} {self.group_id_p}")
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
        if reply_id and reply_id in self.reply_ids():
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

        Note: @computed_field doesn't not work properly with mypy so the _p @property is used
        to fix this.

        This method should be overridden in subclasses to provide the
        specific query for the group ID.

        Returns:
            dict: The query used to identify the group ID.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    # MARK: Lock Management

    async def wait_for_lock(self, timeout: int = 2) -> bool:
        """
        Waits for the operation to be unlocked within a specified timeout.

        This method checks if the operation is locked and waits until it is
        unlocked or the timeout is reached.

        Args:
            timeout (int): The maximum time to wait for the lock to be released, in seconds.

        Returns:
            bool: True if the operation was unlocked, False if the timeout was reached.
        """
        # TODO: #113 Make this much more efficient in the way it handles the db_client
        start_time = timer()
        while await self.locked:
            if (timer() - start_time) > timeout:
                return False
            await asyncio.sleep(0.5)
        return True

    @property
    async def locked(self) -> bool:
        """
        Returns the locked status of the operation.

        This method checks if the operation is currently locked, which
        indicates that it is being processed and should not be modified
        or accessed by other threads or processes.

        Returns:
            bool: True if the operation is locked, False otherwise.
        """
        if TrackedBaseModel.db_client:
            async with TrackedBaseModel.db_client as db_client:
                result: dict[str, Any] | None = await db_client.find_one(
                    collection_name=self.collection,
                    query=self.group_id_query,
                    projection={"locked": True},
                )
            if result:
                return result.get("locked", False)
        return False

    async def lock_op(self) -> UpdateResult | None:
        """
        Locks the operation to prevent concurrent processing.

        This method sets the `locked` attribute to True, indicating that
        the operation is currently being processed and should not be
        modified or accessed by other threads or processes.

        Returns:
            None
        """
        if TrackedBaseModel.db_client:
            async with TrackedBaseModel.db_client as db_client:
                ans: UpdateResult = await db_client.update_one(
                    collection_name=self.collection,
                    query=self.group_id_query,
                    update={"$set": {"locked": True}},
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
        if TrackedBaseModel.db_client:
            async with TrackedBaseModel.db_client as db_client:
                # Remember my update_one already has the $set
                ans: UpdateResult = await db_client.update_one(
                    collection_name=self.collection,
                    query=self.group_id_query,
                    update={"$unset": {"locked": ""}},
                    upsert=True,
                )
                return ans
        return None

    async def save(
        self, exclude_unset: bool = False, exclude_none: bool = True, **kwargs: Any
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
