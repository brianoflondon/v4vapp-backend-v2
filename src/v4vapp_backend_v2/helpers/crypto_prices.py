import asyncio
import pickle
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from timeit import default_timer as timer
from typing import Annotated, Any, ClassVar, Dict, List

import httpx
from binance.spot import Spot  # type: ignore
from pydantic import BaseModel, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.setup import InternalConfig, async_time_decorator, logger
from v4vapp_backend_v2.database.db_retry import (
    mongo_call,
    summarize_write_result,  # optional pretty log
)
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta
from v4vapp_backend_v2.hive.hive_extras import call_hive_internal_market

ALL_PRICES_COINGECKO = (
    "https://api.coingecko.com/api/v3/simple"
    "/price?ids=bitcoin,hive,"
    "hive_dollar&vs_currencies=btc,usd,eur,aud"
)
ALL_PRICES_COINMARKETCAP = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"

SATS_PER_BTC = 100_000_000  # 100 million Satoshis per Bitcoin

TESTING_CACHE_TIMES = {
    "CoinGecko": 360,
    "Binance": 360,
    "CoinMarketCap": 1800,
    "HiveInternalMarket": 360,
    "Global": 180,
}

CACHE_TIMES = {
    "CoinGecko": 180,
    "Binance": 120,
    "CoinMarketCap": 1800,
    "HiveInternalMarket": 60,
    "Global": 60,
}

DB_RATES_COLLECTION: str = "rates"  # Collection name for storing rates in the database

DB_RATES_MIN_INTERVAL: int = 60 * 2 - 10  # 2 minutes 50 seconds

ICON = "💱"


def _log_rates_insert_done(t: asyncio.Task) -> None:
    try:
        res = t.result()
        # summarize_write_result is optional; remove if not available
        try:
            msg = summarize_write_result(res)
        except Exception:
            msg = str(res)
        logger.debug(f"{ICON} Rates insert completed: {msg}")
    except Exception as e:
        logger.warning(f"{ICON} Rates insert task failed: {e}", extra={"notification": False})


def currency_to_receive(memo: str) -> Currency:
    """
    Detects the currency to receive based on the memo.
    Args:
        memo (str): The memo to check.
    Returns:
        Currency: The detected currency, defaults to HIVE if not found.
    """
    if not memo or "#sats" in memo.lower() or "#keepsats" in memo.lower():
        return Currency.SATS
    if "#hbd" in memo.lower():
        return Currency.HBD
    if "#hive" in memo.lower():
        return Currency.HIVE
    return Currency.HIVE  # Default to HIVE if no specific currency is detected


class CurrencyPair(StrEnum):
    HIVE_USD = "hive_usd"
    HBD_USD = "hbd_usd"
    BTC_USD = "btc_usd"
    HIVE_HBD = "hive_hbd"
    SATS_HIVE = "hive_sats"
    SATS_HBD = "hbd_sats"


class HiveRatesDB(BaseModel):
    """
    HiveRatesDB is a model that represents the database structure for storing
    cryptocurrency exchange rates.

    Attributes:
        timestamp (datetime): The timestamp when the rates were fetched.
        hive_usd (float): The exchange rate of HIVE to USD.
        btc_usd (float): The exchange rate of BTC to USD.
        sats_hive (float): The exchange rate of Satoshi to HIVE.
        sats_usd (float): The exchange rate of Satoshi to USD.
        sats_hbd (float): The exchange rate of Satoshi to HBD.
        hive_hbd (float): The exchange rate of HIVE to HBD.
    """

    timestamp: datetime
    hive_usd: float
    hbd_usd: float
    btc_usd: float
    hive_hbd: float
    sats_hive: float
    sats_usd: float
    sats_hbd: float


# Define the annotated type
RawResponseType = Annotated[Dict[str, Any] | List[Dict[str, Any]], "Raw response type"]


class QuoteResponse(BaseModel):
    """
    QuoteResponse is a model that represents the response of cryptocurrency quotes.

    Attributes:
        hive_usd (float): The price of HIVE in USD. Default is 0.
        hbd_usd (float): The price of HBD in USD. Default is 0.
        btc_usd (float): The price of BTC in USD. Default is 0.
        hive_hbd (float): The price of HIVE in HBD. Default is 0.
        raw_response (Dict[str, Any]): The raw response data. Default is an
            empty dictionary.
        fetch_date (datetime): The date and time when the data was fetched. Default is
            the current UTC time.
        error (str): Error message, if any. Default is an empty string.

    Methods:
        __init__: Initializes a new instance of QuoteResponse.
        sats_hive (float): Calculates Satoshis per HIVE based on btc_usd and hive_usd.
        sats_hbd (float): Calculates Satoshis per HBD based on btc_usd and hbd_usd.
        age (int): Calculates the age of the quote in seconds.
    """

    hive_usd: float = 0
    hbd_usd: float = 0
    btc_usd: float = 0
    hive_hbd: float = 0
    raw_response: RawResponseType = Field(
        default={},
        description="The raw response to queries for this quote as received from the source",
        exclude=True,
    )
    source: str = ""
    fetch_date: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc)
    error: str = ""
    error_details: Dict[str, Any] = {}

    def __init__(
        self,
        hive_usd: float = 0,
        hbd_usd: float = 0,
        btc_usd: float = 0,
        hive_hbd: float = 0,
        raw_response: RawResponseType = {},
        source: str = "",
        fetch_date: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc),
        error: str = "",
        error_details: Dict[str, Any] = {},
        **kwargs,
    ) -> None:
        super().__init__()
        self.hive_usd = round(hive_usd, 4)
        self.hbd_usd = round(hbd_usd, 4)
        self.hive_usd = round(hive_usd, 4)
        self.btc_usd = round(btc_usd, 2)
        self.hive_hbd = round(hive_hbd, 4)
        self.raw_response = raw_response
        self.source = source
        self.fetch_date = fetch_date
        self.error = error
        self.error_details = error_details

    @computed_field
    def sats_hive(self) -> float:
        """
        Calculate the number of Satoshis equivalent to one HIVE.
        Computed property so included in model_dump
        Returns:
            float: The number of Satoshis per HIVE, rounded to 4 decimal places.
        """
        if self.btc_usd == 0:
            # raise ValueError("btc_usd cannot be zero")
            return 0.0
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hive_usd, 4)

    @property
    def sats_hive_p(self) -> float:
        """
        Helper Property to help with type checking because `@computed_field`
        is not recognized by some tools.
        Returns:
            float: The value of sats_hive.
        """
        if self.btc_usd == 0:
            # raise ValueError("btc_usd cannot be zero")
            return 0.0
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hive_usd, 4)

    @computed_field
    def sats_hbd(self) -> float:
        """
        Calculate the number of Satoshis equivalent to one HBD (Hive Backed Dollar).
        Computed property so included in model_dump
        Returns:
            float: The number of Satoshis per HBD, rounded to 4 decimal places.
        """
        if self.btc_usd == 0:
            return 0.0
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hbd_usd, 4)

    @property
    def sats_hbd_p(self) -> float:
        """
        Helper Property to help with type checking because `@computed_field`
        is not recognized by some tools.
        Returns:
            float: The value of sats_hbd.
        """
        if self.btc_usd == 0:
            return 0.0
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hbd_usd, 4)

    @property
    def sats_usd(self) -> float:
        """
        Calculate Satoshis per USD based on btc_usd.
        Computed property so included in model_dump
        """
        if self.btc_usd == 0:
            return 0.0
        return round(SATS_PER_BTC / self.btc_usd, 4)

    @property
    def sats_usd_p(self) -> float:
        """
        Helper Property to help with type checking because `@computed_field`
        is not recognized by some tools. Added for consistency with other properties.
        Returns:
            float: The value of sats_usd.
        """
        return self.sats_usd

    @computed_field
    def age(self) -> float:
        """Calculate the age of the quote in seconds."""
        return (datetime.now(tz=timezone.utc) - self.fetch_date).total_seconds()

    @property
    def age_p(self) -> float:
        """Calculate the age of the quote in seconds."""
        return (datetime.now(tz=timezone.utc) - self.fetch_date).total_seconds()

    def get_age(self) -> float:
        """Calculate the age of the quote in seconds. Function version."""
        return (datetime.now(tz=timezone.utc) - self.fetch_date).total_seconds()

    @property
    def log_data(self) -> Dict[str, Any]:
        return self.model_dump(exclude={"raw_response"}, exclude_none=True)


class AllQuotes(BaseModel):
    """
    AllQuotes class is responsible for fetching and aggregating cryptocurrency quotes
    from various services.

    Attributes:
        quotes (Dict[str, QuoteResponse]): A dictionary to store quotes from different
        services.
        fetch_date (datetime): The date and time when the quotes were fetched.
        source (str): The source of the quotes.

        Class Var:

    Methods:
        get_all_quotes(use_cache: bool = True, timeout: float = 30.0):
            Asynchronously fetches quotes from multiple services and stores them in
            the quotes attribute. If the fetching exceeds the specified timeout, it
            logs an error and sets the quotes with timeout errors.

        quote() -> QuoteResponse:
            Retrieves the authoritative quote based on predefined rules. If a valid
            quote from Binance is available, it is returned. If HiveInternalMarket is
            also available, its hive_hbd attribute is included in the response. If no
            valid quote from Binance is found, an average quote is calculated and
            returned.

        calculate_average_quote() -> QuoteResponse:
            Calculates the average values of various cryptocurrency quotes from the
            valid quotes. Returns an object containing the average values for HIVE to
            USD, HBD to USD, BTC to USD, and HIVE to HBD.

        hive_hbd() -> float:
            Calculates the average HIVE to HBD value from the available quotes. If
            HiveInternalMarket is available, its hive_hbd value is returned. If no
            valid Hive HBD price is found, raises a ValueError.
    """

    quotes: Dict[str, QuoteResponse] = {}
    quote: QuoteResponse = QuoteResponse()
    fetch_date: datetime = datetime.now(tz=timezone.utc)
    source: str = ""
    redis_hit: bool = False

    fetch_date_class: ClassVar[datetime] = datetime.now(tz=timezone.utc)
    db_store_timestamp: ClassVar[datetime | None] = None

    def get_one_quote(self) -> QuoteResponse:
        """
        Get the authoritative quote based on rules.

        This method retrieves the authoritative quote from available sources based on
        predefined rules. If a valid quote from Binance is available, it is returned.
        If HiveInternalMarket is also available, its hive_hbd attribute is included in
        the response. If no valid quote from Binance is found, an average quote is
        calculated and returned.

        Returns:
            QuoteResponse: The authoritative quote or an error message if no valid
            quote is found.

        Side Effect:
            Updates the self.quote object
        """
        if self.quotes:
            if Binance.__name__ in self.quotes and not self.quotes[Binance.__name__].error:
                quote = self.quotes[Binance.__name__]
                # If HiveInternalMarket is available, use its hive_hbd value
                if (
                    HiveInternalMarket.__name__ in self.quotes
                    and not self.quotes[HiveInternalMarket.__name__].error
                    and self.quotes[HiveInternalMarket.__name__].hive_hbd > 0
                ):
                    quote_dict = quote.model_dump()
                    quote_dict["hive_hbd"] = self.quotes[HiveInternalMarket.__name__].hive_hbd
                    quote = QuoteResponse.model_validate(quote_dict)
                self.quote = quote
                self.source = Binance.__name__
                return quote
            else:
                # If Binance is not available, calculate average from other sources
                avg_quote = self.calculate_average_quote()
                if avg_quote:
                    self.quote = avg_quote
                    self.source = "Average"
                    return avg_quote

        return QuoteResponse(error="No valid quote found")

    async def get_binance_quote(self) -> QuoteResponse:
        """
        Get the quote from Binance. Special non async call for use in Transfer init

        Returns:
            QuoteResponse: The quote from Binance or an error message if the quote
            could not be fetched.
        """
        binance_quote = Binance().get_quote_sync()
        self.quotes["Binance"] = binance_quote
        self.fetch_date = binance_quote.fetch_date
        return binance_quote

    async def get_all_quotes(
        self, use_cache: bool = True, timeout: float = 60.0, store_db: bool = True
    ) -> None:
        """
        Asynchronously fetches cryptocurrency quotes from multiple services, with optional caching, timeout, and database storage.
        Args:
            use_cache (bool, optional): If True, attempts to use cached quotes before fetching new data. Defaults to True.
            timeout (float, optional): Maximum time in seconds to wait for all quote services to respond. Defaults to 60.0.
            store_db (bool, optional): If True, stores the fetched quotes in the database. Defaults to True.
        Returns:
            None
        Side Effects:
            - Updates self.quotes with the latest quote data or error responses.
            - Updates self.fetch_date with the current fetch timestamp.
            - Logs detailed information and errors during the fetching process.
            - Stores quotes in Redis cache and optionally in the database.
        Raises:
            asyncio.TimeoutError: If fetching quotes exceeds the specified timeout.
            Exception: If any quote service raises an exception during fetching.
        Note:
            Errors from individual services are logged and included in the quote responses.
        """
        start = timer()
        global_cache = await self.check_global_cache()
        logger.info(f"{ICON} Global rate check cache hit: {global_cache}, use_cache: {use_cache}")
        if use_cache and global_cache:
            logger.debug(
                f"{ICON} Quotes fetched from main cache in {timer() - start:.4f} seconds",
            )
            return
        all_services = [
            CoinGecko(),
            Binance(),
            CoinMarketCap(),
            HiveInternalMarket(),
        ]
        self.fetch_date = datetime.now(tz=timezone.utc)
        tasks: dict[str, asyncio.Task] = {}
        try:
            async with asyncio.timeout(timeout):
                logger.debug(f"{ICON} Fetching quotes with timeout of {timeout} seconds")
                async with asyncio.TaskGroup() as tg:
                    tasks = {
                        service.__class__.__name__: tg.create_task(service.get_quote(use_cache))
                        for service in all_services
                    }

        except asyncio.TimeoutError as e:
            self.quotes = {
                service.__class__.__name__: QuoteResponse(error=f"Timeout after {timeout} seconds")
                for service in all_services
            }
            logger.error(
                f"{ICON} Quote fetching exceeded timeout of {timeout} seconds",
                extra={"timeout": timeout, "error": e},
            )

        self.quotes = {}
        for service_name, task in tasks.items():
            try:
                self.quotes[service_name] = await task
            except Exception as e:
                logger.error(f"Error fetching quote from {service_name}: {e}")
                self.quotes[service_name] = QuoteResponse(error=str(e))

        logger.info(
            f"{ICON} Quotes fetched successfully in {timer() - start:.4f} seconds",
            extra={
                "quotes": self.quotes,
                "fetch_date": self.fetch_date,
            },
        )
        for quote in self.quotes.values():
            if quote.error:
                logger.error(
                    f"{ICON} Error in quote from {quote.source}: {quote.error}",
                    extra={"notification": False, **quote.log_data},
                )
        self.get_one_quote()
        self.fetch_date = self.quote.fetch_date
        AllQuotes.fetch_date_class = self.fetch_date
        self.quote = self.get_one_quote()
        self.redis_hit = False
        redis_client = InternalConfig.redis
        cache_data_pickle = pickle.dumps(self.global_quote_pack())
        cache_times = (
            TESTING_CACHE_TIMES if InternalConfig().config.development.enabled else CACHE_TIMES
        )
        redis_client.setex(
            "all_quote_class_quote", time=cache_times["Global"], value=cache_data_pickle
        )
        if store_db:
            await self.db_store_quote()

    def global_quote_pack(self) -> Dict[str, Any]:
        """
        Pack the global quotes into a dictionary format.

        Returns:
            Dict[str, Any]: A dictionary containing the packed global quotes.
        """
        return {
            "quotes": self.all_quotes_filtered(),
            "fetch_date": self.fetch_date.isoformat() if self.fetch_date else None,
            "source": self.source,
        }

    def all_quotes_filtered(self) -> Dict[str, Dict[str, Any]]:
        """
        Filter out quotes with errors and return the remaining valid quotes.

        Returns:
            Dict[str, QuoteResponse]: A dictionary containing only the valid quotes
            without errors.
        """
        no_error_quotes = {}
        for service_name, quote in self.quotes.items():
            if not quote.error:
                quote_dict = quote.model_dump(exclude={"raw_response"})
                # Ensure fetch_date is serialized as ISO string
                if "fetch_date" in quote_dict and hasattr(quote_dict["fetch_date"], "isoformat"):
                    quote_dict["fetch_date"] = quote_dict["fetch_date"].isoformat()
                no_error_quotes[service_name] = quote_dict
        return no_error_quotes

    def unpack_quotes(self, cache_data: Dict[str, Any]) -> Dict[str, QuoteResponse]:
        """
        Unpack the quotes from the cached data, handling datetime conversion.

        Returns:
            Dict[str, QuoteResponse]: A dictionary containing the unpacked quotes.
        """
        quotes = {}
        for service_name, quote_data in cache_data.get("quotes", {}).items():
            # Convert ISO string back to datetime if needed
            if "fetch_date" in quote_data and isinstance(quote_data["fetch_date"], str):
                quote_data["fetch_date"] = datetime.fromisoformat(
                    quote_data["fetch_date"].replace("Z", "+00:00")
                )
            quotes[service_name] = QuoteResponse.model_validate(quote_data)
        return quotes

    async def check_global_cache(self) -> bool:
        """
        Check if the global cache is available and valid.

        Returns:
            bool: True if the global cache is valid, False otherwise.
        """
        redis_client = InternalConfig.redis
        cache_data_pickle = redis_client.get("all_quote_class_quote")
        if cache_data_pickle:
            cache_data = pickle.loads(cache_data_pickle)
            # Handle fetch_date conversion
            if "fetch_date" in cache_data:
                if isinstance(cache_data["fetch_date"], str):
                    self.fetch_date = datetime.fromisoformat(
                        cache_data["fetch_date"].replace("Z", "+00:00")
                    )
                else:
                    self.fetch_date = cache_data["fetch_date"]
            self.quotes = self.unpack_quotes(cache_data)
            self.get_one_quote()
            self.source = cache_data["source"]
            self.redis_hit = True
            return True
        self.redis_hit = False
        return False

    def legacy_api_format(self) -> dict[str, Any]:
        """
        Format cryptocurrency quotes in a compact, flat structure for API responses.

        Returns:
            dict: A dictionary with cryptocurrency exchange rates and metadata.
        """
        # Get the authoritative quote (usually from Binance unless error)
        quote = self.get_one_quote()

        # Calculate sats per USD
        sats_usd = SATS_PER_BTC / quote.btc_usd if quote.btc_usd else 0

        # Collect any errors from services
        fetch_errors = []
        for service_name, service_quote in self.quotes.items():
            if service_quote.error:
                fetch_errors.append(f"{service_name}: {service_quote.error}")

        # Determine if this was a Redis hit - simplistic approach
        age = quote.age

        # Build the formatted output
        result = {
            "BTC_USD": quote.btc_usd,
            "sats_USD": sats_usd,
            "HBD_USD": quote.hbd_usd,
            "HiveMarket_HBD_USD": quote.hbd_usd,  # Same as HBD_USD unless you have a specific source
            "Hive_USD": quote.hive_usd,
            "Hive_HBD": quote.hive_hbd,
            "sats_Hive": quote.sats_hive,
            "sats_HBD": quote.sats_hbd,
            "Hive_sats": 1.0 / quote.sats_hive_p if quote.sats_hive_p else 0,
            "HBD_sats": 1.0 / quote.sats_hbd_p if quote.sats_hbd_p else 0,
            "fetch_error": fetch_errors,
            "last_fetch": quote.fetch_date.isoformat(),
            "fetch_time": age,  # Using age as a proxy for fetch time
            "conversion": None,  # Add conversion logic if needed
            "redis_hit": self.redis_hit,
        }

        return result

    # MARK: DB Store Quote

    async def db_store_quote(self, store_db: bool = True) -> HiveRatesDB:
        """
        Store cryptocurrency quotes in the database.

        This asynchronous method saves cryptocurrency exchange rates into the database.
        It uses the MongoDB client to insert records into the "hive_rates" collection.
        Each record includes a timestamp, a currency pair, and its corresponding value.

        The following currency pairs are stored:
        - "hive_usd": Hive to USD exchange rate.
        - "btc_usd": Bitcoin to USD exchange rate.
        - "sats_hive": Satoshi to Hive exchange rate.
        - "hive_hbd": Hive to HBD exchange rate.

        Logs debug messages for each successful insertion.

        Returns:
            None
        """
        record = HiveRatesDB(
            timestamp=self.fetch_date,
            hive_usd=self.quote.hive_usd,
            hbd_usd=self.quote.hbd_usd,  # Assuming sats_hbd is used for hbd_usd
            btc_usd=self.quote.btc_usd,
            sats_hive=self.quote.sats_hive_p,
            sats_usd=self.quote.sats_usd,
            sats_hbd=self.quote.sats_hbd_p,
            hive_hbd=self.quote.hive_hbd,
        )
        if not store_db:
            return record
        if (
            AllQuotes.db_store_timestamp
            and self.fetch_date - AllQuotes.db_store_timestamp
            < timedelta(seconds=DB_RATES_MIN_INTERVAL)
        ):
            delta = format_time_delta(datetime.now(tz=timezone.utc) - AllQuotes.db_store_timestamp)
            logger.info(f"{ICON} Skipping database store, last store was {delta} ago")
            return record
        try:
            AllQuotes.db_store_timestamp = self.fetch_date
            context = f"{DB_RATES_COLLECTION}:{self.fetch_date.isoformat()}"
            # Do not await: runs in background
            task = asyncio.create_task(
                mongo_call(
                    lambda: AllQuotes.collection().insert_one(document=record.model_dump()),
                    error_code="mongodb_rates_insert",
                    context=context,
                ),
                name=f"rates_insert:{self.fetch_date.isoformat()}",
            )
            task.add_done_callback(_log_rates_insert_done)
            return record
        except Exception as e:
            logger.warning(
                f"{ICON} Failed to insert rates into database: {e}",
                extra={"notification": False},
            )
        return record

    @property
    def collection_name(self) -> str:
        """
        Get the collection name for storing quotes in the database.

        Returns:
            str: The name of the collection where quotes are stored.
        """
        return DB_RATES_COLLECTION

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Get the collection for storing quotes in the database.

        Returns:
            AsyncCollection: The collection where quotes are stored.
        """
        return InternalConfig.db[DB_RATES_COLLECTION]

    def calculate_average_quote(self) -> QuoteResponse | None:
        """
        Calculate the average values of various cryptocurrency quotes.

        This method filters out quotes with errors and calculates the average
        values for HIVE to USD, HBD to USD, BTC to USD, and HIVE to HBD from
        the remaining valid quotes.

        Returns:
            QuoteResponse: An object containing the average values for the
            following:
            - hive_usd (float): The average HIVE to USD value.
            - hbd_usd (float): The average HBD to USD value.
            - btc_usd (float): The average BTC to USD value.
            - hive_hbd (float): The average HIVE to HBD value.
            - raw_response (dict): An empty dictionary.

            If there are no valid quotes, returns None.
        """
        # Must exclude HiveInternalMarket quote because it only quotes for HBD Hive
        good_quotes = [
            quote
            for quote in self.quotes.values()
            if not (quote.error or quote.source == "HiveInternalMarket")
        ]
        error_details = {
            quote.source: quote.error_details for quote in self.quotes.values() if quote.error
        }

        if not good_quotes:
            self.source = "failure"
            return None
        self.source = ", ".join([quote.source for quote in good_quotes])

        avg_hive_usd = sum(quote.hive_usd for quote in good_quotes) / len(good_quotes)
        avg_hbd_usd = sum(quote.hbd_usd for quote in good_quotes) / len(good_quotes)
        avg_btc_usd = sum(quote.btc_usd for quote in good_quotes) / len(good_quotes)
        avg_hive_hbd = sum(quote.hive_hbd for quote in good_quotes) / len(good_quotes)

        # get the latest fetch date of the quotes in good_quotes
        fetch_dates = [quote.fetch_date for quote in good_quotes]
        self.fetch_date = (
            max(fetch for fetch in fetch_dates if fetch is not None)
            if fetch_dates
            else datetime.now(tz=timezone.utc)
        )
        return QuoteResponse(
            hive_usd=avg_hive_usd,
            hbd_usd=avg_hbd_usd,
            btc_usd=avg_btc_usd,
            hive_hbd=avg_hive_hbd,
            source=self.source,
            fetch_date=self.fetch_date,
            raw_response={},  # You can decide what to put here
            error_details=error_details,
        )

    @property
    def hive_hbd(self) -> float:
        if HiveInternalMarket.__name__ in self.quotes:
            return self.quotes[HiveInternalMarket.__name__].hive_hbd
        hive_hbd = 0.0
        count = 0
        for service_name, quote in self.quotes.items():
            if quote.hive_hbd:
                hive_hbd += quote.hive_hbd
                count += 1
        if count == 0:
            raise ValueError("No valid Hive HBD price found")
        return hive_hbd / count


class QuoteServiceError(Exception):
    pass


class CoinGeckoError(QuoteServiceError):
    pass


class BinanceError(QuoteServiceError):
    pass


class CoinMarketCapError(QuoteServiceError):
    pass


class HiveInternalMarketError(QuoteServiceError):
    pass


class QuoteService(ABC):
    @abstractmethod
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        pass

    async def check_cache(self, use_cache: bool = True) -> QuoteResponse | None:
        if use_cache:
            key = f"{self.__class__.__name__}:get_quote"
            redis_client = InternalConfig.redis
            cached_quote = redis_client.get(key)
            if cached_quote:
                return pickle.loads(cached_quote)
        return None

    async def set_cache(self, quote: QuoteResponse) -> None:
        key = f"{self.__class__.__name__}:get_quote"
        if InternalConfig().config.development.enabled:
            cache_times = TESTING_CACHE_TIMES
        else:
            cache_times = CACHE_TIMES
        expiry = cache_times[self.__class__.__name__]
        redis_client = InternalConfig.redis
        redis_client.setex(key, time=expiry, value=pickle.dumps(quote))


class CoinGecko(QuoteService):
    @async_time_decorator
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        cached_quote = await self.check_cache(use_cache=use_cache)
        if cached_quote:
            return cached_quote
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    ALL_PRICES_COINGECKO, timeout=10, follow_redirects=True
                )
                if response.status_code == 200:
                    pri = response.json()
                    quote_response = QuoteResponse(
                        hive_usd=pri["hive"]["usd"],
                        hbd_usd=pri["hive_dollar"]["usd"],
                        btc_usd=pri["bitcoin"]["usd"],
                        hive_hbd=pri["hive"]["usd"] / pri["hive_dollar"]["usd"],
                        raw_response=pri,
                        source=self.__class__.__name__,
                        fetch_date=datetime.now(tz=timezone.utc),
                    )
                    await self.set_cache(quote_response)
                    return quote_response
                else:
                    raise CoinGeckoError(f"Failed to get quote: {response.text}")
        except Exception as ex:
            message = f"Problem calling {self.__class__.__name__} API {ex}"
            return QuoteResponse(
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class Binance(QuoteService):
    @async_time_decorator
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        """
        Retrieve a cryptocurrency quote, optionally using a cached value.

        Args:
            use_cache (bool): Whether to use the cached quote if available. Defaults to True.

        Returns:
            QuoteResponse: The retrieved cryptocurrency quote, either from the cache or fetched synchronously.

        Notes:
            - If a cached quote is available and `use_cache` is True, it will be returned.
            - If no cached quote is available or `use_cache` is False, a new quote will be fetched
              synchronously and stored in the cache for future use.
        """
        cached_quote = await self.check_cache(use_cache=use_cache)
        if cached_quote:
            return cached_quote
        quote = self.get_quote_sync(use_cache=use_cache)
        await self.set_cache(quote)
        return quote

    def get_quote_sync(self, use_cache: bool = True) -> QuoteResponse:
        """
        Fetches cryptocurrency quotes synchronously using the Binance API.

        This method retrieves the median prices for HIVE/USDT, HIVE/BTC, and BTC/USDT
        trading pairs from Binance. It calculates additional derived values such as
        HIVE to HBD and HIVE to BTC prices. The results are returned in a `QuoteResponse`
        object.

        Args:
            use_cache (bool): Whether to use cached data. Defaults to True.

        Returns:
            QuoteResponse: An object containing the fetched cryptocurrency prices,
            metadata, and any errors encountered during the process.

        Raises:
            Exception: If an error occurs during the API call or data processing,
            it is caught and logged, and an error response is returned.
        """
        internal_config = InternalConfig()
        api_keys_config = internal_config.config.api_keys
        try:
            # raise Exception("debug")
            client = Spot(
                api_key=api_keys_config.binance_api_key,
                api_secret=api_keys_config.binance_api_secret,
            )
            ticker_info = client.book_ticker(symbols=["HIVEUSDT", "HIVEBTC", "BTCUSDT"])
            medians = {}
            for ticker in ticker_info:
                bid_price = float(ticker["bidPrice"])
                ask_price = float(ticker["askPrice"])
                median = (bid_price + ask_price) / 2
                medians[ticker["symbol"]] = median

            hive_usd = medians["HIVEUSDT"]
            hbd_usd = 1
            btc_usd = medians["BTCUSDT"]
            hive_hbd = hive_usd / hbd_usd

            # calc hive to btc price based on hiveusdt and btcusdt
            hive_sats = (medians["HIVEUSDT"] / medians["BTCUSDT"]) * 1e8
            # check
            logger.debug(f"{ICON} Binance Hive to BTC price : {hive_sats:.1f}")

            quote_response = QuoteResponse(
                hive_usd=hive_usd,
                hbd_usd=hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=hive_hbd,
                raw_response=ticker_info,
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
            )
            return quote_response

        except Exception as ex:
            message = f"Problem calling {self.__class__.__name__} API {ex}"
            return QuoteResponse(
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class CoinMarketCap(QuoteService):
    @async_time_decorator
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        cached_quote = await self.check_cache(use_cache=use_cache)
        if cached_quote:
            return cached_quote

        internal_config = InternalConfig()
        api_keys_config = internal_config.config.api_keys
        url = ALL_PRICES_COINMARKETCAP
        cmc_ids = {
            "BTC_USD": "1",
            "Hive_USD": "5370",
            "HBD_USD": "5375",
        }
        ids_str = [str(id) for _, id in cmc_ids.items()]
        call_ids = ",".join(ids_str)
        params = {"id": call_ids, "convert": "USD"}
        headers = {
            "Accepts": "application/json",
            "X-CMC_PRO_API_KEY": api_keys_config.coinmarketcap,
        }
        try:
            # raise Exception("debug")
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=10,
                    follow_redirects=True,
                )
            if response.status_code == 200:
                resp_json = response.json()
                quote = resp_json["data"][cmc_ids["Hive_USD"]]["quote"]
                Hive_USD = quote["USD"]["price"]
                quote = resp_json["data"][cmc_ids["HBD_USD"]]["quote"]
                HBD_USD = quote["USD"]["price"]
                quote = resp_json["data"][cmc_ids["BTC_USD"]]["quote"]
                BTC_USD = quote["USD"]["price"]
                Hive_HBD = Hive_USD / HBD_USD
                quote_response = QuoteResponse(
                    hive_usd=Hive_USD,
                    hbd_usd=HBD_USD,
                    btc_usd=BTC_USD,
                    hive_hbd=Hive_HBD,
                    raw_response=resp_json,
                    source=self.__class__.__name__,
                    fetch_date=datetime.now(tz=timezone.utc),
                )
                await self.set_cache(quote_response)
                return quote_response
            else:
                raise CoinMarketCapError(f"Failed to get quote: {response.text}")
        except Exception as ex:
            message = f"Problem calling {self.__class__.__name__} API {ex}"
            return QuoteResponse(
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class HiveInternalMarket(QuoteService):
    @async_time_decorator
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        self.source = "HiveInternalMarket"
        cached_quote = await self.check_cache(use_cache=use_cache)
        if cached_quote:
            return cached_quote
        try:
            hive_quote = await call_hive_internal_market()
            if hive_quote.error:
                raise HiveInternalMarketError(
                    f"Problem calling Hive Market API {hive_quote.error}"
                )
            hive_hbd = hive_quote.hive_hbd if hive_quote.hive_hbd else 0
            raw_response = hive_quote.raw_response
            quote_response = QuoteResponse(
                hive_usd=0,
                hbd_usd=0,
                btc_usd=0,
                hive_hbd=hive_hbd,
                raw_response=raw_response,
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
            )
            # await self.set_cache(quote_response)

            return quote_response
        except Exception as ex:
            message = f"Problem calling {self.__class__.__name__} API {ex}"
            return QuoteResponse(
                source=self.__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


def per_diff(a: float, b: float) -> float:
    """
    Calculate the percentage difference between two numbers.

    Args:
        a (float): The first number.
        b (float): The second number.

    Returns:
        float: The percentage difference between `a` and `b`. If `b` is 0, returns 0.
    """
    if b == 0:
        return 0
    return ((a - b) / b) * 100


# Last line

# Last line

# Last line

# Last line
