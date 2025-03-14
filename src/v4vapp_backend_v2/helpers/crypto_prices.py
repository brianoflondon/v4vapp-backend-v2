import asyncio
import pickle
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import StrEnum
from pprint import pprint
from typing import Any, Dict

import httpx
from binance.spot import Spot  # type: ignore
from pydantic import BaseModel, computed_field

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.async_redis import V4VAsyncRedis
from v4vapp_backend_v2.helpers.hive_extras import call_hive_internal_market

ALL_PRICES_COINGECKO = (
    "https://api.coingecko.com/api/v3/simple"
    "/price?ids=bitcoin,hive,"
    "hive_dollar&vs_currencies=btc,usd,eur,aud"
)
ALL_PRICES_COINMARKETCAP = (
    "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
)

SATS_PER_BTC = 100_000_000  # 100 million Satoshis per Bitcoin

CACHE_TIMES = {
    "CoinGecko": 60,
    "Binance": 60,
    "CoinMarketCap": 180,
    "HiveInternalMarket": 60,
}


class Currency(StrEnum):
    HIVE = "hive"
    HBD = "hbd"
    USD = "usd"
    SATS = "sats"
    MSATS = "msats"
    BTC = "btc"


class CurrencyPair(StrEnum):
    HIVE_USD = "hive_usd"
    HBD_USD = "hbd_usd"
    BTC_USD = "btc_usd"
    HIVE_HBD = "hive_hbd"
    SATS_HIVE = "hive_sats"
    SATS_HBD = "hbd_sats"


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
        quote_age (int): Calculates the age of the quote in seconds.
    """

    hive_usd: float = 0
    hbd_usd: float = 0
    btc_usd: float = 0
    hive_hbd: float = 0
    raw_response: Any = None
    source: str = ""
    fetch_date: datetime = datetime.now(tz=timezone.utc)
    error: str = ""
    error_details: Dict[str, Any] = {}

    def __init__(
        self,
        hive_usd: float = 0,
        hbd_usd: float = 0,
        btc_usd: float = 0,
        hive_hbd: float = 0,
        raw_response: Dict[str, Any] = {},
        source: str = "",
        fetch_date: datetime = datetime.now(tz=timezone.utc),
        error: str = "",
        error_details: Dict[str, Any] = {},
    ) -> None:
        super().__init__()
        self.hive_usd = round(hive_usd, 4)
        self.hbd_usd = round(hbd_usd, 4)
        self.hive_usd = round(hive_usd, 4)
        self.btc_usd = round(btc_usd, 1)
        self.hive_hbd = round(hive_hbd, 4)
        self.raw_response = raw_response
        self.source = source
        self.fetch_date = fetch_date
        self.error = error
        self.error_details = error_details

    @computed_field
    def sats_hive(self) -> float:
        """Calculate Satoshis per HIVE based on btc_usd and hive_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hive_usd, 4)

    @computed_field
    def sats_hbd(self) -> float:
        """Calculate Satoshis per HBD based on btc_usd and hbd_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hbd_usd, 4)

    @property
    def sats_usd(self) -> float:
        """Calculate Satoshis per USD based on btc_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        return round(SATS_PER_BTC / self.btc_usd, 4)

    @computed_field
    def quote_age(self) -> int:
        """Calculate the age of the quote in seconds."""
        return int((datetime.now(tz=timezone.utc) - self.fetch_date).total_seconds())

    @property
    def log(self) -> Dict[str, Any]:
        return self.model_dump(exclude={"raw_response"})


class AllQuotes(BaseModel):
    """
    AllQuotes class is responsible for fetching and aggregating cryptocurrency quotes
    from various services.

    Attributes:
        quotes (Dict[str, QuoteResponse]): A dictionary to store quotes from different
        services.

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
    fetch_date: datetime = datetime.now(tz=timezone.utc)
    source: str = ""

    async def get_all_quotes(self, use_cache: bool = True, timeout: float = 30.0):
        all_services = [
            CoinGecko(),
            Binance(),
            CoinMarketCap(),
            HiveInternalMarket(),
        ]
        self.fetch_date = datetime.now(tz=timezone.utc)
        try:
            async with asyncio.timeout(timeout):
                async with asyncio.TaskGroup() as tg:
                    tasks = {
                        service.__class__.__name__: tg.create_task(
                            service.get_quote(use_cache)
                        )
                        for service in all_services
                    }

        except asyncio.TimeoutError:
            self.quotes = {
                service.__class__.__name__: QuoteResponse(
                    error=f"Timeout after {timeout} seconds"
                )
                for service in all_services
            }
            logger.error(
                f"Quote fetching exceeded timeout of {timeout} seconds {self.quotes}"
            )
            return

        self.quotes = {}
        for service_name, task in tasks.items():
            try:
                self.quotes[service_name] = await task
            except Exception as e:
                logger.error(f"Error fetching quote from {service_name}: {e}")
                self.quotes[service_name] = QuoteResponse(error=str(e))

        self.fetch_date = self.quote.fetch_date

    @property
    def quote(self) -> QuoteResponse:
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

        """
        if self.quotes:
            if (
                Binance.__name__ in self.quotes
                and not self.quotes[Binance.__name__].error
            ):
                self.source = Binance.__name__
                ans = self.quotes[Binance.__name__]
                if HiveInternalMarket.__name__ in self.quotes:
                    ans.hive_hbd = self.hive_hbd
                return ans
            else:
                self.source = "average"
                return self.calculate_average_quote()

        return QuoteResponse(error="No valid quote found")

    def calculate_average_quote(self):
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
            quote.source: quote.error_details
            for quote in self.quotes.values()
            if quote.error
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
            async with V4VAsyncRedis(decode_responses=False) as redis_client:
                cached_quote = await redis_client.get(key)
                if cached_quote:
                    return pickle.loads(cached_quote)
        return None

    async def set_cache(self, quote: QuoteResponse) -> None:
        key = f"{self.__class__.__name__}:get_quote"
        expiry = CACHE_TIMES[self.__class__.__name__]
        async with V4VAsyncRedis(decode_responses=False) as redis_client:
            await redis_client.setex(key, time=expiry, value=pickle.dumps(quote))


class CoinGecko(QuoteService):
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        cached_quote = await self.check_cache(use_cache=use_cache)
        pprint("Calling CoinGecko --------------------------")
        if cached_quote:
            return cached_quote
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    ALL_PRICES_COINGECKO, timeout=10, follow_redirects=True
                )
                if response.status_code == 200:
                    pri = response.json()
                    pprint(pri, indent=2)
                    quote_response = QuoteResponse(
                        hive_usd=pri["hive"]["usd"],
                        hbd_usd=pri["hive_dollar"]["usd"],
                        btc_usd=pri["bitcoin"]["usd"],
                        hive_hbd=pri["hive"]["usd"] / pri["hive_dollar"]["usd"],
                        raw_response=pri,
                        source=__class__.__name__,
                        fetch_date=datetime.now(tz=timezone.utc),
                    )
                    await self.set_cache(quote_response)
                    pprint("Calling CoinGecko --------------------------")
                    return quote_response
                else:
                    raise CoinGeckoError(f"Failed to get quote: {response.text}")
        except Exception as ex:
            message = f"Problem calling {__class__.__name__} API {ex}"
            return QuoteResponse(
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class Binance(QuoteService):
    async def get_quote(self, use_cache: bool = True) -> QuoteResponse:
        cached_quote = await self.check_cache(use_cache=use_cache)
        if cached_quote:
            return cached_quote
        internal_config = InternalConfig()
        api_keys_config = internal_config.config.api_keys
        try:
            raise Exception("debug")

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
            logger.debug(f"Binance Hive to BTC price : {hive_sats:.1f}")

            quote_response = QuoteResponse(
                hive_usd=hive_usd,
                hbd_usd=hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=hive_hbd,
                raw_response=ticker_info,
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
            )
            await self.set_cache(quote_response)
            return quote_response

        except Exception as ex:
            message = f"Problem calling {__class__.__name__} API {ex}"
            return QuoteResponse(
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class CoinMarketCap(QuoteService):
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
            raise Exception("debug")
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
                    source=__class__.__name__,
                    fetch_date=datetime.now(tz=timezone.utc),
                )
                await self.set_cache(quote_response)
                return quote_response
            else:
                raise CoinMarketCapError(f"Failed to get quote: {response.text}")
        except Exception as ex:
            message = f"Problem calling {__class__.__name__} API {ex}"
            return QuoteResponse(
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class HiveInternalMarket(QuoteService):
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
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
            )
            # await self.set_cache(quote_response)

            return quote_response
        except Exception as ex:
            message = f"Problem calling {__class__.__name__} API {ex}"
            return QuoteResponse(
                source=__class__.__name__,
                fetch_date=datetime.now(tz=timezone.utc),
                error=message,
                error_details={"exception": ex},
            )


class CryptoConversion:
    pass


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


# class QuoteResponse(BaseModel):
#     pairs: Any

#     Hive_USD: float = 0
#     HBD_USD: float = 0
#     BTC_USD: float = 0
#     Hive_HBD: float = 0
#     percentage: bool = False
#     error: str = ""
#     fetch_date: datetime = datetime.now(tz=timezone.utc)
#     quote_age: int = 0

#     def __init__(
#         self, HIVE_USD: float, HBD_USD: float, BTC_USD: float, HIVE_HBD: float
#     ) -> None:
#         super().__init__()
#         pairs = {
#             CurrencyPair.HIVE_USD: HIVE_USD,
#             CurrencyPair.HBD_USD: HBD_USD,
#             CurrencyPair.BTC_USD: BTC_USD,
#             CurrencyPair.HIVE_HBD: HIVE_HBD,
#         }

#         quote_age = datetime.now(tz=timezone.utc) - self.fetch_date
#         self.quote_age = int(quote_age.total_seconds())

#     def output(self) -> str:
#         """Produce nicely formatted output for a quote."""
#         per = "%" if self.percentage else " "
#         ans = (
#             f"Hive_USD = {self.Hive_USD:>7.3f}{per} | "
#             f"HBD_USD = {self.HBD_USD:>7.3f}{per} | "
#             f"Hive_HBD = {self.Hive_HBD:>7.3f}{per} | "
#             f"BTC_USD = {self.BTC_USD:>7.1f}{per} | "
#         )
#         return ans

#     def divergence(self, other):
#         """Returns the divergence of two quotes."""
#         if isinstance(other, QuoteResponse):
#             return QuoteResponse(
#                 Hive_USD=per_diff(self.Hive_USD, other.Hive_USD),
#                 HBD_USD=per_diff(self.HBD_USD, other.HBD_USD),
#                 BTC_USD=per_diff(self.BTC_USD, other.BTC_USD),
#                 Hive_HBD=per_diff(self.Hive_HBD, other.Hive_HBD),
#                 percentage=True,
#             )
#         return None

#     def average(self, other):
#         """Returns the average of two quotes."""
#         if isinstance(other, QuoteResponse):
#             return QuoteResponse(
#                 Hive_USD=(self.Hive_USD + other.Hive_USD) / 2,
#                 HBD_USD=(self.HBD_USD + other.HBD_USD) / 2,
#                 BTC_USD=(self.BTC_USD + other.BTC_USD) / 2,
#                 Hive_HBD=(self.Hive_HBD + other.Hive_HBD) / 2,
#             )
#         return None
