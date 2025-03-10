from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict

import httpx
from binance.spot import Spot
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig, logger

ALL_PRICES_COINGECKO = (
    "https://api.coingecko.com/api/v3/simple"
    "/price?ids=bitcoin,hive,"
    "hive_dollar&vs_currencies=btc,usd,eur,aud"
)
SATS_PER_BTC = 100_000_000  # 100 million Satoshis per Bitcoin


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
    raw_response: Dict[str, Any] = {}
    fetch_date: datetime = datetime.now(tz=timezone.utc)
    error: str = ""

    def __init__(
        self,
        hive_usd: float = 0,
        hbd_usd: float = 0,
        btc_usd: float = 0,
        hive_hbd: float = 0,
        raw_response: Dict[str, Any] = {},
        error: str = "",
    ) -> None:
        super().__init__()
        self.hive_usd = round(hive_usd, 4)
        self.hbd_usd = round(hbd_usd, 4)
        self.hive_usd = round(hive_usd, 4)
        self.btc_usd = round(btc_usd, 1)
        self.hive_hbd = round(hive_hbd, 4)
        self.raw_response = raw_response
        self.error = error

    @property
    def sats_hive(self) -> float:
        """Calculate Satoshis per HIVE based on btc_usd and hive_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hive_usd, 4)

    @property
    def sats_hbd(self) -> float:
        """Calculate Satoshis per HBD based on btc_usd and hbd_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return round(sats_per_usd * self.hbd_usd, 4)

    @property
    def quote_age(self) -> int:
        """Calculate the age of the quote in seconds."""
        return int((datetime.now(tz=timezone.utc) - self.fetch_date).total_seconds())


class QuoteService(ABC):
    @abstractmethod
    async def get_quote(self) -> QuoteResponse:
        pass


class QuoteServiceError(Exception):
    pass


class CoinGeckoError(QuoteServiceError):
    pass


class BinanceError(QuoteServiceError):
    pass


class CoinGeckoQuoteService(QuoteService):
    async def get_quote(self) -> QuoteResponse:
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
                    )
                    return quote_response
                else:
                    raise CoinGeckoError(f"Failed to get quote: {response.text}")
        except Exception as e:
            logger.error(e)
            raise CoinGeckoError(f"Failed to get quote: {e}")


class BinanceQuoteService(QuoteService):
    async def get_quote(self) -> QuoteResponse:
        internal_config = InternalConfig()
        binance_config = internal_config.config.binance
        try:
            client = Spot(
                api_key=binance_config.api_key, api_secret=binance_config.api_secret
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
            logger.info(f"Binance Hive to BTC direct: {medians['HIVEBTC']* 1e8:.1f}")

            quote_response = QuoteResponse(
                hive_usd=hive_usd,
                hbd_usd=hbd_usd,
                btc_usd=btc_usd,
                hive_hbd=hive_hbd,
                raw_response=ticker_info,
            )

            return quote_response

        except Exception as ex:
            message = f"Problem calling Binance API {ex}"
            logger.error(message)
            raise BinanceError(message)


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
