from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Dict

import httpx
from pydantic import BaseModel

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
        self.hive_usd = hive_usd
        self.hbd_usd = hbd_usd
        self.btc_usd = btc_usd
        self.hive_hbd = hive_hbd
        self.raw_response = raw_response
        self.error = error

    @property
    def sats_hive(self) -> float:
        """Calculate Satoshis per HIVE based on btc_usd and hive_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return sats_per_usd * self.hive_usd

    @property
    def sats_hbd(self) -> float:
        """Calculate Satoshis per HBD based on btc_usd and hbd_usd."""
        if self.btc_usd == 0:
            raise ValueError("btc_usd cannot be zero")
        sats_per_usd = SATS_PER_BTC / self.btc_usd
        return sats_per_usd * self.hbd_usd

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
            print(e)
            raise CoinGeckoError(f"Failed to get quote: {e}")


class BinanceQuoteService(QuoteService):
    async def get_quote(self) -> QuoteResponse:
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
