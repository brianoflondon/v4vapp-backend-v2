import asyncio
import json
from datetime import datetime
from typing import Any

from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency, QuoteResponse


class CryptoConv(BaseModel):
    """
    Simple dictionary to store the conversion values.
    """

    hive: float = Field(0.0, description="Converted value in HIVE")
    hbd: float = Field(0.0, description="Converted value in HBD")
    usd: float = Field(0.0, description="Converted value in USD")
    sats: int = Field(0, description="Converted value in Sats")
    msats: int = Field(0, description="Converted value in milliSats")
    btc: float = Field(0.0, description="Converted value in Bitcoin")
    sats_hive: float = Field(0.0, description="Sats per HIVE")
    sats_hbd: float = Field(0.0, description="Sats per HBD")
    conv_from: Currency = Field(
        Currency.HIVE, description="The currency from which the conversion is made"
    )
    value: float = Field(0.0, description="The original value before conversion")
    source: str = Field(
        "CryptoConv", description="The source of the quote used for this conversion"
    )
    fetch_date: datetime | None = Field(
        None, description="The date when the conversion was fetched"
    )

    model_config = ConfigDict(
        use_enum_values=True,  # Serializes enum as its value
    )

    @property
    def log_str(self) -> str:
        """
        Generates a formatted string representation of the cryptocurrency conversion.

        Returns:
            str: A string in the format "($<USD amount> <Satoshi amount> sats)", where:
                 - <USD amount> is the conversion value in USD, formatted to two decimal places.
                 - <Satoshi amount> is the conversion value in Sats, formatted with commas as thousand separators.
        """
        return f"(${self.usd:>.2f} {self.sats:,.0f} sats)"

    @property
    def notification_str(self) -> str:
        """
        Generates a formatted string representation of the cryptocurrency conversion.

        Returns:
            str: A string in the format "($<USD amount> <Satoshi amount> sats)", where:
                 - <USD amount> is the conversion value in USD, formatted to two decimal places.
                 - <Satoshi amount> is the conversion value in Sats, formatted with commas as thousand separators.
        """
        return self.log_str


class CryptoConversion(BaseModel):
    conv_from: Currency = Currency.HIVE
    value: float = 0.0
    original: Any = None
    quote: QuoteResponse = QuoteResponse()
    fetch_date: datetime | None = None

    # Cached computed fields
    hive: float = 0.0
    hbd: float = 0.0
    usd: float = 0.0
    sats: float = 0
    msats: int = 0
    btc: float = 0.0

    # model_config = ConfigDict(
    #     arbitrary_types_allowed=True,  # Allow 'Amount' type from beem
    # )

    def __init__(
        self,
        amount: Amount | None = None,
        value: float | int = 0.0,
        conv_from: Currency | None = None,
        quote: QuoteResponse | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if amount or ("amount" in kwargs and isinstance(kwargs["amount"], Amount)):
            self.conv_from = Currency(amount.symbol.lower())
            self.value = amount.amount
            self.original = amount
        elif conv_from:
            if isinstance(conv_from, str):
                self.conv_from = Currency(conv_from.lower())
            self.conv_from = conv_from
            self.value = value

        if quote:
            self.quote = quote
            self._compute_conversions()

    async def get_quote(self, use_cache: bool = True):
        """Fetch the quote and compute all conversions once."""
        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes(use_cache=use_cache)
        for source, quote in all_quotes.quotes.items():
            print(f"{source} {quote.source} {quote.fetch_date} {quote.error}")

        self.quote = all_quotes.quote
        self._compute_conversions()
        self.fetch_date = self.quote.fetch_date

    def _compute_conversions(self):
        """Compute all currency conversions starting from msats."""
        # Step 1: Convert the input value to msats
        if self.conv_from == Currency.HIVE:
            self.msats = int(self.value * self.quote.sats_hive * 1000)
        elif self.conv_from == Currency.HBD:
            self.msats = int(self.value * self.quote.sats_hbd * 1000)
        elif self.conv_from == Currency.USD:
            self.msats = int(self.value * self.quote.sats_usd * 1000)
        elif self.conv_from == Currency.SATS:
            self.msats = int(self.value * 1000)
        else:
            raise ValueError("Unsupported conversion currency")

        # Step 2: Derive sats from msats
        self.sats = round(self.msats / 1000, 0)

        # Step 3: Derive all other values from msats
        self.btc = self.msats / 100_000_000_000  # msats to BTC (1 BTC = 10^11 msats)
        self.usd = round(self.msats / (self.quote.sats_usd * 1000), 6)
        self.hbd = round(self.msats / (self.quote.sats_hbd * 1000), 6)
        self.hive = round(self.msats / (self.quote.sats_hive * 1000), 5)

    @property
    def conversion(self) -> CryptoConv:
        """Return a CryptoConv model with all conversion values."""
        return CryptoConv(
            hive=self.hive,
            hbd=self.hbd,
            usd=self.usd,
            sats=int(self.sats),  # Cast to int to match CryptoConv type
            msats=self.msats,
            btc=self.btc,
            # These two values are floats, they are property functions of quote
            sats_hive=self.quote.sats_hive,  # type: float
            sats_hbd=self.quote.sats_hbd,  # type: float
            conv_from=self.conv_from,
            value=self.value,
            source=self.quote.source,
            fetch_date=self.quote.fetch_date,
        )

    @property
    def c_dict(self) -> dict[str, Any]:
        """Return a dictionary of all conversions."""
        return {
            Currency.HIVE: self.hive,
            Currency.HBD: self.hbd,
            Currency.USD: self.usd,
            Currency.SATS: self.sats,
            Currency.BTC: self.btc,
            Currency.MSATS: self.msats,
            "sats_hive": self.quote.sats_hive,
            "sats_hbd": self.quote.sats_hbd,
            "conv_from": self.conv_from,
            "value": self.value,
        }


if __name__ == "__main__":
    # Test CryptoConversion
    amount = Amount("10 HIVE")
    conv = CryptoConversion(amount=amount)
    asyncio.run(conv.get_quote(use_cache=False))
    print(f"Fetch date: {conv.fetch_date}")
    print(f"HIVE: {conv.hive}")
    print(f"HBD: {conv.hbd}")
    print(f"USD: {conv.usd}")
    print(f"SATS: {conv.sats}")
    print(f"Source: {conv.quote.source}")
    print(json.dumps(conv.c_dict, indent=2, default=str))
    print(json.dumps(conv.conversion.model_dump(), indent=2, default=str))
    asyncio.run(conv.get_quote(use_cache=False))
    print(f"Fetch date: {conv.fetch_date}")
