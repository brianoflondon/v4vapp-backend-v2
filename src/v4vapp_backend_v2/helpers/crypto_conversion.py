import asyncio
import json
from typing import Any

from beem.amount import Amount  # type: ignore
from pydantic import BaseModel

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency, QuoteResponse


class CryptoConversion(BaseModel):
    conv_from: Currency = Currency.HIVE
    value: float = 0.0
    original: Any = None
    quote: QuoteResponse = QuoteResponse()

    # Cached computed fields
    hive: float = 0.0
    hbd: float = 0.0
    usd: float = 0.0
    sats: int = 0
    msats: int = 0
    btc: float = 0.0

    class Config:
        arbitrary_types_allowed = True  # Allow 'Amount' type from beem

    def __init__(
        self,
        amount: Amount = None,
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
        else:
            raise ValueError("Either amount or conv_from must be provided")
        if quote:
            self.quote = quote
            self._compute_conversions()

    async def get_quote(self):
        """Fetch the quote and compute all conversions once."""
        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes()
        self.quote = all_quotes.quote
        self._compute_conversions()

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
    asyncio.run(conv.get_quote())
    print(f"HIVE: {conv.hive}")
    print(f"HBD: {conv.hbd}")
    print(f"USD: {conv.usd}")
    print(f"SATS: {conv.sats}")
    print(json.dumps(conv.c_dict, indent=2))
