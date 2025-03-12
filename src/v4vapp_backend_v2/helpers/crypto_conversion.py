import asyncio
import json
from typing import Any

from beem.amount import Amount  # type: ignore
from pydantic import BaseModel, computed_field

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency, QuoteResponse


class CryptoConversion(BaseModel):
    conv_from: Currency = Currency.HIVE
    value: float = 0.0
    original: Any = None
    quote: QuoteResponse = QuoteResponse()

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

    async def get_quote(self):
        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes()
        self.quote = all_quotes.quote

    @property
    def c_dict(self) -> dict[str, Any]:
        return {
            Currency.HIVE: self.hive,
            Currency.HBD: self.hbd,
            Currency.USD: self.usd,
            Currency.SATS: self.sats,
            Currency.BTC: self.btc,
            Currency.MSATS: self.msats,
            "conv_from": self.conv_from,
            "value": self.value,
        }

    @computed_field
    def hive(self) -> float:
        if self.conv_from == Currency.HIVE:
            return round(self.value, 3)
        elif self.conv_from == Currency.HBD:
            return round(self.value / self.quote.hive_hbd, 3)
        elif self.conv_from == Currency.USD:
            return round(self.value / self.quote.hive_usd, 3)
        elif self.conv_from == Currency.SATS:
            return round(self.value / self.quote.sats_hive, 3)
        else:
            raise ValueError("Unsupported conversion currency")

    @computed_field
    def hbd(self) -> float:
        if self.conv_from == Currency.HBD:
            return round(self.value, 4)
        elif self.conv_from == Currency.HIVE:
            return round(self.value * self.quote.hive_hbd, 4)
        elif self.conv_from == Currency.USD:
            return round(self.value / self.quote.hbd_usd, 4)
        elif self.conv_from == Currency.SATS:
            return round(self.value / self.quote.sats_hbd, 4)
        else:
            raise ValueError("Unsupported conversion currency")

    @computed_field
    def usd(self) -> float:
        if self.conv_from == Currency.USD:
            return round(self.value, 4)
        elif self.conv_from == Currency.HIVE:
            return round(self.value * self.quote.hive_usd, 4)
        elif self.conv_from == Currency.HBD:
            return round(self.value * self.quote.hbd_usd, 4)
        elif self.conv_from == Currency.SATS:
            return round(self.value / self.quote.sats_usd, 4)
        else:
            raise ValueError("Unsupported conversion currency")

    @computed_field
    def sats(self) -> int:
        if self.conv_from == Currency.SATS:
            return int(self.value)
        elif self.conv_from == Currency.HIVE:
            return int(self.value * self.quote.sats_hive)
        elif self.conv_from == Currency.HBD:
            return int(self.value * self.quote.sats_hbd)
        elif self.conv_from == Currency.USD:
            return int(self.value * self.quote.sats_usd)
        else:
            raise ValueError("Unsupported conversion currency")

    @computed_field
    def msats(self) -> int:
        return (self.sats) * 1000

    @computed_field
    def btc(self) -> float:
        return (self.sats) / 100_000_000


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
