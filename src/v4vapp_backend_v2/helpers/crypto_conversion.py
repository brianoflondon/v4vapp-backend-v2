import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from math import isclose
from typing import Any, ClassVar

from nectar.amount import Amount
from pydantic import BaseModel, ConfigDict, Field, computed_field

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency, QuoteResponse
from v4vapp_backend_v2.helpers.service_fees import limit_test, msats_fee
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


class CryptoConv(BaseModel):
    """
    Simple dictionary to store the conversion values.
    """

    hive: float = Field(0.0, description="Converted value in HIVE")
    hbd: float = Field(0.0, description="Converted value in HBD")
    usd: float = Field(0.0, description="Converted value in USD")
    sats: int = Field(0, description="Converted value in Sats")
    msats: int = Field(0, description="Converted value in milliSats")
    msats_fee: int = Field(0, description="Service fee in milliSats")
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

    UNIT_TOLERANCE: ClassVar[dict[str, float]] = {
        "HIVE": 0.003,
        "HBD": 0.002,
        "USD": 0.002,
        "SATS": 0.5,
        "MSATS": 500,
        "BTC": 5e-9,
    }

    REL_TOL: ClassVar[float] = 1e-7

    def __init__(
        self,
        recalc_conv_from: Currency | None = None,
        conv_from: Currency | None = None,
        value: float | None = None,
        converted_value: float | None = None,
        timestamp: datetime | None = None,
        quote: QuoteResponse | None = None,
        **data: Any,
    ):
        if recalc_conv_from and value and quote:
            # If recalc_conv_from and value are provided, we assume it's a conversion from one currency to another
            conversion = CryptoConversion(
                conv_from=recalc_conv_from,
                value=value,
                quote=quote,
            )
            data = conversion.c_dict
        if value is not None:
            # If value is provided, we set it as the original value
            data["value"] = value
        if conv_from is not None:
            # If conv_from is provided, we set it as the conversion source
            data["conv_from"] = conv_from
        if data.get("converted_value", converted_value) and data.get("conv_from", conv_from):
            # If 'converted' is in data, we assume it's a conversion from one Hive to HBD or vice versa,
            # and we need to set the hive and hbd values accordingly using the internal market rates.
            if data.get("conv_from", conv_from) == Currency.HIVE:
                data["hive"] = data.get("value", value)
                data["hbd"] = data.get("converted_value", converted_value)
            elif data.get("conv_from", conv_from) == Currency.HBD:
                data["hbd"] = data.get("value", value)
                data["hive"] = data.get("converted_value", converted_value)
            data["source"] = "Hive Internal Trade"
            data["fetch_date"] = data.get("fetch_date", timestamp) or datetime.now(tz=timezone.utc)
            quote = data.get("quote", quote)
            # TODO: #109 implement a way to look up historical quote
            if quote and quote.sats_usd > 0:
                data["source"] = quote.source
                data["fetch_date"] = quote.fetch_date or datetime.now(tz=timezone.utc)
                data["sats_hive"] = quote.sats_hive
                data["sats_hbd"] = quote.sats_hbd
                data["sats"] = int(data["hive"] * quote.sats_hive)
                data["msats"] = int(data["sats"] * 1000)
                data["btc"] = data["msats"] / 100_000_000_000
                data["usd"] = round(data["sats"] / (quote.sats_usd), 6)

        super().__init__(**data)
        # If msats is not set, calculate it from the other values
        if "msats" not in data:
            self.msats = int(self.sats * 1000)
        # If sats is not set, calculate it from the msats
        if "sats" not in data:
            self.sats = int(self.msats / 1000)

    def __neg__(self):
        # List of fields NOT to invert
        rate_fields = {"sats_hive", "sats_hbd", "conv_from", "source", "fetch_date"}
        values = self.model_dump()
        for key in values:
            if key not in rate_fields and isinstance(values[key], (int, float)):
                values[key] = -values[key]
        return self.__class__(**values)

    def __mul__(self, other):
        if isinstance(other, (int, float)):
            values = self.model_dump()
            rate_fields = {"sats_hive", "sats_hbd", "conv_from", "source", "fetch_date"}
            for key in values:
                if key not in rate_fields and isinstance(values[key], (int, float)):
                    values[key] = values[key] * other
            return self.__class__(**values)
        return NotImplemented

    def __rmul__(self, other):
        return self.__mul__(other)

    def __eq__(self, other):
        if isinstance(other, CryptoConv):
            if not self.fetch_date and not other.fetch_date:
                return True
            if self.msats == 0 and other.msats == 0:
                return True
            if not isclose(
                self.hive, other.hive, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["HIVE"]
            ):
                return False
            if not isclose(
                self.hbd, other.hbd, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["HBD"]
            ):
                return False
            if not isclose(
                self.usd, other.usd, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["USD"]
            ):
                return False
            if not isclose(
                self.sats, other.sats, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["SATS"]
            ):
                return False
            if not isclose(
                self.msats, other.msats, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["MSATS"]
            ):
                return False
            if not isclose(
                self.btc, other.btc, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["BTC"]
            ):
                return False
            if not isclose(
                self.msats_fee,
                other.msats_fee,
                rel_tol=self.REL_TOL,
                abs_tol=self.UNIT_TOLERANCE["MSATS"],
            ):
                return False
            return True
        return NotImplemented

    def is_unset(self) -> bool:
        """
        Check if the conversion values are unset (zero).

        Returns:
            bool: True if all conversion values are zero, False otherwise.
        """
        return (
            self.hive == 0.0
            and self.hbd == 0.0
            and self.usd == 0.0
            and self.sats == 0
            and self.msats == 0
            and self.btc == 0.0
            and self.msats_fee == 0
        )

    def is_set(self) -> bool:
        """
        Check if the conversion values are set (non-zero).

        Returns:
            bool: True if any conversion value is non-zero, False otherwise.
        """
        return not self.is_unset()

    def limit_test(self) -> bool:
        """
        Check if the conversion is within the limits.

        Returns:
            bool: True if the conversion is within limits, False otherwise.

        Raises:
            V4VMinimumInvoice: If the amount is less than the configured minimum invoice payment in satoshis.
            V4VMaximumInvoice: If the amount is greater than the configured maximum invoice payment in satoshis.

        """
        limit_test_result = limit_test(self.msats)
        return limit_test_result

    @computed_field
    def in_limits(self) -> bool:
        """
        Check if the conversion is within the limits.

        Returns:
            bool: True if the conversion is within limits, False otherwise.
        """
        try:
            return self.limit_test()
        except ValueError:
            return False

    @property
    def log_str(self) -> str:
        """
        Generates a formatted string representation of the cryptocurrency conversion.

        Returns:
            str: A string in the format "($<USD amount> <Satoshi amount> sats)", where:
                 - <USD amount> is the conversion value in USD, formatted to two decimal places.
                 - <Satoshi amount> is the conversion value in Sats, formatted with commas as thousand separators.
        """
        fee_sats: int = int(round(self.msats_fee / 1000, 0))
        fee_str: str = f" ±{fee_sats:,}" if fee_sats > 0 else ""
        return f"(${self.usd:>.2f} {self.sats:,.0f} sats){fee_str}"

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

    @property
    def amount_hive(self) -> Amount:
        """
        Returns the conversion value in HIVE as an Amount object.

        Returns:
            Amount: The conversion value in HIVE.
        """
        return Amount(f"{self.hive:.3f} HIVE")

    @property
    def amount_hbd(self) -> Amount:
        """
        Returns the conversion value in HBD as an Amount object.

        Returns:
            Amount: The conversion value in HBD.
        """
        return Amount(f"{self.hbd:.3f} HBD")


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
    msats_fee: int = 0

    # model_config = ConfigDict(
    #     arbitrary_types_allowed=True,  # Allow 'Amount' type from beem
    # )
    model_config = ConfigDict(
        use_enum_values=True,  # Serializes enum as its value
    )

    def __init__(
        self,
        amount: Amount | AmountPyd | None = None,
        value: float | int = 0.0,
        conv_from: Currency | None = None,
        quote: QuoteResponse | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if isinstance(value, Decimal):
            value = float(value)
        if (
            isinstance(amount, Amount)
            or isinstance(amount, AmountPyd)
            or ("amount" in kwargs and isinstance(kwargs["amount"], Amount))
        ):
            amount_here = kwargs.get("amount", amount)
            self.conv_from = Currency(amount_here.symbol.lower())
            self.value = amount_here.amount
            self.original = amount_here
            if isinstance(amount_here, AmountPyd):
                self.value = amount_here.amount_decimal
        elif conv_from:
            if isinstance(conv_from, str):
                self.conv_from = Currency(conv_from.lower())
            self.conv_from = conv_from
            self.value = value

        if quote:
            self.quote = quote
            self._compute_conversions()

    async def get_quote(self, use_cache: bool = True):
        """
        Asynchronously retrieves the latest cryptocurrency quotes and updates the instance with the fetched data.
        Args:
            use_cache (bool, optional): Whether to use cached quotes if available. Defaults to True.
        Side Effects:
            - Updates the instance's `quote` attribute with the latest quote data.
            - Calls a private method `_compute_conversions()` to update conversion values.
            - Sets the `fetch_date` attribute to the date when the quote was fetched.
        """

        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes(use_cache=use_cache)
        self.quote = all_quotes.quote
        self._compute_conversions()
        self.fetch_date = self.quote.fetch_date

    def _compute_conversions(self):
        """Compute all currency conversions starting from msats."""
        # Step 1: Convert the input value to msats
        if self.quote is None:
            if self.quote.hive_hbd == 0:
                raise ValueError("Quote is set but hive_hbd is zero")
            raise ValueError("Quote is not available or invalid")

        try:
            if self.conv_from == Currency.MSATS:
                self.msats = int(self.value)
            elif self.conv_from == Currency.SATS:
                self.msats = int(self.value * 1000)
            elif self.conv_from == Currency.HIVE:
                self.msats = int(self.value * self.quote.sats_hive_p * 1000)
            elif self.conv_from == Currency.HBD:
                self.msats = int(self.value * self.quote.sats_hbd_p * 1000)
            elif self.conv_from == Currency.USD:
                self.msats = int(self.value * self.quote.sats_usd_p * 1000)
            else:
                raise ValueError("Unsupported conversion currency")

            # Step 2: Derive sats from msats
            self.sats = round(self.msats / 1000, 0)

            # Step 3: Derive all other values from msats
            self.btc = self.msats / 100_000_000_000  # msats to BTC (1 BTC = 10^11 msats)
            self.usd = round(self.msats / (self.quote.sats_usd_p * 1000.0), 6)
            self.hbd = round(self.msats / (self.quote.sats_hbd_p * 1000.0), 6)
            self.hive = round(self.msats / (self.quote.sats_hive_p * 1000.0), 5)
            self.msats_fee = msats_fee(self.msats)
        except ZeroDivisionError:
            # Handle division by zero if the quote is not available
            self.msats = 0
            self.sats = 0
            self.btc = 0.0
            self.usd = 0.0
            self.hbd = 0.0
            self.hive = 0.0
            self.msats_fee = 0

    @property
    def conversion(self) -> CryptoConv:
        """Return a CryptoConv model with all conversion values."""
        return CryptoConv(
            hive=self.hive,
            hbd=self.hbd,
            usd=self.usd,
            sats=int(self.sats),  # Cast to int to match CryptoConv type
            msats=self.msats,
            msats_fee=self.msats_fee,
            btc=self.btc,
            # These two values are floats, they are property functions of quote
            sats_hive=self.quote.sats_hive_p,
            sats_hbd=self.quote.sats_hbd_p,
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
