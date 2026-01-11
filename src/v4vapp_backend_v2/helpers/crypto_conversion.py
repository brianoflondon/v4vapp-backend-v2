import asyncio
import json
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from math import isclose
from typing import Any, ClassVar

from bson.decimal128 import Decimal128
from nectar.amount import Amount
from pydantic import BaseModel, Field, computed_field, field_validator

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.service_fees import limit_test, msats_fee
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


class CryptoConvV1(BaseModel):
    conv_from: Currency = Currency.HIVE
    sats: float = 0.0
    HIVE: float = 0.0
    HBD: float = 0.0
    USD: float = 0.0

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.HIVE and not self.HBD and not self.USD:
            self.sats = data.get("sats", 0.0)
            self.HIVE = data.get("hive", 0.0)
            self.HBD = data.get("hbd", 0.0)
            self.USD = data.get("usd", 0.0)


class CryptoConv(BaseModel):
    """
    Simple dictionary to store the conversion values.
    """

    hive: Decimal = Field(Decimal(0), description="Converted value in HIVE")
    hbd: Decimal = Field(Decimal(0), description="Converted value in HBD")
    usd: Decimal = Field(Decimal(0), description="Converted value in USD")
    sats: Decimal = Field(Decimal(0), description="Converted value in Sats")
    msats: Decimal = Field(Decimal(0), description="Converted value in milliSats")
    msats_fee: Decimal = Field(Decimal(0), description="Service fee in milliSats")
    btc: Decimal = Field(Decimal(0), description="Converted value in Bitcoin")
    sats_hive: Decimal = Field(Decimal(0), description="Sats per HIVE")
    sats_hbd: Decimal = Field(Decimal(0), description="Sats per HBD")
    conv_from: Currency = Field(
        Currency.HIVE, description="The currency from which the conversion is made"
    )
    value: Decimal = Field(Decimal(0), description="The original value before conversion")
    source: str = Field(
        "CryptoConv", description="The source of the quote used for this conversion"
    )
    fetch_date: datetime | None = Field(
        None, description="The date when the conversion was fetched"
    )

    @property
    def sats_rounded(self) -> Decimal:
        """
        Correctly round sats to the nearest integer using standard rounding.

        Uses ROUND_HALF_UP (standard rounding) instead of Python's default
        ROUND_HALF_EVEN (banker's rounding). This ensures that .5 always rounds up.

        Examples:
        - 4999.4 -> 4999
        - 4999.5 -> 5000 (rounds UP)
        - 4999.6 -> 5000
        """
        return self.sats.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    @field_validator(
        "hive",
        "hbd",
        "usd",
        "btc",
        "sats_hive",
        "sats_hbd",
        "sats",
        "msats",
        "msats_fee",
        "btc",
        "value",
        mode="before",
    )
    @classmethod
    def convert_to_decimal(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        if isinstance(v, Decimal128):
            return Decimal(str(v))
        return v

    UNIT_TOLERANCE: ClassVar[dict[str, float]] = {
        "hive": 0.003,
        "hbd": 0.002,
        "usd": 0.002,
        "sats": 1.5,
        "msats": 500,
        "btc": 5e-9,
        "msats_fee": 2000,  # the fee is so tiny I don't want failures for this
    }

    REL_TOL: ClassVar[float] = 1e-7

    def __init__(
        self,
        recalc_conv_from: Currency | None = None,
        conv_from: Currency | None = None,
        value: float | Decimal | None = None,
        converted_value: float | Decimal | None = None,
        timestamp: datetime | None = None,
        quote: QuoteResponse | None = None,
        **data: Any,
    ):
        """
        Initialize a CryptoConversion instance with various parameters for handling cryptocurrency conversions.
        This method processes the provided parameters to set up conversion data, including values in different currencies
        (e.g., HIVE, HBD, SATS, MSATS, BTC, USD), sources, timestamps, and rates. It handles different scenarios such as
        recalculating conversions, setting values from quotes, or processing exchange order results.
        Args:
            recalc_conv_from (Currency | None, optional): The currency to recalculate the conversion from. If provided
            along with value and quote, creates a new CryptoConversion instance and uses its data. Defaults to None.
            conv_from (Currency | None, optional): The source currency for the conversion. Used to determine how to set
            hive and hbd values. Defaults to None.
            value (float | Decimal | None, optional): The original value to convert. If provided, sets the 'value' in data.
            Defaults to None.
            converted_value (float | Decimal | None, optional): The converted value. Used in conjunction with conv_from
            to set hive and hbd fields. Defaults to None.
            timestamp (datetime | None, optional): The timestamp for the conversion. Used as fetch_date if not provided
            in data. Defaults to None.
            quote (QuoteResponse | None, optional): The quote response containing market rates (e.g., sats_usd, sats_hive_p).
            Used to calculate additional fields like sats, msats, btc, usd, and rates. Defaults to None.
            order_result (ExchangeOrderResult | None, optional): The result of an exchange order. If provided with quote,
            processes trade data to set values like hive, hbd, msats, and trade rates. Defaults to None.
            **data (Any): Additional keyword arguments to be passed to the parent class initializer and used to populate
            the instance attributes.
        Notes:
            - If recalc_conv_from, value, and quote are all provided, a new CryptoConversion is created and its data is used.
            - For conversions involving conv_from and converted_value, sets hive/hbd based on the source currency and marks
              the source as "Hive Internal Trade".
            - If a quote is available, updates source, fetch_date, and calculates sats, msats, btc, usd using the quote rates.
            - For order_result and quote, calculates trade-specific values, including actual trade rates for sats per base currency.
            - After initialization, ensures msats and sats are set by calculating from each other if missing.
        """

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
                data["sats_hive"] = quote.sats_hive_p
                data["sats_hbd"] = quote.sats_hbd_p
                data["sats"] = Decimal(data["hive"]) * quote.sats_hive_p
                data["msats"] = data["sats"] * 1000
                data["btc"] = data["msats"] / 100_000_000_000
                data["usd"] = round(float(data["sats"] / quote.sats_usd_p), 6)

        super().__init__(**data)
        # If msats is not set, calculate it from the other values
        if "msats" not in data:
            self.msats = self.sats * 1000
        # If sats is not set, calculate it from the msats
        if "sats" not in data:
            self.sats = self.msats / 1000

    def __neg__(self):
        # List of fields NOT to invert
        rate_fields = {"sats_hive", "sats_hbd", "conv_from", "source", "fetch_date"}
        values = self.model_dump()
        for key in values:
            if key not in rate_fields and isinstance(values[key], (int, float, Decimal)):
                values[key] = -values[key]
        return self.__class__(**values)

    def __mul__(self, other):
        if isinstance(other, (int, float, Decimal)):
            values = self.model_dump()
            rate_fields = {"sats_hive", "sats_hbd", "conv_from", "source", "fetch_date"}
            for key in values:
                if key not in rate_fields and isinstance(values[key], (int, float, Decimal)):
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
                self.hive, other.hive, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["hive"]
            ):
                return False
            if not isclose(
                self.hbd, other.hbd, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["hbd"]
            ):
                return False
            if not isclose(
                self.usd, other.usd, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["usd"]
            ):
                return False
            if not isclose(
                self.sats, other.sats, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["sats"]
            ):
                return False
            if not isclose(
                self.msats, other.msats, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["msats"]
            ):
                return False
            if not isclose(
                self.btc, other.btc, rel_tol=self.REL_TOL, abs_tol=self.UNIT_TOLERANCE["btc"]
            ):
                return False
            if not isclose(
                self.msats_fee,
                other.msats_fee,
                rel_tol=self.REL_TOL,
                abs_tol=self.UNIT_TOLERANCE["msats_fee"],
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
            self.hive == Decimal(0)
            and self.hbd == Decimal(0)
            and self.usd == Decimal(0)
            and self.sats == Decimal(0)
            and self.msats == Decimal(0)
            and self.btc == Decimal(0)
            and self.msats_fee == Decimal(0)
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
        limit_test_result = limit_test(Decimal(self.msats))
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

    def value_in(self, currency: Currency) -> Decimal:
        """
        Returns the conversion value in the specified currency.
        This is useful when creating LedgerEntries with more decimal places than
        Amount for hive and hbd.

        Args:
            currency (Currency): The currency to convert to.

        Returns:
            Decimal | int: The conversion value in the specified currency.
        """
        if currency == Currency.HIVE:
            return self.hive
        elif currency == Currency.HBD:
            return self.hbd
        elif currency == Currency.MSATS:
            return self.msats
        elif currency == Currency.SATS:
            return self.sats
        elif currency == Currency.USD:
            return self.usd
        elif currency == Currency.BTC:
            return self.btc

        return self.hive

    def amount(self, currency: Currency) -> Amount:
        """
        Returns the conversion value in the original currency as an Amount object.

        Returns:
            Amount: The conversion value in the original currency.
        """
        if self.conv_from == Currency.HIVE:
            return self.amount_hive
        elif self.conv_from == Currency.HBD:
            return self.amount_hbd
        elif currency == Currency.HIVE:
            return self.amount_hive
        elif currency == Currency.HBD:
            return self.amount_hbd

        return self.amount_hive

    def v1(self) -> CryptoConvV1:
        """
        Converts the current instance to a CryptoConvV1 instance.

        Returns:
            CryptoConvV1: The converted instance.
        """
        return CryptoConvV1(
            conv_from=self.conv_from,
            sats=self.sats,
            HIVE=self.hive,
            HBD=self.hbd,
            USD=self.usd,
        )


class CryptoConversion(BaseModel):
    """
    A Pydantic model for handling cryptocurrency conversions between various currencies,
    including HIVE, HBD, USD, SATS, MSATS, and BTC. It uses exchange rates from a QuoteResponse
    to compute and cache conversions starting from a base value in a specified currency.

    Attributes:
        conv_from (Currency): The source currency for conversion (default: Currency.HIVE).
        value (Decimal): The amount in the source currency to convert (default: Decimal(0)).
        original (Any): The original amount object (e.g., Amount or AmountPyd) if provided.
        quote (QuoteResponse): The quote data containing exchange rates.
        fetch_date (datetime | None): The timestamp when the quote was fetched.
        hive (Decimal): Cached converted value in HIVE.
        hbd (Decimal): Cached converted value in HBD.
        usd (Decimal): Cached converted value in USD.
        sats (Decimal): Cached converted value in SATs.
        msats (Decimal): Cached converted value in milliSATs (base unit for conversions).
        btc (Decimal): Cached converted value in BTC.
        msats_fee (Decimal): Cached fee in milliSATs.

    Methods:
        __init__(amount=None, value=Decimal(), conv_from=None, quote=None, **kwargs):
            Initializes the CryptoConversion instance. If an Amount or AmountPyd is provided,
            sets conv_from and value from it. Otherwise, uses the provided conv_from and value.
            If a quote is provided, computes conversions immediately.

        async get_quote(use_cache=True, store_db=True):
            Asynchronously fetches the latest quotes and updates the instance's quote and conversions.
                use_cache (bool): Whether to use cached quotes. Defaults to True.
                store_db (bool): Whether to store quotes in the database. Defaults to True.

        _compute_conversions():
            Computes all currency conversions starting from msats using the current quote.
            Raises ValueError if quote is None or unsupported currency. Handles zero rates with warnings.

    Properties:
        conversion -> CryptoConv:
            Returns a CryptoConv model with all conversion values and metadata.

        c_dict -> dict[str, Any]:
            Returns a dictionary of conversion values keyed by currency and additional metadata.
    """

    conv_from: Currency = Currency.HIVE
    value: Decimal = Decimal(0)
    original: Any = None
    quote: QuoteResponse = QuoteResponse()
    fetch_date: datetime | None = None

    # Cached computed fields
    hive: Decimal = Decimal(0)
    hbd: Decimal = Decimal(0)
    usd: Decimal = Decimal(0)
    sats: Decimal = Decimal(0)
    msats: Decimal = Decimal(0)
    btc: Decimal = Decimal(0)
    msats_fee: Decimal = Decimal(0)

    @field_validator("value", "hive", "hbd", "usd", "btc", "sats", "msats", mode="before")
    @classmethod
    def convert_to_decimal(cls, v) -> Decimal | Any:
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        if isinstance(v, Decimal128):
            return Decimal(str(v))
        if isinstance(v, Decimal):
            return v
        return v

    def __init__(
        self,
        amount: Amount | AmountPyd | None = None,
        value: float | int | Decimal = Decimal(),
        conv_from: Currency | None = None,
        quote: QuoteResponse | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if (
            isinstance(amount, Amount)
            or isinstance(amount, AmountPyd)
            or ("amount" in kwargs and isinstance(kwargs["amount"], Amount))
        ):
            amount_here = kwargs.get("amount", amount)
            self.conv_from = Currency(amount_here.symbol.lower())
            self.value = Decimal(str(amount_here.amount))
            self.original = amount_here
            if isinstance(amount_here, AmountPyd):
                self.value = Decimal(amount_here.amount_decimal)
        elif conv_from:
            if isinstance(conv_from, str):
                self.conv_from = Currency(conv_from.lower())
            self.conv_from = conv_from
            self.value = Decimal(str(value))

        if quote:
            self.quote = quote
            self._compute_conversions()

    async def get_quote(self, use_cache: bool = True, store_db: bool = True) -> None:
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
        await all_quotes.get_all_quotes(use_cache=use_cache, store_db=store_db)
        self.quote = all_quotes.quote
        self._compute_conversions()
        self.fetch_date = self.quote.fetch_date

    def _compute_conversions(self):
        """Compute all currency conversions starting from msats."""
        # Step 1: Convert the input value to msats
        if self.quote is None:
            logger.warning("CryptoConversion: quote is None, cannot compute conversions")
            raise ValueError("Quote is not available or invalid")

        # Validate quote has required rates for conversion
        if self.conv_from == Currency.HIVE and self.quote.sats_hive_p == Decimal(0):
            logger.warning(
                f"CryptoConversion: sats_hive_p is 0, cannot convert from HIVE. "
                f"Quote source: {self.quote.source}"
            )
        if self.conv_from == Currency.HBD and self.quote.sats_hbd_p == Decimal(0):
            logger.warning(
                f"CryptoConversion: sats_hbd_p is 0, cannot convert from HBD. "
                f"Quote source: {self.quote.source}"
            )
        if self.conv_from == Currency.USD and self.quote.sats_usd_p == Decimal(0):
            logger.warning(
                f"CryptoConversion: sats_usd_p is 0, cannot convert from USD. "
                f"Quote source: {self.quote.source}"
            )

        try:
            if self.conv_from == Currency.MSATS:
                self.msats = self.value
            elif self.conv_from == Currency.SATS:
                self.msats = self.value * Decimal(1000)
            elif self.conv_from == Currency.HIVE:
                self.msats = Decimal(self.value) * self.quote.sats_hive_p * Decimal(1000)
            elif self.conv_from == Currency.HBD:
                self.msats = Decimal(self.value) * self.quote.sats_hbd_p * Decimal(1000)
            elif self.conv_from == Currency.USD:
                self.msats = Decimal(self.value) * self.quote.sats_usd_p * Decimal(1000)
            else:
                raise ValueError("Unsupported conversion currency")

            # Step 2: Derive sats from msats
            self.sats = self.msats / Decimal(1000)

            # Step 3: Derive all other values from msats
            self.btc = self.msats / Decimal(100_000_000_000)  # msats to BTC (1 BTC = 10^11 msats)

            # Check for zero divisors and warn before attempting division
            if self.quote.sats_usd_p == 0:
                logger.warning(
                    f"CryptoConversion: sats_usd_p is 0, USD will be 0. "
                    f"Quote source: {self.quote.source}"
                )
                self.usd = Decimal(0)
            else:
                self.usd = Decimal(
                    str(round(self.msats / (self.quote.sats_usd_p * Decimal(1000)), 6))
                )

            if self.quote.sats_hbd_p == 0:
                logger.warning(
                    f"CryptoConversion: sats_hbd_p is 0, HBD will be 0. "
                    f"Quote source: {self.quote.source}"
                )
                self.hbd = Decimal(0)
            else:
                self.hbd = Decimal(
                    str(round(self.msats / (self.quote.sats_hbd_p * Decimal(1000)), 6))
                )

            if self.quote.sats_hive_p == 0:
                logger.warning(
                    f"CryptoConversion: sats_hive_p is 0, HIVE will be 0. "
                    f"Quote source: {self.quote.source}"
                )
                self.hive = Decimal(0)
            else:
                # Use Decimal.quantize with ROUND_HALF_UP to ensure .5 always rounds up
                hive_val = self.msats / (self.quote.sats_hive_p * Decimal(1000))
                # 10 decimal places -> quantizer is 0.0000000001
                quantizer = Decimal("0.0000000001")
                self.hive = hive_val.quantize(quantizer, rounding=ROUND_HALF_UP)

            self.msats_fee = msats_fee(self.msats)
        except ZeroDivisionError as e:
            # Handle division by zero if the quote is not available
            logger.warning(
                f"CryptoConversion: ZeroDivisionError during conversion. "
                f"Quote source: {self.quote.source}, Error: {e}"
            )
            self.msats = Decimal(0)
            self.sats = Decimal(0)
            self.btc = Decimal(0)
            self.usd = Decimal(0)
            self.hbd = Decimal(0)
            self.hive = Decimal(0)
            self.msats_fee = Decimal(0)

    @property
    def conversion(self) -> CryptoConv:
        """Return a CryptoConv model with all conversion values."""
        return CryptoConv(
            hive=self.hive,
            hbd=self.hbd,
            usd=self.usd,
            sats=self.sats,
            msats=self.msats,
            msats_fee=self.msats_fee,
            btc=self.btc,
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
            Currency.HIVE: float(self.hive),
            Currency.HBD: float(self.hbd),
            Currency.USD: float(self.usd),
            Currency.SATS: float(self.sats),
            Currency.BTC: float(self.btc),
            Currency.MSATS: float(self.msats),
            "sats_hive": float(self.quote.sats_hive_p),  # type: ignore
            "sats_hbd": float(self.quote.sats_hbd_p),  # type: ignore
            "conv_from": self.conv_from,
            "value": float(self.value),
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
    print(f"Fetch date: {conv.fetch_date}")
