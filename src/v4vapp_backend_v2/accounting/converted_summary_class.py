from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal
from typing import Union

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv


@dataclass
class ConvertedSummary:
    hive: Decimal = Decimal(0)
    hbd: Decimal = Decimal(0)
    usd: Decimal = Decimal(0)
    sats: Decimal = Decimal(0)
    msats: Decimal = Decimal(0)

    @property
    def sats_rounded(self) -> Decimal:
        """Return sats rounded to the nearest integer."""
        return self.sats.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

    @classmethod
    def from_crypto_conv(cls, crypto_conv: CryptoConv) -> "ConvertedSummary":
        """Create a ConvertedSummary from a CryptoConv object."""
        return cls(
            hive=crypto_conv.hive,
            hbd=crypto_conv.hbd,
            usd=crypto_conv.usd,
            sats=crypto_conv.sats,
            msats=crypto_conv.msats,
        )

    def __add__(self, other: "ConvertedSummary") -> "ConvertedSummary":
        """Add two ConvertedSummary instances or a scalar to all fields."""
        if isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive + other.hive,
                hbd=self.hbd + other.hbd,
                usd=self.usd + other.usd,
                sats=self.sats + other.sats,
                msats=self.msats + other.msats,
            )
        raise TypeError(f"Unsupported operand type for +: '{type(other)}'")

    def __sub__(self, other: "ConvertedSummary") -> "ConvertedSummary":
        """Subtract a ConvertedSummary instance or a scalar from all fields."""
        if isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive - other.hive,
                hbd=self.hbd - other.hbd,
                usd=self.usd - other.usd,
                sats=self.sats - other.sats,
                msats=self.msats - other.msats,
            )
        raise TypeError(f"Unsupported operand type for -: '{type(other)}'")

    def __mul__(self, other: Union["ConvertedSummary", float, int]) -> "ConvertedSummary":
        """Multiply by a ConvertedSummary instance or a scalar for all fields."""
        if isinstance(other, (float, int)):
            other_dec = Decimal(other)
            return ConvertedSummary(
                hive=self.hive * other_dec,
                hbd=self.hbd * other_dec,
                usd=self.usd * other_dec,
                sats=self.sats * other_dec,
                msats=self.msats * other_dec,
            )
        elif isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive * other.hive,
                hbd=self.hbd * other.hbd,
                usd=self.usd * other.usd,
                sats=self.sats * other.sats,
                msats=self.msats * other.msats,
            )
        raise TypeError(f"Unsupported operand type for *: '{type(other)}'")

    def __eq__(self, other: object) -> bool:
        """Check equality with another ConvertedSummary instance."""
        if not isinstance(other, ConvertedSummary):
            return False
        return (
            abs(self.hive - other.hive) < 1e-10
            and abs(self.hbd - other.hbd) < 1e-10
            and abs(self.usd - other.usd) < 1e-10
            and abs(self.sats - other.sats) < 1e-10
            and abs(self.msats - other.msats) < 1e-10
        )

    def __rmul__(self, other: Union[float, int]) -> "ConvertedSummary":
        """Support multiplication when the scalar is on the left (e.g., 2 * obj)."""
        return self.__mul__(other)

    def __neg__(self) -> "ConvertedSummary":
        """Support unary negation (e.g., -obj)."""
        return ConvertedSummary(
            hive=-self.hive, hbd=-self.hbd, usd=-self.usd, sats=-self.sats, msats=-self.msats
        )

    def __truediv__(self, other: Union["ConvertedSummary", float, int]) -> "ConvertedSummary":
        """Divide by a ConvertedSummary instance or a scalar for all fields."""
        if isinstance(other, (float, int)):
            other_dec = Decimal(other)
            if other_dec == 0:
                raise ZeroDivisionError("Division by zero")
            return ConvertedSummary(
                hive=self.hive / other_dec,
                hbd=self.hbd / other_dec,
                usd=self.usd / other_dec,
                sats=self.sats / other_dec,
                msats=self.msats / other_dec,
            )
        elif isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive / other.hive if other.hive != 0 else Decimal("inf"),
                hbd=self.hbd / other.hbd if other.hbd != 0 else Decimal("inf"),
                usd=self.usd / other.usd if other.usd != 0 else Decimal("inf"),
                sats=self.sats / other.sats if other.sats != 0 else Decimal("inf"),
                msats=self.msats / other.msats if other.msats != 0 else Decimal("inf"),
            )
        raise TypeError(f"Unsupported operand type for /: '{type(other)}'")
