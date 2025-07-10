from dataclasses import dataclass
from typing import Union


@dataclass
class ConvertedSummary:
    hive: float = 0.0
    hbd: float = 0.0
    usd: float = 0.0
    sats: float = 0.0
    msats: float = 0.0

    def __add__(self, other: Union["ConvertedSummary", float, int]) -> "ConvertedSummary":
        """Add two ConvertedSummary instances or a scalar to all fields."""
        if isinstance(other, (float, int)):
            return ConvertedSummary(
                hive=self.hive + other,
                hbd=self.hbd + other,
                usd=self.usd + other,
                sats=self.sats + other,
                msats=self.msats + other,
            )
        elif isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive + other.hive,
                hbd=self.hbd + other.hbd,
                usd=self.usd + other.usd,
                sats=self.sats + other.sats,
                msats=self.msats + other.msats,
            )
        raise TypeError(f"Unsupported operand type for +: '{type(other)}'")

    def __sub__(self, other: Union["ConvertedSummary", float, int]) -> "ConvertedSummary":
        """Subtract a ConvertedSummary instance or a scalar from all fields."""
        if isinstance(other, (float, int)):
            return ConvertedSummary(
                hive=self.hive - other,
                hbd=self.hbd - other,
                usd=self.usd - other,
                sats=self.sats - other,
                msats=self.msats - other,
            )
        elif isinstance(other, ConvertedSummary):
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
            return ConvertedSummary(
                hive=self.hive * other,
                hbd=self.hbd * other,
                usd=self.usd * other,
                sats=self.sats * other,
                msats=self.msats * other,
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

    def __radd__(self, other: Union[float, int]) -> "ConvertedSummary":
        """Support addition when the scalar is on the left (e.g., 2 + obj)."""
        return self.__add__(other)

    def __rsub__(self, other: Union[float, int]) -> "ConvertedSummary":
        """Support subtraction when the scalar is on the left (e.g., 2 - obj)."""
        if isinstance(other, (float, int)):
            return ConvertedSummary(
                hive=other - self.hive,
                hbd=other - self.hbd,
                usd=other - self.usd,
                sats=other - self.sats,
                msats=other - self.msats,
            )
        raise TypeError(f"Unsupported operand type for -: '{type(other)}'")

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
            if other == 0:
                raise ZeroDivisionError("Division by zero")
            return ConvertedSummary(
                hive=self.hive / other,
                hbd=self.hbd / other,
                usd=self.usd / other,
                sats=self.sats / other,
                msats=self.msats / other,
            )
        elif isinstance(other, ConvertedSummary):
            return ConvertedSummary(
                hive=self.hive / other.hive if other.hive != 0 else float("inf"),
                hbd=self.hbd / other.hbd if other.hbd != 0 else float("inf"),
                usd=self.usd / other.usd if other.usd != 0 else float("inf"),
                sats=self.sats / other.sats if other.sats != 0 else float("inf"),
                msats=self.msats / other.msats if other.msats != 0 else float("inf"),
            )
        raise TypeError(f"Unsupported operand type for /: '{type(other)}'")
