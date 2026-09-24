from decimal import ROUND_CEILING, Decimal
from typing import Any

DUFFS_PER_DASH = Decimal(100_000_000)
SATS_PER_BTC = Decimal(100_000_000)
MSATS_PER_SAT = Decimal(1000)
BPS_DENOM = Decimal(10_000)
ZERO = Decimal(0)
ONE = Decimal(1)


def to_decimal(value: Any) -> Decimal:
    """Parse an amount without going through binary float."""
    if isinstance(value, Decimal):
        return value
    if value is None:
        raise ValueError("amount is None")
    if isinstance(value, bool):
        raise TypeError("amount must not be a bool")
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    return Decimal(str(value))


def rpc_dash_to_duffs(amount: Any) -> Decimal:
    """dashd JSON amounts — never use float * 1e8."""
    return (to_decimal(amount) * DUFFS_PER_DASH).to_integral_value()


def dash_from_duffs(duffs: Any) -> Decimal:
    return to_decimal(duffs) / DUFFS_PER_DASH


def dash_amount_string(duffs: Any) -> str:
    return f"{dash_from_duffs(duffs):.8f}"


def msats_to_sats(msats: Any) -> Decimal:
    """Ceil millisats to whole sats so Lightning is fully covered on Dash."""
    msat = to_decimal(msats)
    if msat <= ZERO:
        return ZERO
    return (msat / MSATS_PER_SAT).to_integral_value(rounding=ROUND_CEILING)


def duffs_from_sats(sats: Any, dash_btc: Decimal) -> Decimal:
    """Always ceil so the customer cannot underpay by rounding."""
    sats_d = to_decimal(sats)
    if sats_d < ONE:
        raise ValueError("sats must be >= 1")
    if dash_btc <= ZERO:
        raise ValueError("dash_btc must be > 0")
    sats_per_dash = SATS_PER_BTC * dash_btc
    raw = (sats_d * DUFFS_PER_DASH) / sats_per_dash
    return raw.to_integral_value(rounding=ROUND_CEILING)


def json_amount(value: Decimal | None) -> int | str | None:
    """JSON-safe amount: whole numbers as int, otherwise string."""
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return format(value, "f")
