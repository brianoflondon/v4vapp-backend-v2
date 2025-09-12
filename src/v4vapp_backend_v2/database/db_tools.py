from decimal import Decimal
from typing import Any

from bson import Decimal128


def _convert_decimal128_to_decimal(value: Any) -> Any:
    """Convert Decimal128 values to Decimal for arithmetic operations."""
    if isinstance(value, Decimal128):
        return Decimal(str(value))
    elif isinstance(value, dict):
        return {k: _convert_decimal128_to_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_convert_decimal128_to_decimal(item) for item in value]
    else:
        return value
