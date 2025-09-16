from decimal import Decimal
from typing import Any

from bson import Decimal128


def convert_decimal128_to_decimal(value: Any) -> Any:
    """
    This function handles the conversion of Decimal128 values (commonly used in MongoDB)
    to Python Decimal objects, ensuring compatibility with Pydantic models. It processes
    nested structures like dictionaries and lists by applying the conversion recursively.

    Args:
        value (Any): The input value to convert. Can be a Decimal128, dict, list, or any other type.

    Returns:
        Any: The converted value with Decimal128 instances replaced by Decimal objects.
             Dictionaries and lists are processed recursively; other types are returned unchanged.
    """
    if isinstance(value, Decimal128):
        return Decimal(str(value))
    elif isinstance(value, dict):
        return {k: convert_decimal128_to_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [convert_decimal128_to_decimal(item) for item in value]
    else:
        return value
