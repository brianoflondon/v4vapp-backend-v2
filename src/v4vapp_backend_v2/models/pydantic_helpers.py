from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, List

from bson import Decimal128, Int64
from pydantic import GetCoreSchemaHandler, ValidationInfo
from pydantic_core import CoreSchema, core_schema


class BSONInt64(Int64):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.with_info_plain_validator_function(
            cls.validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: int(x),  # Serialize to int for JSON compatibility
                when_used="json",
            ),
        )

    @classmethod
    def validate(cls, value: Any, info: ValidationInfo) -> Int64:
        if isinstance(value, str):
            try:
                value = Int64(value)
            except ValueError:
                raise ValueError(f"Value {value} is not a valid Int64")
        elif isinstance(value, int):
            value = Int64(value)
        elif not isinstance(value, Int64):
            raise TypeError(f"Value {value} is not a valid Int64")

        # Check if the value is within the 64-bit integer range
        if not (-(2**63) <= value < 2**63):
            raise ValueError(f"Value {value} exceeds 64-bit signed integer range")

        return value


def convert_timestamp_to_datetime(timestamp: int | float) -> datetime:
    """
    Convert a Unix timestamp to a timezone-aware datetime object.

    Args:
        timestamp (float or int): The Unix timestamp to convert.

    Returns:
        datetime: A timezone-aware datetime object in UTC.

    Raises:
        ValueError: If the timestamp cannot be converted to a float.
    """
    return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)


def convert_datetime_fields(
    item: dict[str, Any] | List[dict[str, Any]],
) -> dict[str, Any] | List[dict[str, Any]]:
    """
    Converts timestamp fields and Decimal128 fields in an item dictionary to datetime and Decimal objects.

    This function checks for the presence of specific timestamp fields in the
    provided item dictionary and converts them to datetime objects using
    the `convert_timestamp_to_datetime` function. It also converts Decimal128
    fields to Decimal objects. The fields that are converted include:
    - "creation_date"
    - "settle_date"
    - "accept_time" (within each HTLC in the "htlcs" list)
    - "resolve_time" (within each HTLC in the "htlcs" list)
    - "fetch_time" (within each HTLC in the "htlcs" list)
    - "creation_time_ns" (converted from nanoseconds to seconds)
    - "resolve_time_ns" (converted from nanoseconds to seconds)
    - "attempt_time_ns" (converted from nanoseconds to seconds)
    - Any Decimal128 values are converted to Decimal

    Args:
        item (dict): The item dictionary containing timestamp and Decimal128 fields.

    Returns:
        dict: The item dictionary with the specified timestamp fields
              converted to datetime objects and Decimal128 to Decimal.
    """

    def convert_field(value: Any) -> datetime:
        if isinstance(value, datetime):
            # Always return as UTC tz-aware
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value
        if isinstance(value, (int, float)):
            return convert_timestamp_to_datetime(value)
        if isinstance(value, str):
            try:
                # Parse ISO string and force UTC
                dt = datetime.fromisoformat(value)
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except ValueError:
                pass
            try:
                bsonint64 = BSONInt64.validate(value, None)  # type: ignore
                if bsonint64 > 1e12:
                    timestamp = bsonint64 / 1e9
                    try:
                        return convert_timestamp_to_datetime(timestamp=timestamp)
                    except ValueError:
                        pass
            except (ValueError, TypeError):
                pass
            try:
                return convert_timestamp_to_datetime(float(value))
            except ValueError:
                pass
        # Always return a UTC tz-aware datetime as fallback
        return datetime.now(tz=timezone.utc)

    def convert_value(value: Any) -> Any:
        if isinstance(value, Decimal128):
            return Decimal(str(value))
        elif isinstance(value, dict):
            return convert_datetime_fields(value)
        elif isinstance(value, list):
            return [convert_value(v) for v in value]
        else:
            return value

    if isinstance(item, list):
        return [convert_datetime_fields(i) for i in item]  # type: ignore

    # Convert Decimal128 fields recursively
    for key, value in item.items():
        item[key] = convert_value(value)

    keys = [
        "creation_date",
        "settle_date",
        "creation_time_ns",
        "resolve_time_ns",
        "attempt_time_ns",
        "fetch_date",
    ]
    for key in keys:
        value = item.get(key)
        if not value:
            continue
        if isinstance(value, datetime):
            # Ensure datetime is UTC tz-aware
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            continue
        if key in ["creation_time_ns", "resolve_time_ns", "attempt_time_ns"]:
            value = float(value) / 1e9
        item[key] = convert_field(value)

    keys = ["accept_time", "resolve_time"]
    if "htlcs" not in item:
        return item
    for htlc in item.get("htlcs") or []:
        for key in keys:
            value = htlc.get(key)
            if not value:
                continue
            htlc[key] = convert_field(value)
    return item
