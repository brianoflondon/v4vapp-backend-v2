from datetime import datetime, timezone
from typing import Any

from bson import Int64


class BSONInt64(Int64):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value, field):
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


def convert_datetime_fields(item: dict) -> dict:
    """
    Converts timestamp fields in an item dictionary to datetime objects.

    This function checks for the presence of specific timestamp fields in the
    provided item dictionary and converts them to datetime objects using
    the `convert_timestamp_to_datetime` function. The fields that are converted
    include:
    - "creation_date"
    - "settle_date"
    - "accept_time" (within each HTLC in the "htlcs" list)
    - "resolve_time" (within each HTLC in the "htlcs" list)

    Args:
        item (dict): The item dictionary containing timestamp fields.

    Returns:
        dict: The item dictionary with the specified timestamp fields
              converted to datetime objects.
    """

    def convert_field(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return convert_timestamp_to_datetime(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
            bsonint60 = BSONInt64.validate(value, None)
            if bsonint60 > 1e12:
                timestamp = bsonint60 / 1e9
                try:
                    return convert_timestamp_to_datetime(timestamp=timestamp)
                except ValueError:
                    pass
            try:
                return convert_timestamp_to_datetime(float(value))
            except ValueError:
                pass
        return datetime.now(tz=timezone.utc)

    keys = ["creation_date", "settle_date", "creation_time_ns"]

    for key in keys:
        value = item.get(key)
        if not value or isinstance(value, datetime):
            continue
        if key == "creation_time_ns":
            value = float(value) / 1e9
        item[key] = convert_field(value)

    keys = ["accept_time", "resolve_time"]
    if "htlcs" not in item:
        return item
    for htlc in item.get("htlcs") or []:
        for key in keys:
            value = htlc.get(key)
            if not value or isinstance(value, datetime):
                continue
            htlc[key] = convert_field(value)
    return item
