import pytest
from bson import Int64

from v4vapp_backend_v2.models.pydantic_helpers import BSONInt64


def test_bsonint64_from_string():
    value = "1234567890123456789"
    bson_int64 = BSONInt64.validate(value, None)
    assert isinstance(bson_int64, Int64)
    assert bson_int64 == Int64(1234567890123456789)


def test_bsonint64_from_int():
    value = 1234567890123456789
    bson_int64 = BSONInt64.validate(value, None)
    assert isinstance(bson_int64, Int64)
    assert bson_int64 == Int64(1234567890123456789)


def test_bsonint64_from_int64():
    value = Int64(1234567890123456789)
    bson_int64 = BSONInt64.validate(value, None)
    assert isinstance(bson_int64, Int64)
    assert bson_int64 == value


def test_bsonint64_invalid_string():
    value = "invalid_int64"
    with pytest.raises(ValueError, match="Value invalid_int64 is not a valid Int64"):
        BSONInt64.validate(value, None)


def test_bsonint64_invalid_type():
    value = 123.456
    with pytest.raises(TypeError, match="Value 123.456 is not a valid Int64"):
        BSONInt64.validate(value, None)


def test_bsonint64_none():
    value = None
    with pytest.raises(TypeError, match="Value None is not a valid Int64"):
        BSONInt64.validate(value, None)
