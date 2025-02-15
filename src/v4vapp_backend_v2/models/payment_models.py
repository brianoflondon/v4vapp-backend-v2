from typing import List, Optional
from pydantic import BaseModel, ConfigDict
from datetime import datetime
from bson import Int64


class BSONInt64(Int64):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, value, field):
        if isinstance(value, str):
            try:
                value = Int64(int(value))
            except ValueError:
                raise ValueError(f"Value {value} is not a valid int64")
        elif isinstance(value, int):
            value = Int64(value)
        elif not isinstance(value, Int64):
            raise TypeError(f"Value {value} is not a valid int64")
        return value


class HTLCAttempt(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    attempt_id: BSONInt64
    status: int
    attempt_time_ns: BSONInt64
    resolve_time_ns: Optional[BSONInt64] = None
    preimage: Optional[str] = None
    route: Optional[dict] = None
    failure: Optional[dict] = None


class Payment(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    payment_hash: str
    value: Optional[BSONInt64] = None
    creation_date: datetime | None = None
    fee: Optional[BSONInt64] = None
    payment_preimage: str
    value_sat: BSONInt64
    value_msat: BSONInt64
    payment_request: str
    status: str
    fee_sat: BSONInt64
    fee_msat: BSONInt64
    creation_time_ns: BSONInt64
    payment_index: BSONInt64
    failure_reason: int
    htlcs: List[HTLCAttempt]

    class Config:
        arbitrary_types_allowed = True


class ListPaymentsResponse(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    payments: List[Payment]
    first_index_offset: BSONInt64
    last_index_offset: BSONInt64
    total_num_payments: Optional[BSONInt64] = None
