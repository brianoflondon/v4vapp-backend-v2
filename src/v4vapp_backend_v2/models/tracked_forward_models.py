from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from time import time_ns
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.results import UpdateResult

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb


class FinalHtlcEvent(BaseModel):
    settled: bool | None = None
    offchain: bool | None = None


class HtlcInfo(BaseModel):
    incoming_timelock: Decimal | None = None
    outgoing_timelock: Decimal | None = None
    incoming_amt_msat: Decimal | None = None
    outgoing_amt_msat: Decimal | None = None

    @field_validator(
        "incoming_timelock",
        "outgoing_timelock",
        "incoming_amt_msat",
        "outgoing_amt_msat",
        mode="before",
    )
    @classmethod
    def _to_decimal(cls, v: Any) -> Any:
        # Accept dicts from Mongo like {'$numberDecimal': '...'}, ints or strings
        if isinstance(v, dict):
            if "$numberDecimal" in v:
                return Decimal(v["$numberDecimal"])
            if "$numberLong" in v:
                return Decimal(v["$numberLong"])
        if isinstance(v, (int, Decimal)):
            return Decimal(v)
        if isinstance(v, str) and v != "":
            return Decimal(v)
        return v


class ForwardEvent(BaseModel):
    info: HtlcInfo | None = None


class ForwardFailEvent(BaseModel):
    # Intentionally minimal - protobuf has no fields on this message
    pass


class SettleEvent(BaseModel):
    preimage: str | None = None

    @field_validator("preimage", mode="before")
    @classmethod
    def _preimage_to_hex(cls, v: Any) -> Any:
        # Accept bytes -> hex string, or keep string
        if isinstance(v, (bytes, bytearray)):
            return v.hex()
        return v


class LinkFailEvent(BaseModel):
    info: HtlcInfo | None = None
    wire_failure: int | None = None
    failure_detail: int | None = None
    failure_string: str | None = None


class SubscribedEvent(BaseModel):
    pass


class HtlcEventDict(BaseModel):
    incoming_channel_id: str | None = None
    outgoing_channel_id: str | None = None
    incoming_htlc_id: str | None = None
    outgoing_htlc_id: str | None = None
    timestamp_ns: Decimal | None = None
    event_type: int | str | None = None

    forward_event: ForwardEvent | None = None
    forward_fail_event: ForwardFailEvent | None = None
    settle_event: SettleEvent | None = None
    link_fail_event: LinkFailEvent | None = None
    subscribed_event: SubscribedEvent | None = None
    final_htlc_event: FinalHtlcEvent | None = None

    @field_validator(
        "incoming_channel_id",
        "outgoing_channel_id",
        "incoming_htlc_id",
        "outgoing_htlc_id",
        mode="before",
    )
    @classmethod
    def _id_to_str(cls, v: Any) -> Any:
        # Accept ints or decimals and convert to string for stable handling
        if isinstance(v, (int, Decimal)):
            return str(v)
        if isinstance(v, dict):
            # Accept mongo style long {'$numberLong': '...'}
            if "$numberLong" in v:
                return v["$numberLong"]
            if "$numberInt" in v:
                return str(v["$numberInt"])
        return v

    @field_validator("timestamp_ns", mode="before")
    @classmethod
    def _timestamp_to_decimal(cls, v: Any) -> Any:
        # Accept Mongo style, ints or string
        if isinstance(v, dict):
            if "$numberLong" in v:
                return Decimal(v["$numberLong"])
            if "$numberDecimal" in v:
                return Decimal(v["$numberDecimal"])
        if isinstance(v, (int, Decimal)):
            return Decimal(v)
        if isinstance(v, str) and v != "":
            return Decimal(v)
        return v


class TrackedForwardEvent(BaseModel):
    """Pydantic model for HTLC Event forward notification documents.

    This model accepts BSON-style MongoDB JSON representations (e.g. {"$numberDecimal": "..."}
    for decimals and {"$date": "...Z"} for timestamps) and normalizes them to standard
    Python types: Decimal for amounts/fees, datetime for timestamps and str for object ids.
    """

    id: str | None = Field(None, alias="_id")
    htlc_id: int
    group_id: str | None = None
    message_type: str
    message: str | None = None
    from_channel: str | None = None
    to_channel: str | None = None
    amount: Decimal | None = None
    fee: Decimal | None = None
    fee_percent: Decimal | None = None
    fee_ppm: int | None = None
    htlc_event_dict: HtlcEventDict | None = None
    notification: bool = False
    silent: bool = False
    timestamp: datetime | None = None
    process_time: float | None = Field(
        None, description="Time in (s) it took to process this transaction"
    )

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    @classmethod
    def collection_name(cls) -> str:
        return "htlc_events"

    @classmethod
    def collection(cls) -> AsyncCollection:
        """
        Returns the collection associated with this model.

        Returns:
            AsyncCollection: The collection object for this model.
        """
        return InternalConfig.db["htlc_events"]

    async def save(self) -> UpdateResult:
        """
        Saves the current instance to the database.

        This method inserts or updates the document in the database collection
        associated with this model.
        """
        collection = self.collection()
        update = self.model_dump(
            exclude_unset=True,
            exclude_none=True,
            by_alias=True,
        )

        # Convert Decimal objects to floats for MongoDB compatibility
        update = convert_decimals_for_mongodb(update)

        update = {
            "$set": update,
        }
        # Delegate retries and logging to the wrapper
        return await mongo_call(
            lambda: self.collection().update_one(
                filter=self.group_id_query,
                update=update,
                upsert=True,
            ),
            error_code=f"db_save_error_{self.collection_name}",
            context=f"{self.collection_name}:{self.group_id_p}",
        )

    @property
    def op_type(self) -> str:
        """
        Returns the operation type for this tracked forward event.

        Returns:
            str: The operation type.
        """
        return self.message_type or "FORWARD"

    @property
    def group_id_p(self) -> str:
        """
        Returns the group ID for this tracked forward event.

        Returns:
            str: The group ID.
        """
        return self.group_id or ""

    @computed_field
    def short_id(self) -> str:
        """
        Returns a short identifier for this tracked forward event.

        Returns:
            str: The short identifier.
        """
        return f"{self.htlc_id}"

    @property
    def short_id_p(self) -> str:
        """
        Returns a short identifier for this tracked forward event.

        Returns:
            str: The short identifier.
        """
        return f"{self.htlc_id}"

    @property
    def log_str(self) -> str:
        """
        Returns a string representation for logging purposes.

        Returns:
            str: The log string.
        """
        if self.message:
            return self.message
        return f"{self.message_type} HTLC {self.htlc_id}"

    @property
    def log_extra(self) -> Dict[str, Any]:
        """
        Returns extra logging information as a dictionary.

        Returns:
            dict: The extra logging information.
        """
        return {"tracked_forward_event": self.model_dump(exclude_unset=True)}

    @property
    def group_id_query(self) -> Dict[str, Any]:
        """
        Returns the query used to identify the group ID in the database.

        Returns:
            dict: The query used to identify the group ID.
        """
        return {"group_id": self.group_id}

    @field_validator("id", mode="before")
    @classmethod
    def _parse_id(cls, v: Any) -> Any:
        # Accept {'$oid': '...'} or raw str
        if isinstance(v, dict) and "$oid" in v:
            return v["$oid"]
        return v

    @field_validator("amount", "fee", "fee_percent", mode="before")
    @classmethod
    def _parse_decimal(cls, v: Any) -> Any:
        # Accept Mongo style {"$numberDecimal": "..."}, strings or Decimal
        if isinstance(v, dict) and "$numberDecimal" in v:
            return Decimal(v["$numberDecimal"])
        if isinstance(v, str):
            return Decimal(v)
        return v

    @field_validator("timestamp", mode="before")
    @classmethod
    def _parse_timestamp(cls, v: Any) -> Any:
        # Accept Mongo style {"$date": "ISOZ"} or a datetime instance
        if isinstance(v, dict) and "$date" in v:
            # Convert trailing Z to +00:00 for fromisoformat
            iso = v["$date"]
            if iso.endswith("Z"):
                iso = iso[:-1] + "+00:00"
            return datetime.fromisoformat(iso)
        return v

    @model_validator(mode="before")
    @classmethod
    def _normalize_message_key(cls, values: dict[str, Any]) -> dict[str, Any]:
        # Some JSON dumps include a key named "message:" (with a trailing colon).
        # Normalize that into the `message` field so both styles are supported.
        if "message" not in values and "message:" in values:
            values["message"] = values.pop("message:")
        return values

    def __init__(self, **data: Any):
        super().__init__(**data)
        if not self.group_id:
            timestamp_ns = (
                self.htlc_event_dict.timestamp_ns
                if self.htlc_event_dict and self.htlc_event_dict.timestamp_ns
                else time_ns()
            )
            self.group_id = f"forward-{self.htlc_id}-{timestamp_ns}"


__all__ = ["TrackedForwardEvent", "HtlcEventDict", "FinalHtlcEvent"]
