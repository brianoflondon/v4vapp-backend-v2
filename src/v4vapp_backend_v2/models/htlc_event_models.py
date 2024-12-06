from datetime import datetime
from enum import StrEnum
from typing import Optional

from pydantic import BaseModel


class HtlcInfo(BaseModel):
    incoming_timelock: int | None = None
    outgoing_timelock: int | None = None
    incoming_amt_msat: str | None = None
    outgoing_amt_msat: str | None = None


class ForwardEvent(BaseModel):
    info: HtlcInfo


class ForwardFailEvent(BaseModel):
    pass


class SettleEvent(BaseModel):
    preimage: bytes


class FinalHtlcEvent(BaseModel):
    settled: bool
    offchain: bool


class SubscribedEvent(BaseModel):
    pass


class FailureDetail(StrEnum):
    UNKNOWN = "UNKNOWN"
    NO_DETAIL = "NO_DETAIL"
    ONION_DECODE = "ONION_DECODE"
    LINK_NOT_ELIGIBLE = "LINK_NOT_ELIGIBLE"
    ON_CHAIN_TIMEOUT = "ON_CHAIN_TIMEOUT"
    HTLC_EXCEEDS_MAX = "HTLC_EXCEEDS_MAX"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    INCOMPLETE_FORWARD = "INCOMPLETE_FORWARD"
    HTLC_ADD_FAILED = "HTLC_ADD_FAILED"
    FORWARDS_DISABLED = "FORWARDS_DISABLED"
    INVOICE_CANCELED = "INVOICE_CANCELED"
    INVOICE_UNDERPAID = "INVOICE_UNDERPAID"
    INVOICE_EXPIRY_TOO_SOON = "INVOICE_EXPIRY_TOO_SOON"
    INVOICE_NOT_OPEN = "INVOICE_NOT_OPEN"
    MPP_INVOICE_TIMEOUT = "MPP_INVOICE_TIMEOUT"
    ADDRESS_MISMATCH = "ADDRESS_MISMATCH"
    SET_TOTAL_MISMATCH = "SET_TOTAL_MISMATCH"
    SET_TOTAL_TOO_LOW = "SET_TOTAL_TOO_LOW"
    SET_OVERPAID = "SET_OVERPAID"
    UNKNOWN_INVOICE = "UNKNOWN_INVOICE"
    INVALID_KEYSEND = "INVALID_KEYSEND"
    MPP_IN_PROGRESS = "MPP_IN_PROGRESS"
    CIRCULAR_ROUTE = "CIRCULAR_ROUTE"


class FailureCode(StrEnum):
    RESERVED = "RESERVED"
    INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS = "INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS"
    INCORRECT_PAYMENT_AMOUNT = "INCORRECT_PAYMENT_AMOUNT"
    FINAL_INCORRECT_CLTV_EXPIRY = "FINAL_INCORRECT_CLTV_EXPIRY"
    FINAL_INCORRECT_HTLC_AMOUNT = "FINAL_INCORRECT_HTLC_AMOUNT"
    FINAL_EXPIRY_TOO_SOON = "FINAL_EXPIRY_TOO_SOON"
    INVALID_REALM = "INVALID_REALM"
    EXPIRY_TOO_SOON = "EXPIRY_TOO_SOON"
    INVALID_ONION_VERSION = "INVALID_ONION_VERSION"
    INVALID_ONION_HMAC = "INVALID_ONION_HMAC"
    INVALID_ONION_KEY = "INVALID_ONION_KEY"
    AMOUNT_BELOW_MINIMUM = "AMOUNT_BELOW_MINIMUM"
    FEE_INSUFFICIENT = "FEE_INSUFFICIENT"
    INCORRECT_CLTV_EXPIRY = "INCORRECT_CLTV_EXPIRY"
    CHANNEL_DISABLED = "CHANNEL_DISABLED"
    TEMPORARY_CHANNEL_FAILURE = "TEMPORARY_CHANNEL_FAILURE"
    REQUIRED_NODE_FEATURE_MISSING = "REQUIRED_NODE_FEATURE_MISSING"
    REQUIRED_CHANNEL_FEATURE_MISSING = "REQUIRED_CHANNEL_FEATURE_MISSING"
    UNKNOWN_NEXT_PEER = "UNKNOWN_NEXT_PEER"
    TEMPORARY_NODE_FAILURE = "TEMPORARY_NODE_FAILURE"
    PERMANENT_NODE_FAILURE = "PERMANENT_NODE_FAILURE"
    PERMANENT_CHANNEL_FAILURE = "PERMANENT_CHANNEL_FAILURE"
    EXPIRY_TOO_FAR = "EXPIRY_TOO_FAR"
    MPP_TIMEOUT = "MPP_TIMEOUT"


class LinkFailEvent(BaseModel):
    info: HtlcInfo
    wire_failure: FailureCode
    failure_detail: FailureDetail
    failure_string: str


class EventType(StrEnum):
    UNKNOWN = "UNKNOWN"
    SEND = "SEND"
    RECEIVE = "RECEIVE"
    FORWARD = "FORWARD"


class HtlcEvent(BaseModel):
    incoming_channel_id: int | None = None
    outgoing_channel_id: int | None = None
    incoming_htlc_id: int | None = None
    outgoing_htlc_id: int | None = None
    timestamp_ns: int | None = None
    event_type: Optional[EventType] = None
    forward_event: Optional[ForwardEvent] = None
    forward_fail_event: Optional[ForwardFailEvent] = None
    settle_event: Optional[SettleEvent] = None
    link_fail_event: Optional[LinkFailEvent] = None
    subscribed_event: Optional[SubscribedEvent] = None
    final_htlc_event: Optional[FinalHtlcEvent] = None

    @property
    def timestamp(self) -> Optional[datetime]:
        if self.timestamp_ns is not None:
            return datetime.fromtimestamp(self.timestamp_ns / 1e9)
        return None

    @property
    def forward_amt_earned(self) -> str:
        if (
            self.forward_event
            and self.forward_event.info
            and self.forward_event.info.incoming_amt_msat
            and self.forward_event.info.outgoing_amt_msat
        ):
            forward_amount = int(self.forward_event.info.incoming_amt_msat) // 1000
            earned: float = (
                int(self.forward_event.info.incoming_amt_msat)
                - int(self.forward_event.info.outgoing_amt_msat)
            ) / 1000
            return f"{forward_amount:,.0f} sats (earned: {earned:,.3f})"
        return ""
