from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class HtlcInfo(BaseModel):
    incoming_timelock: int | None = None
    outgoing_timelock: int | None = None
    incoming_amt_msat: int | None = None
    outgoing_amt_msat: int | None = None


class ForwardEvent(BaseModel):
    info: HtlcInfo


class ForwardFailEvent(BaseModel):
    pass


class SettleEvent(BaseModel):
    preimage: bytes | None = None


class FinalHtlcEvent(BaseModel):
    settled: bool | None = None
    offchain: bool | None = None


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
    info: HtlcInfo | None = None
    wire_failure: FailureCode | None = None
    failure_detail: FailureDetail | None = None
    failure_string: str | None = None


class EventType(StrEnum):
    UNKNOWN = "UNKNOWN"
    SEND = "SEND"
    RECEIVE = "RECEIVE"
    FORWARD = "FORWARD"


class ForwardAmtFee(BaseModel):
    forward_amount: int
    fee: float


class HtlcEvent(BaseModel):
    incoming_channel_id: int | None = None
    outgoing_channel_id: int | None = None
    incoming_htlc_id: int | None = None
    outgoing_htlc_id: int | None = None
    timestamp_ns: int | None = None
    event_type: EventType = EventType.UNKNOWN
    forward_event: ForwardEvent | None = None
    forward_fail_event: ForwardFailEvent | None = None
    settle_event: SettleEvent | None = None
    link_fail_event: LinkFailEvent | None = None
    subscribed_event: SubscribedEvent | None = None
    final_htlc_event: FinalHtlcEvent | None = None

    @property
    def timestamp(self) -> datetime | None:
        if self.timestamp_ns is not None:
            return datetime.fromtimestamp(self.timestamp_ns / 1e9)
        return None

    @property
    def is_forward_attempt(self) -> bool:
        return self.event_type == EventType.FORWARD and any(
            event
            and event.info
            and event.info.incoming_amt_msat
            and event.info.outgoing_amt_msat
            for event in (self.forward_event, self.link_fail_event)
        )

    @property
    def is_forward_fail(self) -> bool:
        if self.event_type == EventType.FORWARD and (
            self.forward_fail_event or self.link_fail_event
        ):
            return True
        return False

    @property
    def is_forward_settle(self) -> bool:
        return self.settle_event is not None and self.event_type == EventType.FORWARD

    @property
    def forward_amt_fee(self) -> ForwardAmtFee:
        """
        Calculate the forward amount and fee for an HTLC event.

        This method calculates the forward amount and fee based on the incoming and
        outgoing amounts in millisatoshis (msat) from the event information. If the
        event has a forward message, it will iterate through the forward and link
        fail events to find the relevant event information.

        Returns:
            ForwardAmtFee: An object containing the forward amount (in satoshis) and
            the fee (in satoshis).
        """
        if self.has_forward_message:
            for event in (self.forward_event, self.link_fail_event):
                if event and event.info:
                    info = event.info
                    break
            else:
                return ForwardAmtFee(forward_amount=0, fee=0)

            incoming_amt_msat = info.incoming_amt_msat or 0
            outgoing_amt_msat = info.outgoing_amt_msat or 0

            forward_amount = incoming_amt_msat // 1000
            earned: float = (incoming_amt_msat - outgoing_amt_msat) / 1000
            return ForwardAmtFee(forward_amount=forward_amount, fee=earned)
        return ForwardAmtFee(forward_amount=0, fee=0)

    @property
    def has_forward_message(self) -> bool:
        """
        Check if the event has a forward message.

        Returns:
            bool: True if the event is a forward fail, forward attempt, or
            forward settle; False otherwise.
        """
        if self.is_forward_fail or self.is_forward_attempt or self.is_forward_settle:
            return True
        return False

    def forward_message(
        self, incoming_channel_name: str, outgoing_channel_name: str
    ) -> str:
        """
        Generates a message string describing the forwarding event of an HTLC
        (Hashed TimeLock Contract).

        Args:
            incoming_channel_name (str): The name of the incoming channel.
            outgoing_channel_name (str): The name of the outgoing channel.

        Returns:
            str: A formatted string describing the forwarding event,
                 including the forward amount, fee, fee percentage, and the
                 result of the forward attempt (e.g., success, failure).
        """

        # 💰 Forwarded 222 V4VAPP Hive GoPodcasting! → WalletOfSatoshi.com. Earned 0.006 0.00% (27)
        if not self.has_forward_message:
            return ""

        try:
            fee_percent = self.forward_amt_fee.fee / self.forward_amt_fee.forward_amount
        except ZeroDivisionError:
            fee_percent = 0
        fee_ppm = fee_percent * 1_000_000

        forward_result = "Forward Attempt"
        message = f"💰 {self.incoming_htlc_id} "
        if self.forward_fail_event or self.link_fail_event:
            forward_result = "Forward Fail   "
            if self.link_fail_event:
                fee_percent = 0
                fee_ppm = 0
                forward_result = (
                    self.link_fail_event.failure_string
                    if self.link_fail_event.failure_string
                    else "Unknown"
                )
            message = (
                f"💰 {self.incoming_htlc_id} "
                f"{forward_result} {self.forward_amt_fee.forward_amount:,.0f} "
                f"{incoming_channel_name} → {outgoing_channel_name}. "
                f"Fee {self.forward_amt_fee.fee:,.3f} "
                f"{fee_percent:.2%} ({fee_ppm:,.0f})"
            )
        elif self.settle_event:
            if self.link_fail_event:
                forward_result = (
                    f"💰⭕️ Forward Fail {self.link_fail_event.failure_string} "
                )
            else:
                forward_result = "💰✅ Forward Settle "
            message = (
                f"💰 {self.incoming_htlc_id} "
                f"{forward_result} "
                f"{incoming_channel_name} → {outgoing_channel_name}. "
            )

        return message
