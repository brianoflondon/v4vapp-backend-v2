from datetime import datetime, timezone
from enum import StrEnum
from typing import Dict, List

from pydantic import BaseModel

from v4vapp_backend_v2.config import LoggerFunction
from v4vapp_backend_v2.models.lnd_models import LNDInvoice


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
    forward_amount: float
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
    def htlc_id(self) -> int | None:
        """
        Returns the HTLC (Hashed Time-Locked Contract) ID.

        This method returns the incoming HTLC ID if it exists; otherwise, it returns the outgoing HTLC ID.
        If neither exists, it returns None.

        Returns:
            int | None: The HTLC ID, or None if neither incoming nor outgoing HTLC ID is present.
        """
        return self.incoming_htlc_id or self.outgoing_htlc_id

    @property
    def timestamp(self) -> datetime | None:
        """
        Converts the timestamp in nanoseconds to a datetime object.

        Returns:
            datetime | None: A datetime object representing the timestamp if `timestamp_ns` is not None,
                             otherwise None.
        """
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
        if self.is_forward_fail_attempt_settle:
            for event in (self.forward_event, self.link_fail_event):
                if event and event.info:
                    info = event.info
                    break
            else:
                return ForwardAmtFee(forward_amount=0, fee=0)

            incoming_amt_msat = info.incoming_amt_msat or 0
            outgoing_amt_msat = info.outgoing_amt_msat or 0

            forward_amount = outgoing_amt_msat / 1000
            earned: float = (incoming_amt_msat - outgoing_amt_msat) / 1000
            return ForwardAmtFee(forward_amount=forward_amount, fee=earned)
        return ForwardAmtFee(forward_amount=0, fee=0)

    @property
    def is_forward_fail_attempt_settle(self) -> bool:
        """
        Check if the event has a forward message.

        Returns:
            bool: True if the event is a forward fail, forward attempt, or
            forward settle; False otherwise.
        """
        if self.is_forward_fail or self.is_forward_attempt or self.is_forward_settle:
            return True
        return False


class ChannelName(BaseModel):
    channel_id: int
    name: str


class HtlcTrackingList(BaseModel):
    events: list[HtlcEvent] = []
    names: Dict[int, str] = {}
    invoices: list[LNDInvoice] = []

    def add_event(self, event: HtlcEvent) -> int:
        htlc_id = event.htlc_id
        if htlc_id is None:
            return -1
        self.events.append(event)
        return htlc_id

    def add_invoice(self, invoice: LNDInvoice) -> int:
        add_index = invoice.add_index
        if add_index is None:
            return -1
        if self.lookup_invoice(add_index):
            self.remove_invoice(add_index)
        self.invoices.append(invoice)
        return add_index

    def remove_invoice(self, add_index: int) -> None:
        if not self.lookup_invoice(add_index):
            return
        self.invoices = [
            invoice for invoice in self.invoices if invoice.add_index != add_index
        ]

    def remove_expired_invoices(self) -> None:
        def check_expiry(invoice: LNDInvoice) -> bool:
            current_time = int(datetime.now(tz=timezone.utc).timestamp())
            return current_time > invoice.creation_date.timestamp() + (
                invoice.expiry or 300
            )

        for invoice in self.invoices:
            if check_expiry(invoice):
                self.remove_invoice(invoice.add_index)

    def lookup_invoice(self, add_index: int) -> LNDInvoice | None:
        for invoice in self.invoices:
            if invoice.add_index == add_index:
                return invoice
        return None

    def lookup_invoice_by_htlc_id(self, htlc_id: int) -> LNDInvoice | None:
        for invoice in self.invoices:
            if invoice and invoice.htlcs:
                for htlc_data in invoice.htlcs:
                    if int(htlc_data["htlc_index"]) == int(htlc_id):
                        return invoice
        return None

    def invoice_htlc_id(self, add_index: int) -> int | None:
        invoice = self.lookup_invoice(add_index)
        if invoice and invoice.htlcs:
            for htlc in invoice.htlcs:
                return htlc["htlc_index"]
        return None

    @property
    def num_invoices(self) -> int:
        return len(self.invoices)

    def add_name(self, channel_name: ChannelName) -> None:
        self.names[channel_name.channel_id] = channel_name.name

    @property
    def num_events(self) -> int:
        return len(self.events)

    def list_htlc_id(self, htlc_id: int) -> List[HtlcEvent]:
        """Returns a list of events with the given htlc_id."""
        return [
            event
            for event in self.events
            if event.incoming_htlc_id == htlc_id or event.outgoing_htlc_id == htlc_id
        ]

    def list_all_htlc_ids(self) -> List[int]:
        """Returns a list of all htlc_ids in the events."""
        return list(
            {
                event.incoming_htlc_id
                for event in self.events
                if event.incoming_htlc_id is not None
            }
            | {
                event.outgoing_htlc_id
                for event in self.events
                if event.outgoing_htlc_id is not None
            }
        )

    def delete_event(self, htlc_id: int) -> None:
        self.events = [
            event
            for event in self.events
            if event.incoming_htlc_id != htlc_id and event.outgoing_htlc_id != htlc_id
        ]

    def complete_group(self, htlc_id: int) -> bool:
        group_list = self.list_htlc_id(htlc_id)
        if group_list:
            match group_list[0].event_type:
                case EventType.FORWARD:
                    if len(group_list) == 3:
                        return True
                    if len(group_list) == 2:
                        has_forward_event = any(
                            event.event_type == EventType.FORWARD
                            and event.link_fail_event
                            for event in group_list
                        )
                        has_unknown_event = any(
                            event.event_type == EventType.UNKNOWN
                            and event.final_htlc_event
                            for event in group_list
                        )
                        if has_forward_event and has_unknown_event:
                            for event in group_list:
                                if event.final_htlc_event:
                                    event.final_htlc_event.settled = True
                            return True
                    return False
                    return True if len(group_list) == 3 else False
                case EventType.SEND:
                    return True if len(group_list) == 2 else False
                case EventType.RECEIVE:
                    return True if len(group_list) == 2 else False
                case _:
                    return True

        return False

    def message(self, htlc_id: int) -> str:
        if htlc_id is None or htlc_id < 0:
            return "no message"
        group_list = self.list_htlc_id(htlc_id)
        if group_list:
            match group_list[0].event_type:
                case EventType.FORWARD:
                    if self.complete_group(htlc_id):
                        message_str = self.forward_message(group_list)
                    else:
                        message_str = f"💰 Forward in progress {htlc_id}"
                    return message_str
                case EventType.SEND:
                    if self.complete_group(htlc_id):
                        message_str = self.send_message(group_list)
                    else:
                        message_str = f"⚡️ Send in progress {htlc_id}"
                    return message_str
                case EventType.RECEIVE:
                    if self.complete_group(htlc_id):
                        message_str = self.receive_message(group_list)
                    else:
                        message_str = f"💵 Receive in progress {htlc_id}"
                    return message_str
                case _:
                    return "Unknown"
        return "no message"

    def log_event(
        self, htlc_id: int, logger_func: LoggerFunction, extra: dict = {}
    ) -> None:
        """
        Logs an event message for a given HTLC (Hashed TimeLock Contract) ID.

        Args:
            htlc_id (int): The ID of the HTLC event to log.
            logger_func (LoggerFunction): The logging function to use for logging the
                event message.
            extra (dict, optional): Additional context or metadata to include in the
                log. Defaults to an empty dictionary.
                logger_func must take extra as a keyword argument if extra is passed.

        Returns:
            None
        """
        message_str = self.message(htlc_id)
        if extra:
            logger_func(message_str, extra=extra)
        else:
            logger_func(message_str)

    def send_message(self, group_list: List[HtlcEvent]) -> str:
        end_message = "✅ Settled"
        primary_event = group_list[0]
        if (
            primary_event.forward_event
            and primary_event.forward_event.info
            and primary_event.forward_event.info.outgoing_amt_msat
        ):
            amount = primary_event.forward_event.info.outgoing_amt_msat / 1000
        else:
            amount = 0

        if primary_event.outgoing_channel_id:
            sent_via = self.lookup_name(primary_event.outgoing_channel_id)
        else:
            sent_via = "Unknown"

        message_str = f"⚡️ Sent {amount:,.0f} " f"out {sent_via}. " f"{end_message}"
        return message_str

    def receive_message(self, group_list: List[HtlcEvent]) -> str:
        primary_event = group_list[0]
        amount = 0
        if primary_event.incoming_channel_id:
            received_via = self.lookup_name(primary_event.incoming_channel_id)
        else:
            received_via = "Unknown"

        for_memo = ""
        htlc_id = primary_event.htlc_id
        if htlc_id:
            incoming_invoice = self.lookup_invoice_by_htlc_id(htlc_id=htlc_id)
            if incoming_invoice:
                amount = incoming_invoice.value
                for_memo = (
                    f" for {incoming_invoice.memo}" if incoming_invoice.memo else ""
                )
                self.remove_invoice(incoming_invoice.add_index)

        message_str = f"💵 Received {amount:,.0f}{for_memo}" f" via {received_via}"
        return message_str

    def forward_message(self, group_list: List[HtlcEvent]) -> str:
        primary_event = group_list[0]
        start_message = "💰 Attempted "
        if len(group_list) == 2:
            if primary_event.link_fail_event:
                if (
                    primary_event.link_fail_event.info
                    and primary_event.link_fail_event.info.incoming_amt_msat
                ):
                    amount = primary_event.link_fail_event.info.incoming_amt_msat / 1000
                else:
                    amount = 0
                failure_string = primary_event.link_fail_event.failure_string
                end_message = f"❌ Not Settled {amount:.0f} {failure_string}"
            else:
                end_message = "❌ Not Settled"

        elif group_list[2].event_type == EventType.FORWARD and (
            group_list[2].forward_fail_event or group_list[2].link_fail_event
        ):
            end_message = "❌ Forward Fail"
        elif group_list[2].final_htlc_event and group_list[2].final_htlc_event.settled:
            start_message = "💰 Forwarded "
            end_message = f"✅ Earned {primary_event.forward_amt_fee.fee:,.3f} "
        else:
            end_message = "❌ Not Settled"
        message_str = (
            f"{start_message} "
            f"{primary_event.forward_amt_fee.forward_amount:,.0f} "
            f"{self.lookup_name(primary_event.incoming_channel_id)} → "
            f"{self.lookup_name(primary_event.outgoing_channel_id)}. "
            f"{end_message}"
        )
        return message_str

    def lookup_name(self, channel_id: int | None = None) -> str:
        if channel_id is None:
            return "Unknown"
        return self.names.get(channel_id, str(channel_id))
