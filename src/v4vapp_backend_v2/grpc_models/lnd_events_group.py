from typing import List
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from google.protobuf.json_format import MessageToDict
from typing import Union


def event_type_name(event_type: routerrpc.HtlcEvent.EventType) -> str:
    return routerrpc.HtlcEvent.EventType.Name(event_type)


class LndChannelName:
    channel_id: int
    name: str

    def __init__(self, channel_id: int, name: str) -> None:
        self.channel_id = channel_id
        self.name = name

    def __str__(self):
        return f"{self.name} ({self.channel_id})"

    def to_dict(self) -> dict:
        return {
            "channel_id": self.channel_id,
            "name": self.name,
        }


class ForwardAmtFee:
    forward_amount: float
    fee: float

    def __init__(self, forward_amount: float = 0, fee: float = 0) -> None:
        self.forward_amount = forward_amount
        self.fee = fee


EventItem = Union[routerrpc.HtlcEvent, lnrpc.Invoice, lnrpc.Payment, LndChannelName]


class LndEventsGroup:
    htlc_events: List[routerrpc.HtlcEvent] = []
    invoices: List[lnrpc.Invoice] = []
    payments: List[lnrpc.Payment] = []
    channel_names: dict[int, LndChannelName] = {}

    def __init__(
        self,
        htlc_events: List[routerrpc.HtlcEvent] = [],
        invoices: List[lnrpc.Invoice] = [],
        payments: List[lnrpc.Payment] = [],
    ) -> None:
        self.htlc_events = htlc_events
        self.invoices = invoices
        self.payments = payments

    # MARK: Universal Methods

    def append(self, item: EventItem) -> int:
        """
        Appends an event item to the appropriate list based on its type.

        Args:
            item (EventItem): The event item to append. It can be of type
                              routerrpc.HtlcEvent, lnrpc.Invoice, or lnrpc.Payment.

        Returns:
            int: The result of the corresponding add method based on the type of the item.
                 Returns 0 if the item type does not match any known types.
        """
        match type(item):
            case routerrpc.HtlcEvent:
                return self.add_htlc_event(item)
            case lnrpc.Invoice:
                return self.add_invoice(item)
            case lnrpc.Payment:
                return self.add_payment(item)
            case _:
                if isinstance(item, LndChannelName):
                    return self.add_channel_name(item)
                return 0

    def clear(self) -> None:
        self.clear_htlc_events()
        self.clear_invoices()
        self.clear_payments()
        self.clear_channel_names()

    def complete_group(self, event: EventItem) -> bool:
        event_type = event.__class__.__name__
        match event_type:
            case "HtlcEvent":
                return self.htlc_complete_group(
                    event.incoming_htlc_id or event.outgoing_htlc_id
                )
            case "Invoice":
                return True
            case "Payment":
                return True
            case "ChannelName":
                # Channel Group is never complete and we'll never delete channels
                return False
            case _:
                return False

    def remove_group(self, event: EventItem) -> None:
        event_type = event.__class__.__name__
        match event_type:
            case "HtlcEvent":
                htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
                self.htlc_events = [
                    event
                    for event in self.htlc_events
                    if event.incoming_htlc_id != htlc_id
                    and event.outgoing_htlc_id != htlc_id
                ]
            case "Invoice":
                self.invoices.remove(event)
            case "Payment":
                self.payments.remove(event)
            case _:
                pass

    def list_groups(self) -> List[List[EventItem]]:
        return []

    def message(self, event: EventItem) -> str:
        event_type = event.__class__.__name__
        match event_type:
            case "HtlcEvent":
                return self.message_htlc_event(event)
            case "Invoice":
                return f"🧾 Invoice: {event.value_msat//1000:,.0f}"
            case "Payment":
                return f"💸 Payment: {event.value_msat//1000:,.0f}"
            case "ChannelName":
                return f"🔗 Channel: {event}"
            case _:
                return ""

    def message_htlc_event(self, event: routerrpc.HtlcEvent) -> str:
        htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
        # Special exception for when the htlc_id is 0 during the subscribe start up
        if htlc_id == 0 or not self.htlc_complete_group(htlc_id):
            return f"{event_type_name(event.event_type)} {htlc_id} in progress"
        match event.event_type:
            case routerrpc.HtlcEvent.EventType.FORWARD:
                message_str = self.message_forward_event(htlc_id)
            case routerrpc.HtlcEvent.EventType.UNKNOWN:
                message_str = self.message_forward_event(htlc_id)
            case routerrpc.HtlcEvent.EventType.SEND:
                message_str = self.message_send_event(htlc_id)
            case routerrpc.HtlcEvent.EventType.RECEIVE:
                message_str = f"💵 {htlc_id} receive complete"
            case _:
                message_str = f"no message {event_type_name(event.event_type)}"
        return message_str

    # MARK: HTLC Event Methods
    def add_htlc_event(self, htlc_event: routerrpc.HtlcEvent) -> int:
        htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
        self.htlc_events.append(htlc_event)
        return htlc_id

    def by_htlc_id(self, htlc_id: int) -> List[routerrpc.HtlcEvent]:
        return [
            event
            for event in self.htlc_events
            if event.incoming_htlc_id == htlc_id or event.outgoing_htlc_id == htlc_id
        ]

    def list_htlc_ids(self) -> List[int]:
        return list(
            {
                event.incoming_htlc_id or event.outgoing_htlc_id
                for event in self.htlc_events
            }
        )

    def list_groups_htlc(self) -> List[List[routerrpc.HtlcEvent]]:
        grouped_events = {}
        for event in self.htlc_events:
            htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
            if htlc_id not in grouped_events:
                grouped_events[htlc_id] = []
                grouped_events[htlc_id].append(event)
        return list(grouped_events.values())

    def htlc_complete_group(self, htlc_id: int) -> bool:
        """
        Determines if an HTLC (Hashed Time-Locked Contract) group is complete based on the given
        HTLC ID.

        Args:
            htlc_id (int): The ID of the HTLC to check.

        Returns:
            bool: True if the HTLC group is complete, False otherwise.

        The function checks the type of events associated with the given HTLC ID and determines
        if the group is complete based on the following conditions:
        - If the event type is "FORWARD":
            - The group is complete if it contains 3 events.
            - The group is complete if it contains 2 events and one is a forward event with a
              link fail event and the other is an unknown event with a final HTLC event.
        - If the event type is "SEND" or "RECEIVE":
            - The group is complete if it contains 2 events.
        - For any other event type, the group is considered complete.
        """

        group_list = self.by_htlc_id(htlc_id)
        if group_list:
            match event_type_name(group_list[0].event_type):
                case "FORWARD":
                    if len(group_list) == 3:
                        return True
                    if len(group_list) == 2:
                        has_forward_event = any(
                            event_type_name(event.event_type) == "FORWARD"
                            and event.link_fail_event
                            for event in group_list
                        )
                        has_unknown_event = any(
                            event_type_name(event.event_type) == "UNKNOWN"
                            and event.final_htlc_event
                            for event in group_list
                        )
                        if has_forward_event and has_unknown_event:
                            for event in group_list:
                                if event.final_htlc_event:
                                    event.final_htlc_event.settled = True
                            return True
                    return False
                case "SEND":
                    return True if len(group_list) == 2 else False
                case "RECEIVE":
                    return True if len(group_list) == 2 else False
                case _:
                    return True
        return False

    def clear_htlc_events(self) -> None:
        self.htlc_events.clear()

    def message_forward_event(self, htlc_id: int) -> str:
        """
        Returns the message of the forward event in the HTLC group with the given HTLC ID.

        Args:
            htlc_id (int): The ID of the HTLC group to check.

        Returns:
            str: The message of the forward event in the HTLC group with the given HTLC ID.
        """
        group_list = self.by_htlc_id(htlc_id)
        if group_list:
            primary_event: routerrpc.HtlcEvent = group_list[0]
            start_message = "💰 Attempted"
            from_channel = self.lookup_name(primary_event.incoming_channel_id)
            to_channel = self.lookup_name(primary_event.outgoing_channel_id)
            end_message = f"3 events"

            if len(group_list) == 2:
                if primary_event.link_fail_event:
                    if (
                        primary_event.link_fail_event.info
                        and primary_event.link_fail_event.info.incoming_amt_msat
                    ):
                        amount = (
                            primary_event.link_fail_event.info.incoming_amt_msat / 1000
                        )
                    else:
                        amount = 0
                    failure_string = primary_event.link_fail_event.failure_string
                    end_message = f"❌ Not Settled {amount:.0f} {failure_string}"
                else:
                    end_message = "❌ Not Settled"

            elif group_list[2].event_type == routerrpc.HtlcEvent.EventType.FORWARD and (
                group_list[2].forward_fail_event or group_list[2].link_fail_event
            ):
                end_message = "❌ Forward Fail"
            elif (
                group_list[2].final_htlc_event
                and group_list[2].final_htlc_event.settled
            ):
                start_message = "💰 Forwarded"
                end_message = (
                    f"✅ Earned {self.forward_amt_fee(primary_event).fee:,.3f} "
                )
            else:
                end_message = "❌ Not Settled"
            message_str = (
                f"{start_message} "
                f"{self.forward_amt_fee(primary_event).forward_amount:,.0f} "
                f"{from_channel} → "
                f"{to_channel} "
                f"{end_message}"
            )
            return message_str

    def forward_amt_fee(self, event: routerrpc.HtlcEvent) -> ForwardAmtFee:
        info = event.forward_event.info
        if info:
            incoming_amt_msat = info.incoming_amt_msat or 0
            outgoing_amt_msat = info.outgoing_amt_msat or 0

            forward_amount = outgoing_amt_msat / 1000
            earned: float = (incoming_amt_msat - outgoing_amt_msat) / 1000
            return ForwardAmtFee(forward_amount=forward_amount, fee=earned)
        return ForwardAmtFee(forward_amount=0, fee=0)

    def message_send_event(self, htlc_id: int) -> str:

        group_list = self.by_htlc_id(htlc_id)
        primary_event = group_list[0]
        secondary_event = group_list[1]
        end_message = "✅ Settled" if secondary_event.settle_event else "❌ Not Settled"
        start_message = "⚡️ Sent" if secondary_event.settle_event else "⚡️ Probing"
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

        message_str = (
            f"{start_message} {amount:,.0f} " f"out {sent_via}. " f"{end_message}"
        )
        return message_str

    # MARK: Invoice Methods
    def add_invoice(self, invoice: lnrpc.Invoice) -> int:
        add_index = invoice.add_index or 0
        self.invoices.append(invoice)
        return add_index

    def clear_invoices(self) -> None:
        self.invoices.clear()

    # MARK: Payment Methods
    def add_payment(self, payment: lnrpc.Payment) -> int:
        payment_index = payment.payment_index or 0
        self.payments.append(payment)
        return payment_index

    def clear_payments(self) -> None:
        self.payments.clear()

    def search_payment(self, htlc_id: int) -> lnrpc.Payment:
        """
        Search for a payment in the payments list by HTLC ID.

        Args:
            htlc_id (int): The HTLC ID to search for.

        Returns:
            lnrpc.Payment: The payment object if found, None otherwise.
        """
        for payment in self.payments:
            if payment.htlc_id == htlc_id:
                return payment
        return None

    # MARK: Channel Name Methods

    def add_channel_name(self, channel_name: LndChannelName) -> int:
        self.channel_names[channel_name.channel_id] = channel_name
        return channel_name.channel_id

    def clear_channel_names(self) -> None:
        self.channel_names.clear()

    def lookup_name(self, channel_id: int) -> str:
        return self.channel_names.get(
            channel_id, LndChannelName(channel_id, f"Channel {channel_id}")
        ).name

    # MARK: Magic Methods

    def __contains__(self, item: EventItem) -> bool:
        match type(item):
            case routerrpc.HtlcEvent:
                return item in self.htlc_events
            case lnrpc.Invoice:
                return item in self.invoices
            case lnrpc.Payment:
                return item in self.payments
            case _:
                if isinstance(item, LndChannelName):
                    return item.channel_id in self.channel_names
                return False

    def to_dict(self) -> dict:
        return {
            "htlc_events": [self._event_to_dict(event) for event in self.htlc_events],
            "invoices": [self._event_to_dict(invoice) for invoice in self.invoices],
            "payments": [self._event_to_dict(payment) for payment in self.payments],
        }

    def _event_to_dict(self, event: EventItem) -> dict:
        return MessageToDict(event, preserving_proto_field_name=True)
