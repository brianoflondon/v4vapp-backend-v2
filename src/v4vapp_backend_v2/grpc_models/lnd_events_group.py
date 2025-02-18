from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Union

from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import format_time_delta, get_in_flight_time


def event_type_name(event_type: routerrpc.HtlcEvent.EventType) -> str:
    return routerrpc.HtlcEvent.EventType.Name(event_type)


def payment_event_status_name(event_status: lnrpc.Payment.PaymentStatus) -> str:
    return lnrpc.Payment.PaymentStatus.Name(event_status)


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

    @property
    def fee_percent(self) -> float:
        return self.fee / self.forward_amount * 100 if self.forward_amount else 0

    @property
    def fee_ppm(self) -> float:
        return self.fee / self.forward_amount * 1_000_000 if self.forward_amount else 0


EventItem = Union[routerrpc.HtlcEvent, lnrpc.Invoice, lnrpc.Payment, LndChannelName]


class LndEventsGroup:
    htlc_events: List[routerrpc.HtlcEvent] = []
    lnrpc_invoices: List[lnrpc.Invoice] = []
    lnrpc_payments: List[lnrpc.Payment] = []
    channel_names: dict[int, LndChannelName] = {}

    def __init__(
        self,
        htlc_events: List[routerrpc.HtlcEvent] = [],
        invoices: List[lnrpc.Invoice] = [],
        payments: List[lnrpc.Payment] = [],
    ) -> None:
        self.htlc_events = htlc_events
        self.lnrpc_invoices = invoices
        self.lnrpc_payments = payments

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
                return self.htlc_complete_group(event)
            case "Invoice":
                invoice_group = self.get_invoice_list_by_pre_image(event.r_preimage)
                if self.is_invoice_expired(event):
                    return True
                return True if len(invoice_group) == 2 else False
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
                invoice_group = self.get_invoice_list_by_pre_image(event.r_preimage)
                for invoice in invoice_group:
                    self.lnrpc_invoices.remove(invoice)
                self.clear_expired_invoices()
            case "Payment":
                self.lnrpc_payments.remove(event)
            case _:
                pass

    def list_groups(self) -> List[List[EventItem]]:
        return []

    def report_event_counts(self) -> dict:
        return {
            "htlc_events": len(self.htlc_events),
            "invoices": len(self.lnrpc_invoices),
            "payments": len(self.lnrpc_payments),
            "channel_names": len(self.channel_names),
        }

    def report_event_counts_str(self) -> str:
        counts = self.report_event_counts()
        return (
            f"HTLC Events: {counts['htlc_events']}, "
            f"Invoices: {counts['invoices']}, "
            f"Payments: {counts['payments']}, "
            f"Channel Names: {counts['channel_names']}"
        )

    def message(self, event: EventItem, dest_alias: str = None) -> Tuple[str, dict]:
        """
        Generates a message string based on the type of the given event.
        Args:
            event (EventItem): The event object containing details about the event.
            dest_alias (str, optional): The destination alias for the event. Defaults to None.
                has to be passed in because it is read from a matching payment and looked up
        Returns:
            str: A formatted message string representing the event.
        """

        event_type = event.__class__.__name__
        match event_type:
            case "HtlcEvent":
                return self.message_htlc_event(event, dest_alias)
            case "Invoice":
                return (
                    f"🧾 Invoice: {event.value_msat//1000:,.0f} ({event.add_index})",
                    {},
                )
            case "Payment":
                return self.message_payment_event(event, dest_alias)
            case "ChannelName":
                return f"🔗 Channel: {event}", event.to_dict()
            case _:
                return "", {}

    def message_htlc_event(
        self, event: routerrpc.HtlcEvent, dest_alias: str = None
    ) -> Tuple[str, dict]:
        htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
        # Special exception for when the htlc_id is 0 during the subscribe start up
        if htlc_id == 0 or not self.htlc_complete_group(event):
            return f"{event_type_name(event.event_type)} {htlc_id} in progress", {
                "htlc_id": htlc_id
            }
        match event.event_type:
            case routerrpc.HtlcEvent.EventType.FORWARD:
                return self.message_forward_event(htlc_id)
            case routerrpc.HtlcEvent.EventType.UNKNOWN:
                return self.message_forward_event(htlc_id)
            case routerrpc.HtlcEvent.EventType.SEND:
                return self.message_send_event(htlc_id, dest_alias)
            case routerrpc.HtlcEvent.EventType.RECEIVE:
                return self.message_receive_event(htlc_id)
            case _:
                return f"no message {event_type_name(event.event_type)}"
        return "", {}

    def message_payment_event(
        self, event: routerrpc.HtlcEvent, dest_alias: str = None
    ) -> Tuple[str, dict]:
        if not isinstance(event, lnrpc.Payment):
            return "", {}
        creation_date = datetime.fromtimestamp(
            event.creation_time_ns / 1e9, tz=timezone.utc
        )
        # in_flight_time = format_time_delta(
        #     datetime.now(tz=timezone.utc) - creation_date
        # )
        in_flight_time = get_in_flight_time(creation_date)
        ans_dict = {
            "creation_date": creation_date,
            "in_flight_time": in_flight_time,
            "dest_alias": dest_alias,
        }
        return (
            f"💸 Payment: {event.value_msat//1000:,.0f} sats "
            f"(event.payment_index) "
            f"to: {dest_alias or 'Unknown'} "
            f"in flight: {in_flight_time} "
            f"{payment_event_status_name(event.status)} {event.payment_index}",
        ), ans_dict

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

    def get_htlc_event_pre_image(self, htlc_id: int) -> str:
        for event in self.htlc_events:
            if event.incoming_htlc_id == htlc_id or event.outgoing_htlc_id == htlc_id:
                preimage = (
                    event.settle_event.preimage.hex()
                    if event.settle_event.preimage != b""
                    else None
                )
                if preimage:
                    return preimage
        return ""

    def list_htlc_ids(self) -> List[int]:
        return list(
            {
                event.incoming_htlc_id or event.outgoing_htlc_id
                for event in self.htlc_events
            }
        )

    def list_groups_htlc(self) -> List[List[routerrpc.HtlcEvent]]:
        """
        Groups HTLC (Hashed Time-Locked Contract) events by their HTLC ID.

        This method iterates over the list of HTLC events and groups them based on their
        incoming or outgoing HTLC ID. Each unique HTLC ID will have a list of associated
        events.

        Returns:
            List[List[routerrpc.HtlcEvent]]: A list of lists, where each inner list contains
            HTLC events that share the same HTLC ID.
        """
        grouped_events = {}
        for event in self.htlc_events:
            htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
            if htlc_id not in grouped_events:
                grouped_events[htlc_id] = []
                grouped_events[htlc_id].append(event)
        return list(grouped_events.values())

    def htlc_complete_group(self, event: routerrpc.HtlcEvent) -> bool:
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
        htlc_id = event.incoming_htlc_id or event.outgoing_htlc_id
        group_list = self.by_htlc_id(htlc_id)
        if group_list:
            match event_type_name(group_list[0].event_type):
                case "FORWARD":
                    if len(group_list) == 3:
                        if not (event == group_list[-1]):
                            return False
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
                            if event == group_list[1]:
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

    def message_forward_event(self, htlc_id: int) -> Tuple[str, dict]:
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
                    end_message = f"❌ {amount:.0f} {failure_string}"
                else:
                    end_message = "❌"

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
                    f"{self.forward_amt_fee(primary_event).fee_percent:.2f}% "
                    f"{self.forward_amt_fee(primary_event).fee_ppm:.0f} ppm"
                )
            else:
                end_message = "❌"
            message_str = (
                f"{start_message} "
                f"{self.forward_amt_fee(primary_event).forward_amount:,.0f} "
                f"{from_channel} → "
                f"{to_channel} "
                f"{end_message} ({htlc_id})"
            )
            ans_dict = {
                "htlc_id": htlc_id,
                "from_channel": from_channel,
                "to_channel": to_channel,
                "amount": self.forward_amt_fee(primary_event).forward_amount,
                "fee": self.forward_amt_fee(primary_event).fee,
                "fee_percent": self.forward_amt_fee(primary_event).fee_percent,
                "fee_ppm": self.forward_amt_fee(primary_event).fee_ppm,
            }
            return message_str, ans_dict

    def forward_amt_fee(self, event: routerrpc.HtlcEvent) -> ForwardAmtFee:
        info = event.forward_event.info
        if info:
            incoming_amt_msat = info.incoming_amt_msat or 0
            outgoing_amt_msat = info.outgoing_amt_msat or 0

            forward_amount = outgoing_amt_msat / 1000
            earned: float = (incoming_amt_msat - outgoing_amt_msat) / 1000
            return ForwardAmtFee(forward_amount=forward_amount, fee=earned)
        return ForwardAmtFee(forward_amount=0, fee=0)

    def get_payment_by_pre_image(self, pre_image: str) -> lnrpc.Payment:
        for payment in self.lnrpc_payments:
            if payment.payment_preimage == pre_image:
                return payment
        return None

    def message_send_event(
        self, htlc_id: int, dest_alias: str = None
    ) -> Tuple[str, dict]:
        """
        Constructs a message string based on the HTLC (Hashed Time-Locked Contract) event details.
        Args:
            htlc_id (int): The ID of the HTLC event.
            dest_alias (str, optional): The alias of the destination. Defaults to None.
        Returns:
            str: A formatted message string describing the HTLC event status.
        Raises:
            IndexError: If the group_list does not contain at least two events.
        """
        group_list = self.by_htlc_id(htlc_id)
        primary_event = group_list[0]
        secondary_event = group_list[1]
        if secondary_event.settle_event:
            payment = self.search_payment_preimage(
                secondary_event.settle_event.preimage
            )
            if payment:
                fee = payment.fee_msat / 1000 if payment.fee_msat else 0
            else:
                fee = 0
            end_message = f"fee: {fee:,.3f} ✅"
        else:
            end_message = "❌"

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
            f"{start_message} {amount:,.0f} "
            f"to {dest_alias or 'Unknown'} "
            f"out {sent_via}. "
            f"{end_message} ({htlc_id})"
        )
        ans_dict = {
            "htlc_id": htlc_id,
            "amount": amount,
            "sent_via": sent_via,
            "dest_alias": dest_alias,
            "end_message": end_message,
        }
        return message_str, ans_dict

    def message_receive_event(self, htlc_id: int) -> Tuple[str, dict]:
        group_list = self.by_htlc_id(htlc_id)
        primary_event = group_list[0]
        htlc_id = primary_event.incoming_htlc_id or primary_event.outgoing_htlc_id
        if primary_event.incoming_channel_id:
            received_via = self.lookup_name(primary_event.incoming_channel_id)
        else:
            received_via = "Unknown"
        for_memo = ""
        amount = 0
        htlc_id_str = ""
        if htlc_id:
            incoming_invoice = self.lookup_invoice_by_htlc_id(htlc_id=htlc_id)
            if incoming_invoice:
                amount = incoming_invoice.value
                htlc_id_str = f" ({htlc_id})"
                for_memo = (
                    f" for {incoming_invoice.memo}" if incoming_invoice.memo else ""
                )

        message_str = (
            f"💵 Received {amount:,}{for_memo} via " f"{received_via}{htlc_id_str}"
        )
        ans_dict = {
            "htlc_id": htlc_id,
            "amount": amount,
            "received_via": received_via,
            "for_memo": for_memo,
        }
        return message_str, ans_dict

    # MARK: Invoice Methods
    def add_invoice(self, invoice: lnrpc.Invoice) -> int:
        add_index = invoice.add_index or 0
        self.lnrpc_invoices.append(invoice)
        return add_index

    def clear_invoices(self) -> None:
        self.lnrpc_invoices.clear()

    def clear_expired_invoices(self) -> None:
        self.lnrpc_invoices = [
            invoice
            for invoice in self.lnrpc_invoices
            if not self.is_invoice_expired(invoice)
        ]

    def lookup_invoice_by_htlc_id(self, htlc_id: int) -> lnrpc.Invoice:
        for invoice in self.lnrpc_invoices:
            if invoice and invoice.htlcs:
                for htlc_data in invoice.htlcs:
                    if int(htlc_data.htlc_index) == int(htlc_id):
                        return invoice
        return None

    def get_invoice_list_by_pre_image(self, pre_image: str) -> List[lnrpc.Invoice]:
        answer = []
        for invoice in self.lnrpc_invoices:
            if invoice.r_preimage == pre_image:
                answer.append(invoice)
            return answer
        return []

    def is_invoice_expired(self, invoice: lnrpc.Invoice) -> bool:
        """
        Check if the event has expired.

        Args:
            event (lnrpc.Payment): The event to check.

        Returns:
            bool: True if the event has expired, False otherwise.
        """
        expiry_date = datetime.fromtimestamp(
            invoice.creation_date + invoice.expiry, tz=timezone.utc
        )
        expired = datetime.now(tz=timezone.utc) > expiry_date
        return expired

    # MARK: Payment Methods
    def add_payment(self, payment: lnrpc.Payment) -> int:
        payment_index = payment.payment_index or 0
        self.lnrpc_payments.append(payment)
        return payment_index

    def clear_payments(self) -> None:
        self.lnrpc_payments.clear()

    def search_payment(self, htlc_id: int) -> lnrpc.Payment:
        """
        Search for a payment in the payments list by HTLC ID.

        Args:
            htlc_id (int): The HTLC ID to search for.

        Returns:
            lnrpc.Payment: The payment object if found, None otherwise.
        """
        for payment in self.lnrpc_payments:
            if payment.htlc_id == htlc_id:
                return payment
        return None

    def search_payment_preimage(self, pre_image: str) -> lnrpc.Payment:
        """
        Search for a payment in the payments list by preimage.

        Args:
            pre_image (str): The preimage to search for.

        Returns:
            lnrpc.Payment: The payment object if found, None otherwise.
        """
        for payment in self.lnrpc_payments:
            for htlc in payment.htlcs:
                if htlc.preimage == pre_image:
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
                return item in self.lnrpc_invoices
            case lnrpc.Payment:
                return item in self.lnrpc_payments
            case _:
                if isinstance(item, LndChannelName):
                    return item.channel_id in self.channel_names
                return False

    def to_dict(self) -> dict:
        return {
            "htlc_events": [self._event_to_dict(event) for event in self.htlc_events],
            "invoices": [
                self._event_to_dict(invoice) for invoice in self.lnrpc_invoices
            ],
            "payments": [
                self._event_to_dict(payment) for payment in self.lnrpc_payments
            ],
        }

    def _event_to_dict(self, event: EventItem) -> dict:
        return MessageToDict(event, preserving_proto_field_name=True)
