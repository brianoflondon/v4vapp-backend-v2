from typing import List
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from google.protobuf.json_format import MessageToDict


def event_type_name(event_type: routerrpc.HtlcEvent.EventType) -> str:
    return routerrpc.HtlcEvent.EventType.Name(event_type)


class LndEventsGroup:
    htlc_events: List[routerrpc.HtlcEvent] = []
    invoices: List[lnrpc.Invoice] = []
    payments: List[lnrpc.Payment] = []

    def __init__(
        self,
        htlc_events: List[routerrpc.HtlcEvent] = [],
        invoices: List[lnrpc.Invoice] = [],
        payments: List[lnrpc.Payment] = [],
    ) -> None:
        self.htlc_events = htlc_events
        self.invoices = invoices
        self.payments = payments

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

    def htlc_complete_group(self, htlc_id: int) -> bool:
        """
        Determines if the HTLC (Hashed TimeLock Contract) group is complete based on the given HTLC ID.

        Args:
            htlc_id (int): The ID of the HTLC to check.

        Returns:
            bool: True if the HTLC group is complete, False otherwise.

        The function checks the event type of the first event in the group and performs the following checks:
        - If the event type is "FORWARD":
            - Returns True if the group contains exactly 3 events.
            - If the group contains 2 events, it checks for the presence of a "FORWARD" event with a link failure and an "UNKNOWN" event with a final HTLC event.
              If both conditions are met, it marks the final HTLC event as settled and returns True.
            - Returns False otherwise.
        - If the event type is "SEND" or "RECEIVE":
            - Returns True if the group contains exactly 2 events, False otherwise.
        - For any other event type, returns True.
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

    def add_invoice(self, invoice: lnrpc.Invoice) -> int:
        add_index = invoice.add_index or 0
        self.invoices.append(invoice)
        return add_index

    def clear_invoices(self) -> None:
        self.invoices.clear()

    def add_payment(self, payment: lnrpc.Payment) -> int:
        payment_index = payment.payment_index or 0
        self.payments.append(payment)
        return payment_index

    def clear_payments(self) -> None:
        self.payments.clear()

    def __contains__(
        self, item: routerrpc.HtlcEvent | lnrpc.Invoice | lnrpc.Payment
    ) -> bool:
        match type(item):
            case routerrpc.HtlcEvent:
                return item in self.htlc_events
            case lnrpc.Invoice:
                return item in self.invoices
            case lnrpc.Payment:
                return item in self.payments
            case _:
                return False

    def __repr__(self) -> str:
        return f"HtlcEventGroup({self.htlc_events})"

    def __str__(self) -> str:
        return str(self.htlc_events)

    def to_dict(self) -> dict:
        return {
            "htlc_events": [
                self._htlc_event_to_dict(event) for event in self.htlc_events
            ]
        }

    def _htlc_event_to_dict(self, event: routerrpc.HtlcEvent) -> dict:
        event_dict = MessageToDict(event, preserving_proto_field_name=True)
        return event_dict
