import json
from typing import Generator
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from google.protobuf.json_format import MessageToDict, ParseDict
from v4vapp_backend_v2.grpc_models.lnd_events_group import LndEventsGroup


def read_log_file_htlc_events(
    file_path: str,
) -> Generator[routerrpc.HtlcEvent | lnrpc.Invoice | lnrpc.Payment, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if log_entry.get("htlc_event"):
                    event = ParseDict(log_entry["htlc_event"], routerrpc.HtlcEvent())
                elif log_entry.get("invoice"):
                    event = ParseDict(log_entry["invoice"], lnrpc.Invoice())
                elif log_entry.get("payment"):
                    event = ParseDict(log_entry["payment"], lnrpc.Payment())
                yield event
            except Exception as e:
                print(f"Error parsing log entry: {e}")
                continue


def test_lnd_events_group():
    lnd_events_group = LndEventsGroup()
    for event in read_log_file_htlc_events(
        "tests/data/lnd_events/v4vapp-backend-v2.safe_log.jsonl"
    ):
        if isinstance(event, routerrpc.HtlcEvent):
            htlc_id = lnd_events_group.add_htlc_event(event)
            assert event in lnd_events_group
            print(htlc_id, lnd_events_group.htlc_complete_group(htlc_id))
        if isinstance(event, lnrpc.Invoice):
            add_index = lnd_events_group.add_invoice(event)
            assert event in lnd_events_group
            print("Invoice: ", add_index)
        if isinstance(event, lnrpc.Payment):
            payment_index = lnd_events_group.add_payment(event)
            assert event in lnd_events_group
            print("Payment: ", payment_index)

    # Test the fall through case
    assert 1 not in lnd_events_group

    print("Invoices: ", len(lnd_events_group.invoices))
    print("Payments: ", len(lnd_events_group.payments))
    print("HTLC Events: ", len(lnd_events_group.htlc_events))
    assert len(lnd_events_group.invoices) == 28
    assert len(lnd_events_group.payments) == 45
    assert len(lnd_events_group.htlc_events) == 83

    for htlc_event in lnd_events_group.htlc_events:
        htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
        print(htlc_id, MessageToDict(htlc_event, preserving_proto_field_name=True))
        print(lnd_events_group.htlc_complete_group(htlc_id))

    lnd_events_group.clear_htlc_events()
    assert len(lnd_events_group.htlc_events) == 0
