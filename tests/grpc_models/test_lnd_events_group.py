import json
from typing import Generator
import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from google.protobuf.json_format import MessageToDict, ParseDict
from v4vapp_backend_v2.grpc_models.lnd_events_group import (
    LndEventsGroup,
    LndChannelName,
    event_type_name,
)


def read_log_file(
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
                elif log_entry.get("channel_name"):
                    event = LndChannelName(
                        channel_id=log_entry["channel_name"]["channel_id"],
                        name=log_entry["channel_name"]["name"],
                    )
                yield event
            except Exception as e:
                print(f"Error parsing log entry: {e}")
                continue


def test_lnd_events_group():
    lnd_events_group = LndEventsGroup()
    for event in read_log_file(
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

    for htlc_event in lnd_events_group.htlc_events:
        htlc_id = htlc_event.incoming_htlc_id or htlc_event.outgoing_htlc_id
        print(htlc_id, MessageToDict(htlc_event, preserving_proto_field_name=True))
        print(lnd_events_group.htlc_complete_group(htlc_id))

    lnd_events_group.clear_htlc_events()
    assert len(lnd_events_group.htlc_events) == 0


def test_append_method():
    lnd_events_group = LndEventsGroup()
    for event in read_log_file(
        "tests/data/lnd_events/v4vapp-backend-v2.safe_log.jsonl"
    ):
        identifier = lnd_events_group.append(event)
        assert event in lnd_events_group
        # Now test sending an event instead of an event name
        print(
            identifier,
            event.__class__.__name__,
            lnd_events_group.complete_group(event=event),
        )
        print(lnd_events_group.message(event=event))

    json_dump = json.dumps(lnd_events_group.to_dict(), indent=2)
    assert json_dump is not None
    lnd_events_group.clear()


def test_channel_names():
    lnd_events_group = LndEventsGroup()
    for event in read_log_file(
        "tests/data/lnd_events/v4vapp-backend-v2.safe_log.jsonl"
    ):
        print(event)
        lnd_events_group.append(event)

    for channel_name in lnd_events_group.channel_names.values():
        print(channel_name, channel_name.channel_id, channel_name.name)
        print(lnd_events_group.lookup_name(channel_name.channel_id))


def test_message_forward_events():
    lnd_events_group = LndEventsGroup()
    for event in read_log_file(
        "tests/data/lnd_events/v4vapp-backend-v2.safe_log.jsonl"
        # "tests/data/htlc_events_test_data2.safe_log"
    ):
        lnd_events_group.append(event)
        lnd_events_group.complete_group(event=event)

    print(lnd_events_group.report_event_counts())
    print(lnd_events_group.report_event_counts_str())

    for group in lnd_events_group.list_groups_htlc():
        dest_alias = None
        if type(event) == routerrpc.HtlcEvent:
            event_id = group[0].incoming_htlc_id or group[0].outgoing_htlc_id
            pre_image = lnd_events_group.get_htlc_event_pre_image(event_id)
            if pre_image:
                matching_payment = lnd_events_group.get_payment_by_pre_image(pre_image)
                if matching_payment:
                    dest_alias = "Simulated lookup"
                    # dest_alias = await get_node_alias_from_pay_request(
                    #     matching_payment.payment_request, client
                    # )
        print(lnd_events_group.message(event=group[0], dest_alias=dest_alias))

    for invoice in lnd_events_group.invoices:
        print(lnd_events_group.message(event=invoice))

    lnd_events_group.clear()

    for invoice in lnd_events_group.invoices:
        print(lnd_events_group.message(event=invoice))
