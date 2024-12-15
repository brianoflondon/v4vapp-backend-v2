import json
from typing import Generator

from pydantic import ValidationError

from v4vapp_backend_v2.models.htlc_event_models import (
    ChannelName,
    HtlcEvent,
    HtlcTrackingList,
)

htlc_event_data_list = [
    # 0 start of htlc_event_data_list
    {"subscribed_event": {}},
    # 1 RECEIVE
    {
        "incoming_channel_id": "800082725764071425",
        "incoming_htlc_id": "13875",
        "timestamp_ns": "1733489340854445234",
        "event_type": "RECEIVE",
        "settle_event": {"preimage": "q0u8DjlUYYylMhR4JY2FqjmfFtQTS+qbcxzODF0KmdI="},
    },
    # 2 SEND
    {
        "outgoing_channel_id": "800082725764071425",
        "outgoing_htlc_id": "10790",
        "timestamp_ns": "1733489437451262542",
        "event_type": "SEND",
        "forward_event": {
            "info": {"outgoing_timelock": 873592, "outgoing_amt_msat": "1234000"}
        },
    },
    # 3 SEND
    {
        "outgoing_channel_id": "800082725764071425",
        "outgoing_htlc_id": "10790",
        "timestamp_ns": "1733489438977352078",
        "event_type": "SEND",
        "settle_event": {"preimage": "PLHj0HUnPbUUa2/4bBiBPAxG3Tu6ZkXiANSDHAGgctU="},
    },
    # 4 FORWARD
    {
        "incoming_channel_id": "821398957719289857",
        "outgoing_channel_id": "800082725764071425",
        "incoming_htlc_id": "27267",
        "outgoing_htlc_id": "13876",
        "timestamp_ns": "1733490008211850238",
        "event_type": "FORWARD",
        "forward_event": {
            "info": {
                "incoming_timelock": 873652,
                "outgoing_timelock": 873552,
                "incoming_amt_msat": "22003",
                "outgoing_amt_msat": "22000",
            }
        },
    },
    # 5 FORWARD
    {
        "incoming_channel_id": "821398957719289857",
        "outgoing_channel_id": "800082725764071425",
        "incoming_htlc_id": "27268",
        "outgoing_htlc_id": "13878",
        "timestamp_ns": "1733490545441960529",
        "event_type": "FORWARD",
        "settle_event": {"preimage": "Vo98OvVvhuHwgls7HaCme9J2SwMWAGiomlSfoXhEUJk="},
    },
    # 6 FORWARD END
    {
        "incoming_channel_id": "821398957719289857",
        "incoming_htlc_id": "27268",
        "timestamp_ns": "1733490545577188389",
        "final_htlc_event": {"settled": True, "offchain": True},
    },
    # 7 RECEIVE
    {
        "incoming_channel_id": "821398957719289857",
        "incoming_htlc_id": "27602",
        "timestamp_ns": "1733566043184062900",
        "event_type": "RECEIVE",
        "settle_event": {"preimage": "kHOpZbcqpXWhSRqcW5OMPu4guHsYKpv1pMhXtTRkqjg="},
    },
    # 8 Final HTLC Event
    {
        "incoming_channel_id": "821398957719289857",
        "incoming_htlc_id": "27602",
        "timestamp_ns": "1733566043312622051",
        "final_htlc_event": {"settled": True, "offchain": True},
    },
    # 9 FORWARD Fail
    {
        "incoming_channel_id": 949409599124406300,
        "outgoing_channel_id": 888405395399180300,
        "incoming_htlc_id": 928,
        "timestamp_ns": 1733597007363239700,
        "event_type": "FORWARD",
        "link_fail_event": {
            "info": {
                "incoming_timelock": 874393,
                "outgoing_timelock": 874293,
                "incoming_amt_msat": 7540960454,
                "outgoing_amt_msat": 7532283264,
            },
            "wire_failure": "TEMPORARY_CHANNEL_FAILURE",
            "failure_detail": "INSUFFICIENT_BALANCE",
            "failure_string": "insufficient bandwidth to route htlc",
        },
    },
    # 10 Settle Event
    {
        "incoming_channel_id": 821398957719289900,
        "outgoing_channel_id": 920269242371670000,
        "incoming_htlc_id": 27809,
        "outgoing_htlc_id": 1831,
        "timestamp_ns": 1733650382468938000,
        "event_type": "FORWARD",
        "settle_event": {"preimage": "b'+E7LxMHJZY9ske8wGPhGNmezR1rzWpUXdmAEbbYo7QM='"},
    },
    # 11 Forward Fail Event
    {
        "incoming_channel_id": 920269242371670000,
        "outgoing_channel_id": 887757782897721300,
        "incoming_htlc_id": 3296,
        "outgoing_htlc_id": 2011,
        "timestamp_ns": 1733667260658913800,
        "event_type": "FORWARD",
        "forward_fail_event": {},
    },
]


def test_htlc_event():
    for count, htlc_event_data in enumerate(htlc_event_data_list):
        htlc_event = HtlcEvent.model_validate(htlc_event_data)
        print(count, htlc_event.event_type)
        print(count, htlc_event.forward_message())
        print(count, htlc_event.forward_amt_fee)
        print("-" * 80)


def read_log_file_htlc_events(file_path: str) -> Generator[HtlcEvent, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "htlc_event" in log_entry:
                    yield HtlcEvent.model_validate(log_entry["htlc_event"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def read_log_file_channel_names(file_path: str) -> Generator[ChannelName, None, None]:
    with open(file_path, "r") as file:
        # Parse each line as JSON and yield the htlc_event data
        for line in file.readlines():
            try:
                log_entry = json.loads(line)
                if "channel_name" in log_entry:
                    yield ChannelName.model_validate(log_entry["channel_name"])

            except ValidationError as e:
                print(e)
                continue
            except Exception as e:
                print(e)
                continue


def test_group_detection():
    tracking = HtlcTrackingList()
    try:
        for name in read_log_file_channel_names(
            "tests/data/htlc_events_test_data2.safe_log"
        ):
            tracking.add_name(name)
            print(name)
            print("-" * 80)

        for id, name in tracking.names.items():
            print(id, name)

        for htlc_event in read_log_file_htlc_events(
            "tests/data/htlc_events_test_data2.safe_log"
        ):
            htlc_id = tracking.add_event(htlc_event)
            events_in_group = tracking.list_htlc_id(htlc_id=htlc_id)
            complete = tracking.complete_group(htlc_id=htlc_id)
            complete_str = "✅" if tracking.complete_group(htlc_id=htlc_id) else "❌"
            print(
                f"{complete_str} "
                f"{htlc_id:>6} "
                f"{htlc_event.event_type.value} "
                f"{htlc_event.timestamp} "
                f"events in group "
                f"{len(events_in_group)} "
                f"{tracking.complete_group(htlc_id=htlc_id)}"
            )
            print(tracking.message(htlc_id=htlc_id))
            if complete:
                print(f"Group {htlc_id} is complete")
                tracking.delete_event(htlc_id=htlc_id)
            print("-" * 80)
    except FileNotFoundError as e:
        print(e)
        assert False
    assert len(tracking.list_htlc_id(385)) == 0
    assert len(tracking.list_htlc_id(1338)) == 0

    all_htlc_ids = tracking.list_all_htlc_ids()

    for htlc_id in all_htlc_ids:
        events_in_group = tracking.list_htlc_id(htlc_id=htlc_id)
        print(
            f"HTLC ID: {htlc_id} - {events_in_group[0].event_type.value} "
            f"Events in group: {len(events_in_group)}"
        )
        print(tracking.list_htlc_id(htlc_id=htlc_id))

    assert len(tracking.events) == 0
