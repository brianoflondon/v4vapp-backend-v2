from v4vapp_backend_v2.models.htlc_event_models import HtlcEvent

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
        print(count, htlc_event.forward_message("incoming_channel", "outgoing_channel"))
        print(count, htlc_event.forward_amt_fee)
        print("-" * 80)
