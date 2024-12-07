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
]


def test_htlc_event():
    for htlc_event_data in htlc_event_data_list:
        print(htlc_event_data)
        htlc_event = HtlcEvent.model_validate(htlc_event_data)
        if htlc_event.event_type:
            assert htlc_event.event_type == htlc_event_data["event_type"]
            if htlc_event.event_type == "FORWARD":
                if htlc_event.forward_event and htlc_event.forward_event.info:
                    assert htlc_event.forward_event.info.incoming_amt_msat == int(
                        htlc_event_data["forward_event"]["info"]["incoming_amt_msat"]
                    )
                    print(htlc_event.forward_amt_earned)
                print(
                    htlc_event.forward_message("incoming_channel", "outgoing_channel")
                )
        print(htlc_event.model_dump_json(indent=2, exclude_none=True))
