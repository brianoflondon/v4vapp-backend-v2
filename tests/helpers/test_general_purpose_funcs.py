from datetime import datetime, timedelta, timezone

import pytest

from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_convert_keepsats,
    detect_hbd,
    detect_keepsats,
    detect_paywithsats,
    format_time_delta,
    get_in_flight_time,
    is_markdown,
    seconds_only,
    snake_case,
)


def test_snake_case():
    assert snake_case("OpBase") == "op_base"
    assert snake_case("ProducerReward") == "producer_reward"
    assert snake_case("ProducerRewardRaw") == "producer_reward_raw"
    assert snake_case("VestingShares") == "vesting_shares"
    assert snake_case("AccountWitnessVote") == "account_witness_vote"
    assert snake_case("VoterDetails") == "voter_details"


def test_seconds_only():
    delta = timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500)
    result = seconds_only(delta)
    expected = timedelta(days=1, seconds=7384)  # 2 hours, 3 minutes, and 4 seconds
    assert result == expected


def test_format_time_delta():
    # Test cases without fractions
    test_cases = [
        (timedelta(days=1, hours=2), "1 days, 2 hours"),
        (timedelta(hours=5, minutes=6, seconds=7), "05:06:07"),
        (timedelta(minutes=8, seconds=9), "00:08:09"),
        (timedelta(seconds=10), "00:00:10"),
        (timedelta(days=0, hours=0, minutes=0, seconds=0), "00:00:00"),
        (timedelta(days=2, hours=0, minutes=0, seconds=0), "2 days, 0 hours"),
        (timedelta(days=0, hours=3, minutes=0, seconds=0), "03:00:00"),
        (timedelta(days=0, hours=0, minutes=4, seconds=0), "00:04:00"),
    ]

    for delta, expected in test_cases:
        assert format_time_delta(delta) == expected

    # Test cases with fractions
    test_cases_with_fractions = [
        (timedelta(hours=1, minutes=2, seconds=3, microseconds=456000), "01:02:03.456"),
        (timedelta(minutes=8, seconds=9, microseconds=123000), "00:08:09.123"),
        (timedelta(seconds=10, microseconds=789000), "00:00:10.789"),
        (
            timedelta(days=0, hours=0, minutes=0, seconds=0, microseconds=0),
            "00:00:00.000",
        ),
    ]

    for delta, expected in test_cases_with_fractions:
        assert format_time_delta(delta, fractions=True) == expected


def test_get_in_flight_time_future_date():
    # Test case where the current time is before the creation date
    future_date = datetime.now(tz=timezone.utc) + timedelta(days=1)
    result = get_in_flight_time(future_date)
    assert result == "00:00:00", f"Expected '00:00:00', but got {result}"


def test_get_in_flight_time_past_date():
    # Test case where the current time is after the creation date
    past_date = datetime.now(tz=timezone.utc) - timedelta(days=1, hours=5, minutes=30)
    result = get_in_flight_time(past_date)
    assert result == "1 days, 5 hours", f"Expected '1 days, 5 hours', but got {result}"


def test_get_in_flight_time_exact_date():
    # Test case where the current time is exactly the creation date
    exact_date = datetime.now(tz=timezone.utc)
    result = get_in_flight_time(exact_date)
    assert result == "00:00:00", f"Expected '00:00:00', but got {result}"


@pytest.mark.parametrize(
    "memo, expected",
    [
        ("#sats are great", True),
        ("sats are great", True),
        ("#keepsats forever", True),
        ("keepsats forever", True),
        ("#paywithsats", False),
        ("", False),
        (None, False),
    ],
)
def test_detect_keepsats(memo, expected):
    assert detect_keepsats(memo) == expected


@pytest.mark.parametrize(
    "memo, expected",
    [
        ("Pay this invoice #paywithsats", True),
        ("#paywithsats", True),
        ("paywithsats", False),
        ("", False),
        (None, False),
    ],
)
def test_detect_paywithsats(memo, expected):
    assert detect_paywithsats(memo) == expected


@pytest.mark.parametrize(
    "memo, expected",
    [
        ("#hbd is great", True),
        ("hbd is great", False),
        ("#HBD is great", True),
        ("", False),
        (None, False),
    ],
)
def test_detect_hbd(memo, expected):
    assert detect_hbd(memo) == expected


@pytest.mark.parametrize(
    "memo, expected",
    [
        ("#convertkeepsats now", True),
        ("convertkeepsats now", False),
        ("#convertkeepsats", True),
        ("", False),
        (None, False),
    ],
)
def test_detect_convert_keepsats(memo, expected):
    assert detect_convert_keepsats(memo) == expected


def test_is_markdown():
    test_messages = [
        # "Plain text message",
        "Check this [link](https://example.com)",
        "This is **bold** text",
        "*italic* words here",
        "Some `code` inline",
        "```\nmultiline code\n```",
        "# Heading 1",
        "- List item",
        "1. Numbered list",
        "_italic_",
    ]
    assert is_markdown("plain text message") is False

    for msg in test_messages:
        print(f"Message: {msg}")
        print(f"Is Markdown? {is_markdown(msg)}\n")
        assert is_markdown(msg) is True
    assert is_markdown("Plain text message") is False


if __name__ == "__main__":
    pytest.main([__file__])
