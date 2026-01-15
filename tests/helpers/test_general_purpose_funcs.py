import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    cap_camel_case,
    check_time_diff,
    detect_convert_keepsats,
    detect_hbd,
    detect_keepsats,
    detect_paywithsats,
    draw_percentage_meter,
    format_time_delta,
    get_entrypoint_filename,
    get_in_flight_time,
    is_markdown,
    re_escape,
    sanitize_markdown_v1,
    seconds_only,
    snake_case,
    timestamp_inc,
)


def test_snake_case():
    for word in [
        "OpBase",
        "ProducerReward",
        "ProducerRewardRaw",
        "VestingShares",
        "AccountWitnessVote",
        "VoterDetails",
    ]:
        snake_case_word = snake_case(word)
        assert word == cap_camel_case(snake_case_word)

    assert snake_case("OpBase") == "op_base"
    assert snake_case("ProducerReward") == "producer_reward"
    assert snake_case("ProducerRewardRaw") == "producer_reward_raw"
    assert snake_case("VestingShares") == "vesting_shares"
    assert snake_case("AccountWitnessVote") == "account_witness_vote"
    assert snake_case("VoterDetails") == "voter_details"


def test_cap_camel_case():
    assert cap_camel_case("op_base") == "OpBase"
    assert cap_camel_case("producer_reward") == "ProducerReward"
    assert cap_camel_case("producer_reward_raw") == "ProducerRewardRaw"
    assert cap_camel_case("vesting_shares") == "VestingShares"
    assert cap_camel_case("account_witness_vote") == "AccountWitnessVote"
    assert cap_camel_case("voter_details") == "VoterDetails"


def test_seconds_only():
    delta = timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500)
    result = seconds_only(delta)
    expected = timedelta(days=1, seconds=7384)  # 2 hours, 3 minutes, and 4 seconds
    assert result == expected


def test_format_time_delta():
    # Test cases without fractions
    test_cases = [
        (timedelta(days=1, hours=2), "1 day, 2 hours"),
        (timedelta(hours=5, minutes=6, seconds=7), "05:06:07"),
        (timedelta(minutes=8, seconds=9), "08:09"),
        (timedelta(seconds=10), "00:10"),
        (timedelta(days=0, hours=0, minutes=0, seconds=0), "00:00"),
        (timedelta(days=2, hours=0, minutes=0, seconds=0), "2 days, 0 hours"),
        (timedelta(days=0, hours=3, minutes=0, seconds=0), "03:00:00"),
        (timedelta(days=0, hours=0, minutes=4, seconds=0), "04:00"),
    ]

    for delta, expected in test_cases:
        assert format_time_delta(delta) == expected

    # Test cases with fractions
    test_cases_with_fractions = [
        (timedelta(hours=1, minutes=2, seconds=3, microseconds=456000), "01:02:03.456"),
        (timedelta(minutes=8, seconds=9, microseconds=123000), "08:09.123"),
        (timedelta(seconds=10, microseconds=789000), "00:10.789"),
        (
            timedelta(days=0, hours=0, minutes=0, seconds=0, microseconds=0),
            "00:00.000",
        ),
    ]

    for delta, expected in test_cases_with_fractions:
        assert format_time_delta(delta, fractions=True) == expected

    test_cases_just_hours = [
        (timedelta(days=1, hours=2, seconds=1002), "1 day, 2 hours"),
        (timedelta(hours=5, minutes=6, seconds=7), "5 hours"),
        (timedelta(minutes=8, seconds=9), "0 hours"),
        (timedelta(seconds=10), "0 hours"),
        (timedelta(days=0, hours=0, minutes=0, seconds=0), "0 hours"),
    ]
    for delta, expected in test_cases_just_hours:
        ans = format_time_delta(delta, just_days_or_hours=True)
        assert ans == expected, f"Expected: {expected}, Got: {ans}"


def test_get_in_flight_time_future_date():
    # Test case where the current time is before the creation date
    future_date = datetime.now(tz=timezone.utc) + timedelta(days=1)
    result = get_in_flight_time(future_date)
    assert result == "00:00", f"Expected '00:00', but got {result}"


def test_get_in_flight_time_past_date():
    # Test case where the current time is after the creation date
    past_date = datetime.now(tz=timezone.utc) - timedelta(days=1, hours=5, minutes=30)
    result = get_in_flight_time(past_date)
    assert result == "1 day, 5 hours", f"Expected '1 day, 5 hours', but got {result}"


def test_get_in_flight_time_exact_date():
    # Test case where the current time is exactly the creation date
    exact_date = datetime.now(tz=timezone.utc)
    result = get_in_flight_time(exact_date)
    assert result == "00:00", f"Expected '00:00', but got {result}"


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
        "🐝 **liondani Missed block** 101,084,197  1 day, 0 hours",
    ]
    assert is_markdown("plain text message") is False

    for msg in test_messages:
        print(f"Message: {msg}")
        print(f"Is Markdown? {is_markdown(msg)}\n")
        assert is_markdown(msg) is True
    assert is_markdown("Plain text message") is False


@pytest.mark.parametrize(
    "text, reserved_chars, expected",
    [
        ("hello.world", ".", "hello\\.world"),  # Single reserved char
        ("a.b*c[d]", ".*[]", "a\\.b\\*c\\[d\\]"),  # Multiple reserved chars
        ("helloworld", ".*[]", "helloworld"),  # No reserved chars in text
        ("", ".*[]", ""),  # Empty text
        ("hello.world", "", "hello.world"),  # Empty reserved_chars
        (".*[]", ".*[]", "\\.\\*\\[\\]"),  # All chars reserved
        ("a...b", ".", "a\\.\\.\\.b"),  # Repeated reserved chars
        ("hello世界.", ".", "hello世界\\."),  # Non-ASCII chars
        ("helloworld", "^$", "helloworld"),  # Reserved chars not in text
    ],
    ids=[
        "single_reserved_char",
        "multiple_reserved_chars",
        "no_reserved_chars",
        "empty_text",
        "empty_reserved_chars",
        "all_chars_reserved",
        "repeated_reserved_chars",
        "non_ascii_chars",
        "reserved_chars_not_in_text",
    ],
)
def test_re_escape(text, reserved_chars, expected):
    result = re_escape(text, reserved_chars)
    assert result == expected, f"Expected '{expected}', got '{result}'"


@pytest.mark.parametrize(
    "text, expected",
    [
        # Basic text, no formatting
        ("Hello world", "Hello world"),
        # Single * or _ not escaped
        ("Hello *world", "Hello *world"),
        ("Hello _world", "Hello _world"),
        # Paired * or _ escaped
        ("Hello *world*", "Hello \\*world\\*"),
        ("Hello _world_", "Hello \\_world\\_"),
        # Mixed paired and unpaired
        ("*Hello* world *test*", "\\*Hello\\* world \\*test\\*"),
        ("_Hello_ world _test_", "\\_Hello\\_ world \\_test\\_"),
        # Links preserved, no escaping inside
        ("[link](https://example.com)", "[link](https://example.com)"),
        # Text with link and formatting outside
        ("See *this* [link](https://example.com)", "See \\*this\\* [link](https://example.com)"),
        # Formatting inside link text (not escaped)
        ("[*link*](https://example.com)", "[*link*](https://example.com)"),
        # Multiple links with text
        (
            "Text [one](https://a.com) *bold* [two](https://b.com)",
            "Text [one](https://a.com) \\*bold\\* [two](https://b.com)",
        ),
        # Empty string
        ("", ""),
        # Only link
        ("[x](https://x.com)", "[x](https://x.com)"),
        # Complex case with overlapping concerns
        (
            "Start *bold* [link *text*](https://example.com) _underline_ end",
            "Start \\*bold\\* [link *text*](https://example.com) \\_underline\\_ end",
        ),
        # No valid link, treated as text
        ("[text](not-a-url) *bold*", "[text](not-a-url) \\*bold\\*"),
    ],
    ids=[
        "plain_text",
        "single_star",
        "single_underscore",
        "paired_stars",
        "paired_underscores",
        "mixed_stars",
        "mixed_underscores",
        "simple_link",
        "text_with_link",
        "formatting_in_link",
        "multiple_links",
        "empty_string",
        "only_link",
        "complex_case",
        "invalid_link",
    ],
)
def test_sanitize_markdown_v1(text, expected):
    result = sanitize_markdown_v1(text)
    assert result == expected, f"Expected '{expected}', got '{result}'"


def test_draw_percentage_meter_data():
    print()
    for i in range(0, 240, 10):
        print(draw_percentage_meter(i, width=8))


def test_check_time_diff_string_timestamp_positive_diff():
    # Create a timestamp 60 seconds in the past
    past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=60)
    timestamp = past_time.isoformat()
    result = check_time_diff(timestamp)
    # Allow some variance due to execution time (e.g., 59-61 seconds)
    assert 59 <= result.total_seconds() <= 61, f"Expected ~60s, got {result.total_seconds()}s"


def test_check_time_diff_string_timestamp_negative_diff():
    # Create a timestamp 60 seconds in the future
    future_time = datetime.now(tz=timezone.utc) + timedelta(seconds=60)
    timestamp = future_time.isoformat()
    result = check_time_diff(timestamp)
    # Absolute value, allow variance (59-61 seconds)
    assert 59 <= result.total_seconds() <= 61, f"Expected ~60s, got {result.total_seconds()}s"


def test_check_time_diff_datetime_with_tz():
    # Datetime 120 seconds in the past, timezone-aware
    past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=120)
    result = check_time_diff(past_time)
    assert 119 <= result.total_seconds() <= 121, f"Expected ~120s, got {result.total_seconds()}s"


def test_check_time_diff_datetime_without_tz():
    # Datetime 180 seconds in the past, naive (no timezone)
    past_time = datetime.now(tz=timezone.utc) - timedelta(seconds=180)
    naive_time = past_time.replace(tzinfo=None)  # Strip timezone
    result = check_time_diff(naive_time)
    assert 179 <= result.total_seconds() <= 181, f"Expected ~180s, got {result.total_seconds()}s"


def test_check_time_diff_invalid_string_timestamp():
    timestamp = "invalid-timestamp"
    result = check_time_diff(timestamp)
    assert result == timedelta(seconds=0), "Expected 0s for invalid string"


def test_check_time_diff_none_timestamp():
    timestamp = None
    result = check_time_diff(timestamp)
    assert result == timedelta(seconds=0), "Expected 0s for None input"


def test_check_time_diff_seconds_only_removes_microseconds():
    # Test that microseconds are stripped by seconds_only
    now = datetime.now(tz=timezone.utc)
    # Create a timestamp with microseconds
    micro_time = now - timedelta(seconds=5, microseconds=123456)
    result = check_time_diff(micro_time.isoformat())
    # Total seconds should be 5, no fractional part from microseconds
    assert result == timedelta(seconds=5), f"Expected 5s, got {result}"


def test_check_time_diff_large_time_difference():
    # Test a large difference (e.g., 1 hour in the past)
    past_time = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    result = check_time_diff(past_time.isoformat())
    # 1 hour = 3600 seconds, allow small variance
    assert 3599 <= result.total_seconds() <= 3601, (
        f"Expected ~3600s, got {result.total_seconds()}s"
    )


def test_timestamp_inc():
    # Test incrementing a timestamp by 1 second
    base_time = datetime(2023, 10, 1, 12, 0, 0, tzinfo=timezone.utc)
    incremented_time = timestamp_inc(base_time, inc=timedelta(seconds=1))
    expected_time = base_time + timedelta(seconds=1)
    assert next(incremented_time) == expected_time, (
        f"Expected {expected_time}, got {incremented_time}"
    )

    # Test incrementing a timestamp with microseconds
    base_time_with_micro = datetime(2023, 10, 1, 12, 0, 0, 500000, tzinfo=timezone.utc)
    incremented_time_with_micro = timestamp_inc(base_time_with_micro, inc=timedelta(seconds=1))
    expected_time_with_micro = base_time_with_micro + timedelta(seconds=1)
    assert next(incremented_time_with_micro) == expected_time_with_micro, (
        f"Expected {expected_time_with_micro}, got {incremented_time_with_micro}"
    )
    inc_time = timestamp_inc(base_time_with_micro, inc=timedelta(seconds=0.01))
    for i in range(0, 100):
        print(f"Increment {i}: {next(inc_time)}")


def test_main_file_takes_precedence(monkeypatch):
    fake_main = SimpleNamespace(__file__="some/package/app.py")
    monkeypatch.setitem(sys.modules, "__main__", fake_main)

    p = get_entrypoint_filename()
    assert isinstance(p, Path)
    assert p.name == "app.py"

    # cleanup
    monkeypatch.delitem(sys.modules, "__main__", raising=False)


def test_argv_used_when_no_main(monkeypatch):
    # ensure __main__ exists but has no __file__
    monkeypatch.setitem(sys.modules, "__main__", SimpleNamespace())
    monkeypatch.setattr(sys, "argv", ["relative/path/to/script.py"], raising=False)

    p = get_entrypoint_filename()
    assert isinstance(p, Path)
    assert p.name == "script.py"


def test_inline_or_command_returns_none(monkeypatch):
    monkeypatch.setitem(sys.modules, "__main__", SimpleNamespace())
    monkeypatch.setattr(sys, "argv", ["-c"], raising=False)

    p = get_entrypoint_filename()
    assert p.stem == "unknown"


def test_frozen_returns_executable(monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", "/tmp/fakeexec", raising=False)

    p = get_entrypoint_filename()
    assert isinstance(p, Path)
    assert p == Path(sys.executable).resolve()


def test_path_stem_usage():
    # Demonstrate extracting filename without extension
    p = Path("/foo/bar/baz.py")
    assert p.stem == "baz"
