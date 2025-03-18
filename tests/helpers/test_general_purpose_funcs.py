from datetime import timedelta

import pytest

from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    detect_convert_keepsats,
    detect_hbd,
    detect_keepsats,
    detect_paywithsats,
    is_markdown,
    seconds_only,
)


def test_seconds_only():
    delta = timedelta(days=1, hours=2, minutes=3, seconds=4, microseconds=500)
    result = seconds_only(delta)
    expected = timedelta(days=1, seconds=7384)  # 2 hours, 3 minutes, and 4 seconds
    assert result == expected


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
