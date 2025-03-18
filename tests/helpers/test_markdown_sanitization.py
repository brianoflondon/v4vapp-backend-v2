import re

import pytest

from v4vapp_backend_v2.helpers.general_purpose_funcs import (  # Adjust import path as needed
    re_escape,
    sanitize_markdown_v1,
)


# Tests for re_escape
def test_re_escape_basic():
    """Test basic escaping of reserved characters."""
    text = "Hello *world* _test_"
    reserved_chars = "[*_]"
    result = re_escape(text, reserved_chars)
    assert result == "Hello \\*world\\* \\_test\\_"


def test_re_escape_no_reserved_chars():
    """Test text with no reserved characters."""
    text = "Hello world!"
    reserved_chars = "[*_]"
    result = re_escape(text, reserved_chars)
    assert result == "Hello world!"


def test_re_escape_empty_string():
    """Test empty string input."""
    text = ""
    reserved_chars = "[*_]"
    result = re_escape(text, reserved_chars)
    assert result == ""


def test_re_escape_different_reserved_chars():
    """Test with different set of reserved characters."""
    text = "Hello.world#test"
    reserved_chars = "[.#]"
    result = re_escape(text, reserved_chars)
    assert result == "Hello\\.world\\#test"


def test_re_escape_emoji():
    """Test text with emoji (should not be escaped)."""
    text = "🐝 Hello *world*"
    reserved_chars = "[*_]"
    result = re_escape(text, reserved_chars)
    assert result == "🐝 Hello \\*world\\*"


# Tests for sanitize_markdown_v1
def test_sanitize_markdown_v1_basic():
    """Test basic text with no links."""
    text = "Hello *world* _test_"
    result = sanitize_markdown_v1(text)
    assert result == "Hello \\*world\\* \\_test\\_"


def test_sanitize_markdown_v1_with_link():
    """Test text with a single Markdown V1 link."""
    text = "Check [Google](http://google.com) now"
    result = sanitize_markdown_v1(text)
    assert result == "Check [Google](http://google.com) now"


def test_sanitize_markdown_v1_link_with_reserved():
    """Test text with reserved characters and a link."""
    text = "See *this* [Link](http://example.com) _here_"
    result = sanitize_markdown_v1(text)
    assert result == "See \\*this\\* [Link](http://example.com) \\_here\\_"


def test_sanitize_markdown_v1_multiple_links():
    """Test text with multiple links and reserved characters."""
    text = "*Start* [One](http://one.com) middle [Two](http://two.com) _end_"
    result = sanitize_markdown_v1(text)
    assert (
        result
        == "\\*Start\\* [One](http://one.com) middle [Two](http://two.com) \\_end\\_"
    )


def test_sanitize_markdown_v1_emoji_and_link():
    """Test text with emoji, link, and reserved characters."""
    text = "🐝 Check *this* [HiveHub](https://hivehub.dev/tx/123) now"
    result = sanitize_markdown_v1(text)
    assert result == "🐝 Check \\*this\\* [HiveHub](https://hivehub.dev/tx/123) now"


def test_sanitize_markdown_v1_real_example():
    """Test the specific notification text."""
    text = "🐝 flemingfarm sent 13.000 HIVE to v4vapp ($3.17) flemingfarm Deposit to #SATS [HiveHub](https://hivehub.dev/tx/8eb1e0dd4259bfce72d5cbe3a6ce347fba777d8e) no_preview"
    result = sanitize_markdown_v1(text)
    assert (
        result
        == "🐝 flemingfarm sent 13.000 HIVE to v4vapp ($3.17) flemingfarm Deposit to #SATS [HiveHub](https://hivehub.dev/tx/8eb1e0dd4259bfce72d5cbe3a6ce347fba777d8e) no_preview"
    )


def test_sanitize_markdown_v1_empty():
    """Test empty string."""
    text = ""
    result = sanitize_markdown_v1(text)
    assert result == ""


def test_sanitize_markdown_v1_invalid_link():
    """Test text with malformed link (should still process correctly)."""
    text = "Broken [link(http://example.com) *text*"
    result = sanitize_markdown_v1(text)
    assert result == "Broken [link(http://example.com) \\*text\\*"


def test_sanitize_markdown_v1_nested_brackets():
    """Test text with nested brackets (not a valid link, should escape)."""
    text = "[Not a *link*](not a url) [Real](http://real.com)"
    result = sanitize_markdown_v1(text)
    assert result == "[Not a \\*link\\*](not a url) [Real](http://real.com)"
