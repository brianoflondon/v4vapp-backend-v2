import re
from datetime import timedelta


def seconds_only(delta: timedelta) -> timedelta:
    """
    Returns a new timedelta object with only the days and seconds
    components of the input timedelta.

    Args:
        delta (timedelta): The input timedelta object.

    Returns:
        timedelta: A new timedelta object with only the days and seconds
        components of the input timedelta.
    """
    return timedelta(days=delta.days, seconds=delta.seconds)


def detect_keepsats(memo: str) -> bool:
    """
    Detects if the given memo contains keywords related to keeping sats.
    Notice does not find any occurance of "sats" as a bare word only.

    Args:
        memo (str): The memo to be checked.

    Returns:
        bool: True if the memo contains keywords related to keeping sats,
        False otherwise.
    """
    if not memo:
        return False
    if (
        "#sats" in memo.lower()
        or memo.lower().startswith("sats")
        or "#keepsats" in memo.lower()
        or memo.lower().startswith("keepsats")
    ):
        return True
    return False


def detect_paywithsats(memo: str) -> bool:
    """
    Detects if the given memo contains the phrase '#paywithsats'.
    This can't be at start of a memo, it should always follow the Lightning
    invoice or address which needs to be paid.

    Args:
        memo (str): The memo to check.

    Returns:
        bool: True if the memo contains '#paywithsats', False otherwise.
    """
    if not memo:
        return False
    if "#paywithsats" in memo.lower():
        return True
    return False


def detect_hbd(memo: str) -> bool:
    if not memo:
        return False
    if "#hbd" in memo.lower():
        return True
    return False


def detect_convert_keepsats(memo: str) -> bool:
    """
    Detects if the given memo contains the phrase '#convertkeepsats'.

    Args:
        memo (str): The memo to check.

    Returns:
        bool: True if the memo contains '#convertkeepsats', False otherwise.
    """
    if not memo:
        return False
    if "#convertkeepsats" in memo.lower():
        return True
    return False


def is_markdown(message: str) -> bool:
    """
    Check if a message contains common Markdown formatting patterns.
    Returns True if Markdown-like syntax is detected, False otherwise.
    """
    # Common Markdown patterns
    patterns = [
        r"\[.+?\]\(.+?\)",  # [text](link) - Markdown hyperlinks
        r"\*\*.+?\*\*",  # **bold**
        r"\*.+?\*",  # *italic*
        r"_.+?_",  # _italic_
        r"`.+?`",  # `code`
        r"```[\s\S]*?```",  # ```code blocks```
        r"^#{1,6}\s",  # # Heading (1-6 #'s followed by space)
        r"^\s*[-+*]\s",  # - Lists (unordered)
        r"^\s*\d+\.\s",  # 1. Lists (ordered)
    ]

    # Check if any pattern matches in the message
    for pattern in patterns:
        if re.search(pattern, message, re.MULTILINE):
            return True
    return False


def re_escape(text: str, reserved_chars: str) -> str:
    """
    Escape reserved characters in the text, but only for specified chars.

    Args:
        text (str): The text to escape.
        reserved_chars (str): Characters to escape.

    Returns:
        str: The escaped text.
    """
    return "".join(f"\\{c}" if c in reserved_chars else c for c in text)


def sanitize_markdown_v1(text: str) -> str:
    """
    Sanitize text for Telegram's Markdown (V1) parse mode.
    Escapes '*' and '_' only when necessary, preserves links.

    Args:
        text (str): The input text to sanitize.

    Returns:
        str: The sanitized text compatible with Markdown V1.
    """
    # Reserved characters in Markdown V1 that might need escaping
    reserved_chars = "*_"  # Only escape * and _, not [ or ]

    # Pattern to match Markdown V1 link syntax: [text](url)
    link_pattern = r"\[([^\[\]]*?)\]\((https?://[^\s()]+?)\)"

    # Split text into parts: links and non-link segments
    parts = []
    last_pos = 0
    for match in re.finditer(link_pattern, text):
        start, end = match.span()
        # Add text before the link (sanitize it if needed)
        if last_pos < start:
            segment = text[last_pos:start]
            # Only escape if '*' or '_' could be misinterpreted as formatting
            if re.search(r"\*[^\*]+\*|_[^_]+_", segment):  # Check for paired * or _
                parts.append(re_escape(segment, reserved_chars))
            else:
                parts.append(segment)  # No escaping needed
        # Add the link as-is
        parts.append(text[start:end])
        last_pos = end
    # Add any remaining text after the last link
    if last_pos < len(text):
        segment = text[last_pos:]
        if re.search(r"\*[^\*]+\*|_[^_]+_", segment):  # Check for paired * or _
            parts.append(re_escape(segment, reserved_chars))
        else:
            parts.append(segment)  # No escaping needed

    return "".join(parts)
