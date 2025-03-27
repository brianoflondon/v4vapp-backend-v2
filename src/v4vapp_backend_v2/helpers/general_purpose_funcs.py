import re
from datetime import datetime, timedelta, timezone


def snake_case(name: str) -> str:
    """
    Convert a string to snake_case.
    Args:
        name (str): The string to convert.
    Returns:
        str: The string converted to snake_case.
    """
    return "".join(["_" + i.lower() if i.isupper() else i for i in name]).lstrip("_")


def camel_case(snake_str: str) -> str:
    """
    Convert a snake_case string to camelCase.
    Args:
        snake_str (str): The snake_case string to convert.
    Returns:
        str: The string converted to camelCase.
    """
    components = snake_str.split("_")
    return components[0] + "".join(x.title() for x in components[1:])


def cap_camel_case(snake_str: str) -> str:
    """
    Convert a snake_case string to CamelCase.
    Args:
        snake_str (str): The snake_case string to convert.
    Returns:
        str: The string converted to CamelCase.
    """
    camel_case_word = camel_case(snake_str)
    return camel_case_word[0].upper() + camel_case_word[1:]


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


def format_time_delta(delta: timedelta, fractions: bool = False) -> str:
    """
    Formats a timedelta object as a string.
    If Days are present, the format is "X days, Y hours".
    Otherwise, the format is "HH:MM:SS".
    Args:
        delta (timedelta): The timedelta object to format.

    Returns:
        str: The formatted string.
    """
    if delta.days:
        return f"{delta.days} days, {delta.seconds // 3600} hours"
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if fractions:
        return f"{hours:02}:{minutes:02}:{seconds:02}.{delta.microseconds // 1000:03}"
    return f"{hours:02}:{minutes:02}:{seconds:02}"


def get_in_flight_time(creation_date: datetime) -> str:
    """
    Calculate the time in flight for a given datetime object.
    Args:
        creation_date (datetime): The datetime object to calculate
        the time in flight for.

    Returns:
        str: The formatted string representing the timedelta.
    """

    current_time = datetime.now(tz=timezone.utc)

    if current_time < creation_date:
        in_flight_time = format_time_delta(timedelta(seconds=0.1))
    else:
        in_flight_time = format_time_delta(current_time - creation_date)

    return in_flight_time


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


# MARK: Markdown Functions

# MARKDOWN FUNCTIONS
# -----------------
# The following functions are used to detect and sanitize Markdown-like
# formatting in messages. They are used to determine if a message should
# be sent with Markdown parse mode, and to sanitize the message text to
# avoid formatting issues in Telegram.


def is_markdown(message: str) -> bool:
    """
    Check if a message contains common Markdown formatting patterns.
    Returns True if Markdown-like syntax is detected, False otherwise.
    Args:
        message (str): The message to be checked for Markdown syntax.
    Returns:
        bool: True if Markdown-like syntax is detected, False otherwise.
    The function checks for the following Markdown patterns:
        - [text](link): Markdown hyperlinks
        - **bold**: Bold text
        - *italic*: Italic text
        - _italic_: Italic text
        - `code`: Inline code
        - ```code blocks```: Code blocks
        - # Heading: Headings (1-6 #'s followed by space)
        - - Lists: Unordered lists
        - 1. Lists: Ordered lists
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


# def draw_percentage_meter(
#     percentage: int, max_percent: int = 200, width: int = 20
# ) -> str:
#     """
#     Draws a text-based percentage meter from 0% to max_percent

#     Args:
#         percentage (float): The current percentage (e.g., 175)
#         max_percent (float): Maximum percentage scale (default 200)
#         width (int): Width of the meter in characters (default 50)
#     """
#     # Ensure percentage stays within bounds
#     draw_percentage = max(0, min(percentage, max_percent))

#     # Calculate the filled portion
#     filled_width = int(width * draw_percentage / max_percent)
#     empty_width = width - filled_width

#     # Create the meter using Unicode block characters
#     meter = "█" * filled_width + "░" * empty_width

#     # Calculate percentage string
#     percent_str = f"{percentage:.0f}%"

#     # Combine everything with some styling
#     return f"[{meter}] {percent_str:>6} / {max_percent:.0f}%"


def draw_percentage_meter(percentage, max_percent=200, width=20):
    """
    Draws a fine-grained percentage meter using fractional block characters

    Args:
        percentage (float): Current percentage (e.g., 175)
        max_percent (float): Maximum percentage (default 200)
        width (int): Width in characters (default 20)
    """
    # Bound the percentage
    percentage = max(0, min(percentage, max_percent))

    # Unicode block characters from empty to full (8 levels + empty)
    blocks = [" ", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]

    # Total "units" of resolution (width * 8 since each char has 8 levels)
    total_units = width * 8
    filled_units = int(total_units * percentage / max_percent)

    meter = ""
    remaining_units = filled_units

    # Build the meter character by character
    for _ in range(width):
        if remaining_units >= 8:
            meter += blocks[8]  # Full block
            remaining_units -= 8
        elif remaining_units > 0:
            meter += blocks[remaining_units]  # Partial block
            remaining_units = 0
        else:
            meter += blocks[0]  # Empty

    percent_str = f"{percentage:.0f}%"
    return f"[{meter}] {percent_str:>6} / {max_percent}%"
