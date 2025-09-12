import decimal
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Generator

from bson.decimal128 import Decimal128

from v4vapp_backend_v2.config.setup import logger


# MARK: General Text
def snake_case(name: str) -> str:
    """
    Convert a string to snake_case.
    Args:
        name (str): The string to convert.
    Returns:
        str: The string converted to snake_case.
    """
    return "".join(["_" + i.lower() if i.isupper() else i for i in name]).lstrip("_")


def from_snake_case(snake_str: str) -> str:
    """
    Convert a snake_case string to a human-readable format.
    Args:
        snake_str (str): The snake_case string to convert.
    Returns:
        str: The string converted to a human-readable format.
    """
    return " ".join(word.capitalize() for word in snake_str.split("_"))


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


# MARK: Database


# def convert_decimals(obj):
#     """
#     Recursively converts all Decimal instances within a nested structure (dicts, lists) to floats.

#     Args:
#         obj: The input object, which can be a dict, list, Decimal, or any other type.

#     Returns:
#         The input object with all Decimal instances converted to floats. The structure of dicts and lists is preserved.

#     Example:
#         >>> from decimal import Decimal
#         >>> convert_decimals({'a': Decimal('1.1'), 'b': [Decimal('2.2'), 3]})
#         {'a': 1.1, 'b': [2.2, 3]}
#     """
#     if isinstance(obj, dict):
#         return {k: convert_decimals(v) for k, v in obj.items()}
#     elif isinstance(obj, list):
#         return [convert_decimals(item) for item in obj]
#     elif isinstance(obj, Decimal):
#         return float(obj)  # Or str(obj) if you want string precision
#     else:
#         return obj


def convert_decimals(obj):
    """
    Recursively converts Decimal instances within a nested structure (dicts, lists) to appropriate MongoDB types:
    - Whole-number Decimals to Python int (for MongoDB int64).
    - Fractional Decimals to bson.Decimal128 (for MongoDB Decimal128).
    - Preserves other types and nested structures.

    Args:
        obj: The input object, which can be a dict, list, Decimal, or any other type.

    Returns:
        The input object with all Decimal instances converted to int or Decimal128 as appropriate.
        The structure of dicts and lists is preserved.

    Example:
        >>> from decimal import Decimal
        >>> from bson.decimal128 import Decimal128
        >>> convert_decimals({'a': Decimal('12345678901234567890'), 'b': Decimal('1.23'), 'c': [Decimal('2.0'), 3]})
        {'a': 12345678901234567890, 'b': Decimal128('1.23'), 'c': [2, 3]}
    """
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, Decimal):
        # Check if the Decimal is a whole number (no fractional part)
        if obj == obj.to_integral_value():
            return int(obj)  # Convert to Python int for MongoDB int64
        else:
            try:
                return Decimal128(str(obj))  # Convert to Decimal128 for MongoDB
            except decimal.Inexact:
                # If Decimal128 conversion fails due to precision issues,
                # round to 6 decimal places and try again
                rounded_obj = round(obj, 6)
                try:
                    return Decimal128(str(rounded_obj))
                except Exception as e:
                    logger.warning(
                        f"Failed to convert rounded Decimal {rounded_obj} to Decimal128: {e}, converting to float"
                    )
                    return float(rounded_obj)
            except Exception as e:
                # If Decimal128 conversion fails, convert to float as fallback
                logger.warning(
                    f"Failed to convert Decimal {obj} to Decimal128: {e}, converting to float"
                )
                return float(obj)
    else:
        return obj


# MARK: Date & Time
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


def format_time_delta(
    delta: timedelta | float | int, fractions: bool = False, just_days_or_hours: bool = False
) -> str:
    """
    Formats a timedelta object as a string.
    If Days are present, the format is "X days, Y hours".
    Otherwise, the format is "HH:MM:SS".
    Args:
        delta (timedelta): The timedelta object to format.

    Returns:
        str: The formatted string.
    """
    if isinstance(delta, (int, float)):
        delta = timedelta(seconds=delta)
    if delta.days:
        hours = delta.seconds // 3600
        if hours == 0 and just_days_or_hours:
            return f"{delta.days} {'days' if delta.days != 1 else 'day'}"

        return (
            f"{delta.days} {'days' if delta.days != 1 else 'day'}, {delta.seconds // 3600} hours"
        )
    hours, remainder = divmod(delta.seconds, 3600)
    if just_days_or_hours:
        return f"{hours} {'hours' if hours != 1 else 'hour'}"
    minutes, seconds = divmod(remainder, 60)
    hours_text = f"{hours:02}:" if hours > 0 else ""
    if fractions:
        return f"{hours_text}{minutes:02}:{seconds:02}.{delta.microseconds // 1000:03}"
    return f"{hours_text}{minutes:02}:{seconds:02}"


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


# MARK: Memo processing
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


def paywithsats_amount(memo: str) -> int:
    """
    Extracts the amount specified in a memo string formatted as "paywithsats:amount".
    Args:
        memo (str): The memo string containing the amount, expected in the format "paywithsats:amount".
    Returns:
        int: The extracted amount as an integer if found; otherwise, 0.
    """

    # Extract the amount from the memo, which is expected to be in the format "paywithsats:amount"
    match = re.search(r"paywithsats:(\d+)", memo)
    if match:
        return int(match.group(1))
    return 0


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


def is_clean_memo(memo: str) -> bool:
    """
    Checks if the memo contains the '#clean' tag.
    Args:
        memo (str): The memo to check.
    Returns:
        bool: True if the memo contains '#clean', False otherwise.
    """
    if not memo:
        return False
    if "#clean" in memo.lower():
        return True
    return False


def process_clean_memo(
    memo: str,
) -> str:
    """
    Cleans and processes a memo string by performing several transformations:
    1. Removes the first word (typically a Hive account name).
    2. Strips specified hashtags (e.g., '#v4vapp') from the message.
    3. Removes leading '- ' or '| ' if present.
    4. If the message is identified as a 'clean memo' (via is_clean_memo), further processes:
        - If detect_keepsats returns True, removes content after ' | ', strips '#clean', appends ' | #sats',
          and adds a transaction code if detected (e.g., 'v4v-xxxx').
        - Otherwise, removes content after ' | ' and strips '#clean'.
    Args:
        memo (str): The original memo string to be cleaned and processed.
    Returns:
        str: The cleaned and processed memo string.
    """
    if memo.startswith("lnbc"):
        return memo
    message = memo
    # Strip the Hive account name from the message:
    s = " "
    message = s.join(message.split()[1:])

    # Remove #tags
    remove = ["#v4vapp"]
    for r in remove:
        message = message.replace(r, "").strip()

    if message.startswith("- ") or message.startswith("| "):
        message = message[2:]

    if message.endswith(" |"):
        message = message[:-2]

    if is_clean_memo(message):
        if detect_keepsats(message):
            message = message.split(" | ")[0]
            message = message.replace("#clean", "").strip()
            message = f"{message} | #sats"
            # Detect special case of POS v4vapp looking the #
            transaction_checkCode = re.findall(r"v4v-\w+", message)
            if transaction_checkCode:
                message = f"{message} | {transaction_checkCode[0]}"
        else:
            message = message.split(" | ")[0]
            message = message.replace("#clean", "").strip()

    return message


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


def sanitize_markdown_v2(text: str) -> str:
    """
    Sanitizes a string for Telegram's MarkdownV2 format by escaping reserved characters,
    while preserving URLs in [text](url) links.

    Args:
        text (str): The input text to sanitize.

    Returns:
        str: The sanitized text ready for MarkdownV2 parsing.
    """
    # MarkdownV2 reserved characters that need escaping
    reserved_chars = r"([_*[\]()~`#+=.!|{}>-])"

    # Step 1: Extract and preserve URLs in [text](url) links
    link_pattern = r"\[([^\]]*)\]\(([^)]+)\)"
    links = []

    def store_link(match):
        links.append((match.group(1), match.group(2)))  # Store link text and URL
        return f"__LINK_{len(links) - 1}__"

    text = re.sub(link_pattern, store_link, text)

    # Step 2: Escape reserved characters in the remaining text
    text = re.sub(reserved_chars, r"\\\1", text)

    # Step 3: Restore links
    for i, (link_text, url) in enumerate(links):
        # Escape reserved characters in link_text
        link_text_escaped = re.sub(reserved_chars, r"\\\1", link_text)
        # URLs generally don't need escaping, but ensure no unescaped ) breaks the link
        placeholder = f"__LINK_{i}__"
        # Escape underscores in placeholder to match escaped text
        placeholder_escaped = placeholder.replace("_", r"\_")
        text = text.replace(placeholder_escaped, f"[{link_text_escaped}]({url})")

    return text


def draw_percentage_meter(percentage, max_percent=200, width=20):
    """
    Draws a fine-grained percentage meter using fractional block characters

    Args:
        percentage (float): Current percentage (e.g., 175)
        max_percent (float): Maximum percentage (default 200)
        width (int): Width in characters (default 20)
    """
    # Bound the percentage
    percentage_calc = max(0, min(percentage, max_percent))

    # Unicode block characters from empty to full (8 levels + empty)
    blocks = [" ", "▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"]

    # Total "units" of resolution (width * 8 since each char has 8 levels)
    total_units = width * 8
    filled_units = int(total_units * percentage_calc / max_percent)

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


def seconds_only_time_diff(timestamp: datetime) -> timedelta:
    """
    Calculate the absolute time difference between the current time and a given timestamp.
    Removes the milliseconds from the timedelta.

    Args:
        timestamp (datetime): The timestamp to compare with the current time.

    Returns:
        timedelta: The absolute difference between the current time and the given timestamp.
    """
    return abs(seconds_only(datetime.now(tz=timezone.utc) - timestamp))


def check_time_diff(timestamp: str | datetime) -> timedelta:
    """
    Calculate the difference between the current time and a given timestamp
    Removes the milliseconds from the timedelta.

    Args:
        timestamp (str | datetime): The timestamp in ISO format or datetime object () to
        compare with the current time. Forces UTC if not timezone aware.

    Returns:
        timedelta: The absolute difference between the current time and the given timestamp.

    Logs a warning if the time difference is greater than 1 minute.
    """
    try:
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp).replace(tzinfo=timezone.utc)
        else:
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        time_diff = seconds_only_time_diff(timestamp)
        # Ensure the timedelta is always positive
    except (ValueError, AttributeError, OverflowError, TypeError):
        time_diff = timedelta(seconds=0)
    return time_diff


def sanitize_filename(filename: str) -> str:
    """
    Sanitizes a file name by replacing spaces with underscores and removing
    or replacing invalid characters to make it compatible with Unix-based systems.

    Args:
        filename (str | Path): The original file name.

    Returns:
        Path: The sanitized file name.
    """

    # Replace spaces with underscores
    sanitized = str(filename).replace(" ", "_")
    # Remove invalid characters (anything other than alphanumeric, underscores, hyphens, or dots)
    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "", sanitized)
    return sanitized


def lightning_memo(memo: str) -> str:
    """
    Removes and shortens a lightning invoice from a memo for output.

    Returns:
        str: The shortened memo string.
    """
    # Regex pattern to capture 'lnbc' followed by numbers and one letter
    pattern = r"(lnbc\d+[a-zA-Z])"
    match = re.search(pattern, memo)
    if match:
        # Replace the entire memo with the matched lnbc pattern
        memo = f"⚡️{match.group(1)}...{memo[-5:]}"
    else:
        memo = f"💬{memo}"
    return memo


def truncate_text(text: str, max_length: int, centered: bool = False) -> str:
    """
    Truncates a given text to a specified maximum length, optionally centering it.

    If the text exceeds the maximum length, it is truncated and appended with '...'.
    If the `centered` parameter is set to True, the truncated text is centered within
    the specified maximum length.

    Args:
        text (str): The input text to be truncated.
        max_length (int): The maximum allowed length of the text, including the ellipsis.
        centered (bool, optional): Whether to center the truncated text. Defaults to False.

    Returns:
        str: The truncated (and optionally centered) text.
    """
    if centered:
        text = text[: max_length - 3] + "..." if len(text) > max_length else text
        return text.center(max_length)
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def find_short_id(text: str) -> str | None:
    """
    Finds a short ID in the given text.
    A short ID is defined as a string that starts with '§' and is followed by alphanumeric characters.

    Args:
        text (str): The input text to search for a short ID.

    Returns:
        str | None: The found short ID or None if no valid ID is found.
    """
    match = re.search(r"§\s*([a-zA-Z0-9_]+)", text)
    if match:
        return match.group(1)
    return None


def timestamp_inc(
    start_time: datetime, inc: timedelta = timedelta(seconds=0.01)
) -> Generator[datetime, None, None]:
    """
    Depreciated: not using it
    Increment a timestamp by a given timedelta.

    Args:
        start_time (datetime): The initial timestamp.
        inc (timedelta): The increment to apply.

    Returns:
        datetime: The incremented timestamp.
    """
    if start_time is None:
        start_time = datetime.now(tz=timezone.utc)

    current_time = start_time
    while True:
        current_time += inc
        yield current_time


# Test with your message
if __name__ == "__main__":
    message = "🐝 🧱 Delta 0:55:33 | Mean 0:55:43 | producer_reward | 1 | [HiveHub](https://hivehub.dev/tx/95024715/0000000000000000000000000000000000000000/1) | 0:00:02"
    sanitized = sanitize_markdown_v2(message)
    print("Original:", message)
    print("Sanitized:", sanitized)
    print("Sanitized:", sanitized)
    print("Sanitized:", sanitized)
    print("Sanitized:", sanitized)
