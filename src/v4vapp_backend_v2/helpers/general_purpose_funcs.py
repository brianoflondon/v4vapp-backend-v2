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
