import re
from dataclasses import dataclass

LND_INVOICE_PATTERN = re.compile(
    r"(?s)^(?P<before>.*?)(?P<invoice>(?:lnbc1|lntb1|lnbcrt1)[0-9ac-hj-np-zAC-HJ-NP-Z]{1,}[0-9a-zA-Z]+)(?P<after>.*)$",
    re.IGNORECASE,
)


def _lightning_memo(memo: str) -> str:
    """
    Removes and shortens a lightning invoice from a memo for output.
    If no invoice is found, it just adds a chat bubble to the start.

    Returns:
        str: The shortened memo string or just the original memo with a chat bubble.
    """
    # Regex pattern to capture 'lnbc' followed by numbers and one letter
    pattern = r"(lnbc\d+[a-zA-Z])"
    match = re.search(pattern, memo)
    if match:
        # Replace the entire memo with the matched lnbc pattern
        memo = f"⚡️{match.group(1)}...{memo[-5:]}"
    else:
        if not memo.startswith("💬"):
            memo = f"💬{memo}"
    return memo


@dataclass
class LightningMemo:
    before_text: str
    invoice: str
    after_text: str
    memo: str
    short_memo: str = ""

    def __init__(self, memo: str):
        match = LND_INVOICE_PATTERN.match(memo)
        if match:
            self.before_text = match.group("before")
            self.invoice = match.group("invoice")
            self.after_text = match.group("after")
            self.short_memo = _lightning_memo(self.invoice)
        else:
            self.before_text = ""
            self.invoice = ""
            self.after_text = ""
            self.short_memo = _lightning_memo(memo)
        self.memo = memo
