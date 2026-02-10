import re
from dataclasses import dataclass

LND_INVOICE_PATTERN = re.compile(
    # Accept 'lnbc', 'lntb' or 'lnbcrt' followed by an optional amount token and the '1'
    # bech32 separator, then the bech32 payload characters. This will find invoices
    # embedded anywhere in a memo and supports common amount tokens like '31310n'.
    r"(?s)^(?P<before>.*?)(?P<invoice>(?:lnbc|lntb|lnbcrt)[0-9a-zA-Z]*1[0-9ac-hj-np-zAC-HJ-NP-Z]{1,}[0-9a-zA-Z]+)(?P<after>.*)$",
    re.IGNORECASE,
)


def _lightning_memo(memo: str) -> str:
    """
    Removes and shortens a lightning invoice from a memo for output.
    If no invoice is found, it just adds a chat bubble to the start.

    Returns:
        str: The shortened invoice representation (prefixed with ⚡️) or the
             original memo with a chat bubble when no invoice is present.
    """

    def _shorten(pay_req: str, head: int = 12, tail: int = 5) -> str:
        if len(pay_req) <= head + tail + 3:
            return f"⚡️{pay_req}"
        return f"⚡️{pay_req[:head]}...{pay_req[-tail:]}"

    # Try to find a full invoice first
    match = LND_INVOICE_PATTERN.match(memo)
    if match:
        invoice = match.group("invoice")
        return _shorten(invoice)

    # Fallback to a short fragment (amount prefix like lnbc277880n)
    frag = re.search(r"(lnbc\d+[a-zA-Z])", memo, flags=re.IGNORECASE)
    if frag:
        frag_val = frag.group(1)
        return _shorten(frag_val)

    # No invoice found -> prefix with chat bubble
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
