import re
from dataclasses import dataclass

LND_INVOICE_PATTERN = re.compile(
    # Accept 'lnbc', 'lntb' or 'lnbcrt' followed by an optional amount token and the '1'
    # bech32 separator, then the bech32 payload characters. This will find invoices
    # embedded anywhere in a memo and supports common amount tokens like '31310n'.
    r"(?s)^(?P<before>.*?)(?P<invoice>(?:lnbc|lntb|lnbcrt)[0-9a-zA-Z]*1[0-9ac-hj-np-zAC-HJ-NP-Z]{1,}[0-9a-zA-Z]+)(?P<after>.*)$",
    re.IGNORECASE,
)

LIGHTNING_ADDRESS_PATTERN = re.compile(
    # Find a lightning address (internet identifier) anywhere in the memo and capture
    # surrounding text in `before`/`after` and the address in `ln_address`.
    # Accept optional `⚡`/`⚡️` or `lightning:` prefix immediately before the address.
    r"(?s)^(?P<before>.*?)(?:\u26A1\uFE0F|\u26A1|lightning:)?(?P<ln_address>[A-Za-z0-9_+%\-]+(?:\.[A-Za-z0-9_+%\-]+)*@(?:[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63})(?P<after>.*)$",
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
    """
    Dataclass that parses and stores lightning invoice information from a memo string.

    This class extracts lightning invoice details from a memo field using pattern matching.
    If an invoice is found, it separates the memo into before, invoice, and after text components.
    It also generates a short memo representation from the invoice or original memo.

    Attributes:
        before_text (str): Text appearing before the lightning invoice in the memo.
        invoice (str): The extracted lightning invoice string.
        after_text (str): Text appearing after the lightning invoice in the memo.
        memo (str): The original memo string.
        short_memo (str): A shortened version of the memo derived from the invoice or original memo.
                        Defaults to an empty string.

    Args:
        memo (str): The memo string to parse for lightning invoice information.
    """

    before_text: str
    invoice: str
    ln_address: str
    after_text: str
    memo: str
    short_memo: str = ""
    is_lightning_invoice: bool = False
    is_ln_address: bool = False
    is_lightning: bool = False

    def __init__(self, memo: str):
        match = LND_INVOICE_PATTERN.match(memo)
        match_address = LIGHTNING_ADDRESS_PATTERN.match(memo)
        if match:
            self.before_text = match.group("before")
            self.invoice = match.group("invoice")
            self.ln_address = ""
            self.after_text = match.group("after")
            self.short_memo = _lightning_memo(self.invoice)
            self.is_lightning_invoice = True
        elif match_address:
            self.before_text = match_address.group("before")
            self.invoice = ""
            self.ln_address = match_address.group("ln_address")
            self.after_text = match_address.group("after")
            self.short_memo = _lightning_memo(self.ln_address)
            self.is_ln_address = True
        else:
            self.before_text = ""
            self.invoice = ""
            self.ln_address = ""
            self.after_text = ""
            self.short_memo = _lightning_memo(memo)
            self.is_lightning_invoice = False
        self.memo = memo
        self.is_lightning = self.is_lightning_invoice or self.is_ln_address
        
