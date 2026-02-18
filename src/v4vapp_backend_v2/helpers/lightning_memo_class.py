import re
from dataclasses import dataclass

LND_INVOICE_PATTERN = re.compile(
    # Accept 'lnbc', 'lntb' or 'lnbcrt' followed by an optional amount token and the '1'
    # bech32 separator, then the bech32 payload characters.
    # Tighten the amount token so the regex does not accidentally treat a short
    # fragment like 'lnbc12340n' as a full invoice by matching a digit '1' inside
    # the amount. The amount portion is either digits with an optional suffix
    # (m/u/n/p) or absent, followed by the literal '1' separator.
    r"(?s)^(?P<before>.*?)(?P<invoice>(?:lnbc|lntb|lnbcrt)(?:\d+[munp]?)?1[0-9ac-hj-np-zAC-HJ-NP-Z]{1,}[0-9a-zA-Z]+)(?P<after>.*)$",
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
        # Only treat it as a full invoice if it's longer than the shorten threshold
        # (head=12 + tail=5 + ellipsis 3 = 20). Shorter matches are likely
        # fragments or already-shortened strings and should be ignored here.
        if len(invoice) > 12 + 5 + 3:
            return _shorten(invoice)
        # fall through if the matched "invoice" is too short (treat as plain memo)

    # Fallback to a short fragment (amount prefix like lnbc277880n)
    # Do NOT match fragments that are part of an already-shortened invoice
    # (e.g. 'lnbc12340n...9klw7'). Use a negative lookahead so we only
    # shorten actual fragments that are not immediately followed by '...'.
    frag = re.search(r"(lnbc\d+[a-zA-Z])(?!\.\.\.)", memo, flags=re.IGNORECASE)
    if frag:
        frag_val = frag.group(1)
        # If the fragment appears inside a larger memo, use the overall memo's tail
        # so output becomes '⚡️<frag>...<last5-of-memo>'. For fragment-only memos,
        # keep the existing _shorten() behaviour.
        if frag.start() > 0 or frag.end() < len(memo):
            return f"⚡️{frag_val}...{memo[-5:]}"
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

    original_memo: str
    before_text: str
    invoice: str
    ln_address: str
    after_text: str
    memo: str
    short_memo: str = ""
    is_lightning_invoice: bool = False
    is_ln_address: bool = False
    is_lightning: bool = False

    def __init__(self, memo: str | None = None):
        self.original_memo = memo if memo else ""
        match = LND_INVOICE_PATTERN.match(memo) if memo else None
        match_address = LIGHTNING_ADDRESS_PATTERN.match(memo) if memo else None

        # Invoice case — only accept as a full invoice if the matched string is
        # longer than the shorten threshold (head=12 + tail=5 + ellipsis 3).
        if match:
            candidate = match.group("invoice")
            if len(candidate) > 12 + 5 + 3:
                self.before_text = match.group("before")
                self.invoice = candidate
                self.ln_address = ""
                self.after_text = match.group("after")
                self.short_memo = _lightning_memo(self.invoice)
                self.is_lightning_invoice = True
                self.is_ln_address = False
                self.memo = memo if memo else ""
                self.is_lightning = True

                return

        # Lightning address case
        if match_address:
            self.before_text = match_address.group("before")
            self.invoice = ""
            self.ln_address = match_address.group("ln_address")
            self.after_text = match_address.group("after")
            self.short_memo = _lightning_memo(self.ln_address)
            self.is_ln_address = True
            self.is_lightning_invoice = False
            self.memo = memo if memo else ""
            self.is_lightning = True
            return

        # No invoice or address — default behavior
        self.before_text = ""
        self.invoice = ""
        self.ln_address = ""
        self.after_text = ""
        self.short_memo = _lightning_memo(memo) if memo else ""
        self.is_lightning_invoice = False
        self.is_ln_address = False
        self.memo = memo if memo else ""
        self.is_lightning = False
