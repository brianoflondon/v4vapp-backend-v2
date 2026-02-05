import re

import pytest

from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo, _lightning_memo


def test_lightningmemo_parses_full_invoice_and_shortens_invoice_component():
    invoice = "lnbc1a" + "X" * 20 + "abcde"
    memo = f"before {invoice} after"

    lm = LightningMemo(memo)

    assert lm.invoice == invoice
    assert "before" in lm.before_text
    assert "after" in lm.after_text
    # LightningMemo uses _lightning_memo on the invoice string itself, so the tail comes from the invoice
    assert lm.short_memo == f"⚡️lnbc1a...abcde"


def test_lightningmemo_no_invoice_returns_chat_bubble_prefix():
    memo = "hello there"
    lm = LightningMemo(memo)

    assert lm.invoice == ""
    assert lm.before_text == ""
    assert lm.after_text == ""
    assert lm.short_memo == "💬hello there"


def test_lightning_memo_with_fragment_only_shortens_fragment_and_uses_memo_tail():
    frag = "lnbc1234n"
    # calling with fragment only should produce a short form that includes the fragment and the last 5 chars
    out = _lightning_memo(frag)
    assert out == f"⚡️{frag}...{frag[-5:]}"


def test_lightning_memo_matches_fragment_inside_full_memo_and_uses_overall_tail():
    frag = "lnbc1234n"
    memo = f"pay {frag} thanks"
    out = lightning_memo(memo)
    # when called on full memo the helper uses the overall memo's tail for the '...<last5>' portion
    assert out == f"⚡️{frag}...{memo[-5:]}"
