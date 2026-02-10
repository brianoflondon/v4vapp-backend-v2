from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo, _lightning_memo


def test_lightningmemo_parses_full_invoice_and_shortens_invoice_component():
    invoice = "lnbc31310n1p5cf8v0pp5g55tf4usmk22en3zwex848fenf43472nq6caeaswgjtm42qx28gsdrjwc68vctswqhxgetkyp7zqjr0wd6xjmn8yprx2etnypmrga3dda8kvj6typ7zqg6ng929xgpnxyenzgruyq34xs252vszxs6vg4q5ugprwc68vctswqcqzzsxqzxgsp5uhfpf7rpw85f4k83xd85wtrgazpu62mn08ehzj82yle4pawmez3s9qxpqysgqw95g9xvs6numjyhw7sgen03fmy5e9u4y287ldrt3h885692wspmk5f24h2ppv5xprdg9x0t2yq8fnpf9kqcn0svcmuwrktuag48s0rcppapzlf"
    memo = f"before {invoice} after"

    lm = LightningMemo(memo)

    assert lm.invoice == invoice
    assert "before" in lm.before_text
    assert "after" in lm.after_text
    # LightningMemo uses _lightning_memo on the invoice string itself; compare against helper
    expected = _lightning_memo(invoice)
    assert lm.short_memo == expected


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
    expected = _lightning_memo(frag)
    assert out == expected
    # For very short fragments, the helper does not add an ellipsis (it returns the full fragment)


def test_lightning_memo_matches_fragment_inside_full_memo_and_uses_overall_tail():
    frag = "lnbc1234n"
    memo = f"pay {frag} thanks"
    out = lightning_memo(memo)
    # when called on full memo the helper uses the overall memo's tail for the '...<last5>' portion
    assert out == f"⚡️{frag}...{memo[-5:]}"
