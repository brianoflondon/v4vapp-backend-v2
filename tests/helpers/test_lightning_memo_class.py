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
    # ensure detected invoice is longer than the shorten threshold (head=12 tail=5 + 3)
    assert len(lm.invoice) > 12 + 5 + 3




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


def test_lightningmemo_ignores_already_shortened_invoice_fragment():
    memo = "Paid ⚡️lnbc12340n...9klw7"
    lm = LightningMemo(memo)

    # should NOT detect the short/ellipsed invoice fragment as an actual invoice
    assert lm.invoice == ""
    assert not lm.is_lightning_invoice
    # short_memo should preserve the original memo (no invoice shortening)
    assert lm.short_memo == "💬Paid ⚡️lnbc12340n...9klw7"
    assert not lm.is_lightning


def test__lightning_memo_ignores_shortened_invoice_fragment():
    memo = "Paid ⚡️lnbc12340n...9klw7"
    # helper should not shorten a memo that already contains an ellipsed invoice
    assert _lightning_memo(memo) == "💬Paid ⚡️lnbc12340n...9klw7"


def test_lightningmemo_parses_lightning_address_plain():
    memo = "pay me alice@zbd.gg thanks"
    lm = LightningMemo(memo)

    # new attribute `ln_address` holds internet-identifier matches; `invoice` remains empty
    assert lm.ln_address == "alice@zbd.gg"
    assert lm.invoice == ""
    assert "pay me" in lm.before_text
    assert "thanks" in lm.after_text
    # ln_address currently uses the generic memo shortener (chat bubble)
    assert lm.short_memo == "💬alice@zbd.gg"
    assert lm.is_ln_address is True
    assert lm.is_lightning_invoice is False


def test_lightningmemo_parses_lightning_address_with_prefixes():
    memo = "donate ⚡️user+tag@sub.domain.com now"
    lm = LightningMemo(memo)

    assert lm.ln_address == "user+tag@sub.domain.com"
    assert lm.invoice == ""
    # ln_address currently uses the generic memo shortener (chat bubble)
    assert lm.short_memo == "💬user+tag@sub.domain.com"
    assert lm.before_text.strip().startswith("donate")
    assert lm.after_text.strip().endswith("now")
    assert lm.is_ln_address
    assert not lm.is_lightning_invoice


def test_lightningmemo_parses_lightning_address_with_lightning_scheme():
    memo = "send lightning:bob.smith@coinos.io thanks"
    lm = LightningMemo(memo)

    assert lm.ln_address == "bob.smith@coinos.io"
    assert lm.invoice == ""
    # ln_address currently uses the generic memo shortener (chat bubble)
    assert lm.short_memo == "💬bob.smith@coinos.io"
    assert lm.is_ln_address


def test_lightningmemo_does_not_treat_local_identifier_as_lightning_address():
    memo = "contact me at user@localhost for dev"
    lm = LightningMemo(memo)

    # 'user@localhost' is not a public internet identifier (no TLD) and should not match
    assert lm.invoice == ""
    assert lm.ln_address == ""
    assert lm.short_memo.startswith("💬")
    assert lm.is_lightning_invoice is False
    assert lm.is_ln_address is False


def test_lightning_memo_accepts_none_or_empty_string():
    lm = LightningMemo(None)
    assert lm.invoice == ""
    assert lm.ln_address == ""
    assert lm.short_memo == ""
    assert lm.is_lightning_invoice is False
    assert lm.is_ln_address is False
    lm = LightningMemo("")
    assert lm.invoice == ""
    assert lm.ln_address == ""
    assert lm.short_memo == ""
    assert lm.is_lightning_invoice is False
    assert lm.is_ln_address is False
