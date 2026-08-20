"""Owner-loan Lightning memos must use v4v-funding / v4v-balance-adjustment only."""

import re

import pytest

from v4vapp_backend_v2.models.custom_records import DecodedCustomRecord
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment, PaymentStatus
from v4vapp_backend_v2.process.process_tracked_events import (
    FUNDING_MEMO_REGEX,
    FUNDING_MEMO_TAGS,
    check_funding_balance_adjustment,
)


def _payment(
    description: str | None,
    status: PaymentStatus = PaymentStatus.SUCCEEDED,
    cust_id: str | None = None,
) -> Payment:
    return Payment(status=status, invoice_description=description, cust_id=cust_id)


def _invoice(
    memo: str = "",
    *,
    is_lndtohive: bool = False,
    is_magisats: bool = False,
    keysend_message: str | None = None,
) -> Invoice:
    custom_records = None
    if keysend_message is not None:
        custom_records = DecodedCustomRecord(keysend_message=keysend_message)
    return Invoice(
        memo=memo,
        is_lndtohive=is_lndtohive,
        is_magisats=is_magisats,
        custom_records=custom_records,
    )


@pytest.mark.parametrize("tag", FUNDING_MEMO_TAGS)
def test_operator_tags_match_on_succeeded_payment(tag: str) -> None:
    assert check_funding_balance_adjustment(_payment(tag)) is True
    assert check_funding_balance_adjustment(_payment(tag.upper())) is True
    assert check_funding_balance_adjustment(_payment(f"node top-up {tag} 2026-08-20")) is True


@pytest.mark.parametrize(
    "description",
    [
        "Campaign funding: cmt192vmg030nno0ke9nytja2",
        "funding",
        "Funding",
        "balance adjustment",
        "Balance Adjustment",
        "Sending sats from v4v.app § 3968_108218_1",
        "",
        None,
    ],
)
def test_generic_funding_text_does_not_match_payment(description: str | None) -> None:
    assert check_funding_balance_adjustment(_payment(description)) is False


def test_in_flight_payment_with_operator_tag_does_not_match() -> None:
    assert (
        check_funding_balance_adjustment(_payment("v4v-funding", status=PaymentStatus.IN_FLIGHT))
        is False
    )


def test_payment_with_cust_id_is_never_owner_loan() -> None:
    """Customer Keepsats pays must not book as FUNDING, even with an operator tag."""
    assert check_funding_balance_adjustment(_payment("v4v-funding", cust_id="oadissin")) is False
    assert (
        check_funding_balance_adjustment(
            _payment("Campaign funding: cmt192vmg030nno0ke9nytja2", cust_id="oadissin")
        )
        is False
    )


@pytest.mark.parametrize("cust_id", [None, "", "   "])
def test_operator_tag_matches_when_cust_id_is_unset(cust_id: str | None) -> None:
    assert check_funding_balance_adjustment(_payment("v4v-funding", cust_id=cust_id)) is True


@pytest.mark.parametrize("tag", FUNDING_MEMO_TAGS)
def test_operator_tags_match_on_invoice_memo(tag: str) -> None:
    assert check_funding_balance_adjustment(_invoice(tag)) is True
    assert check_funding_balance_adjustment(_invoice(f"{tag} extra note")) is True


def test_invoice_keysend_message_used_when_memo_empty() -> None:
    assert (
        check_funding_balance_adjustment(_invoice("", keysend_message="v4v-balance-adjustment"))
        is True
    )
    assert check_funding_balance_adjustment(_invoice("", keysend_message="funding")) is False


def test_lndtohive_and_magisats_invoices_never_match() -> None:
    assert check_funding_balance_adjustment(_invoice("v4v-funding", is_lndtohive=True)) is False
    assert check_funding_balance_adjustment(_invoice("v4v-funding", is_magisats=True)) is False


def test_campaign_funding_invoice_does_not_match() -> None:
    assert (
        check_funding_balance_adjustment(_invoice("Campaign funding: cmt192vmg030nno0ke9nytja2"))
        is False
    )


def test_funding_memo_regex_matches_only_operator_tags() -> None:
    pattern = re.compile(FUNDING_MEMO_REGEX, re.IGNORECASE)
    assert pattern.search("v4v-funding")
    assert pattern.search("V4V-BALANCE-ADJUSTMENT extra")
    assert not pattern.search("Campaign funding: cmt192vmg030nno0ke9nytja2")
    assert not pattern.search("funding")
    assert not pattern.search("balance adjustment")
