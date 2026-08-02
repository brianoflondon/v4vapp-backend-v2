"""Minimal same-second tie-break for account balance history lines."""

from datetime import UTC, datetime

from v4vapp_backend_v2.accounting.accounting_classes import (
    AccountBalanceLine,
    balance_line_sort_key,
)


def _line(
    ledger_type: str,
    *,
    ts: datetime | None = None,
    unit: str = "msats",
    short_id: str = "",
    side: str = "debit",
) -> AccountBalanceLine:
    return AccountBalanceLine(
        timestamp=ts or datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC),
        ledger_type=ledger_type,
        unit=unit,
        short_id=short_id,
        side=side,
        description=ledger_type,
    )


def test_same_second_recv_before_fee():
    """Within one second, recv_l must sort before c_j_fee (running totals)."""
    ts = datetime(2026, 8, 1, 19, 27, 21, tzinfo=UTC)
    fee = _line("c_j_fee", ts=ts, short_id="a_fee")
    recv = _line("recv_l", ts=ts, short_id="b_recv")
    ordered = sorted([fee, recv], key=balance_line_sort_key)
    assert [x.ledger_type for x in ordered] == ["recv_l", "c_j_fee"]


def test_timestamp_still_primary():
    early = _line(
        "c_j_fee",
        ts=datetime(2026, 8, 1, 10, 0, 0, tzinfo=UTC),
        short_id="early",
    )
    late = _line(
        "cust_h_in",
        ts=datetime(2026, 8, 1, 11, 0, 0, tzinfo=UTC),
        short_id="late",
    )
    ordered = sorted([late, early], key=balance_line_sort_key)
    assert ordered[0].short_id == "early"
    assert ordered[1].short_id == "late"


def test_short_id_breaks_remaining_ties():
    ts = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
    a = _line("recv_l", ts=ts, short_id="aaa")
    b = _line("recv_l", ts=ts, short_id="bbb")
    ordered = sorted([b, a], key=balance_line_sort_key)
    assert [x.short_id for x in ordered] == ["aaa", "bbb"]
