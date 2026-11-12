"""Dashboard summary of pending_rebalances buy and sell sums."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.admin.routers import dashboard_api
from v4vapp_backend_v2.conversion.exchange_rebalance import PendingRebalance, RebalanceDirection
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.currency_class import Currency

ORDERED_AT = datetime(2026, 11, 11, 10, 30, tzinfo=UTC)
AS_OF = datetime(2026, 11, 11, 12, 44, tzinfo=UTC)

TEMPLATE = Path("src/v4vapp_backend_v2/admin/templates/dashboard.html.jinja")


def _pending(
    *,
    direction: RebalanceDirection,
    qty: str,
    quote: str,
    min_qty: str = "100",
    min_notional: str = "0.001",
    transactions: int = 1,
    transaction_ids: list[str] | None = None,
    base_asset: str = "HIVE",
    quote_asset: str = "BTC",
    exchange: str = "binance",
    updated_at: datetime | None = None,
) -> PendingRebalance:
    return PendingRebalance(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=direction,
        exchange=exchange,
        pending_qty=Decimal(qty),
        pending_quote_value=Decimal(quote),
        min_qty_threshold=Decimal(min_qty),
        min_notional_threshold=Decimal(min_notional),
        transaction_count=transactions,
        transaction_ids=transaction_ids or [],
        updated_at=updated_at or datetime(2026, 11, 11, tzinfo=UTC),
    )


def test_summary_shows_buy_and_sell_progress_toward_the_next_order():
    summary = dashboard_api.summarize_pending_rebalances(
        [
            _pending(
                direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
                qty="25",
                quote="0.00012",
                min_notional="0.0001",
                transactions=2,
                transaction_ids=["buy-a", "buy-b"],
            ),
            _pending(
                direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
                qty="150",
                quote="0.002",
                transactions=4,
                transaction_ids=["sell-a"],
            ),
        ]
    )

    assert summary["error"] is None
    pair = summary["pairs"][0]
    assert pair["symbol"] == "HIVE/BTC"
    assert pair["buy"]["pending_qty_fmt"] == "25.000 HIVE"
    assert pair["buy"]["pending_quote_fmt"] == "12,000 sats"
    assert pair["buy"]["qty_pct"] == 25.0
    assert pair["buy"]["notional_pct"] == 120.0
    # Notional is already over the minimum, so quantity is what still blocks the buy.
    assert pair["buy"]["progress_pct"] == 25.0
    assert pair["buy"]["can_execute"] is False
    assert pair["buy"]["status"] == "Needs 75.000 HIVE"
    assert pair["buy"]["transaction_ids"] == ["buy-a", "buy-b"]

    assert pair["sell"]["can_execute"] is True
    assert pair["sell"]["status"] == "Ready to execute"
    assert pair["sell"]["progress_pct"] == 150.0
    assert pair["sell"]["transaction_count"] == 4

    assert summary["closest"]["label"] == "SELL HIVE/BTC"
    assert summary["closest"]["can_execute"] is True


def test_summary_sums_duplicate_documents_for_the_same_side():
    summary = dashboard_api.summarize_pending_rebalances(
        [
            _pending(
                direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
                qty="10",
                quote="0.00005",
                min_qty="80",
                min_notional="0.00008",
                transactions=2,
                transaction_ids=["buy-a"],
                updated_at=datetime(2026, 11, 1, tzinfo=UTC),
            ),
            _pending(
                direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
                qty="15",
                quote="0.00007",
                min_qty="100",
                min_notional="0.0001",
                transactions=3,
                transaction_ids=["buy-b"],
                updated_at=datetime(2026, 11, 10, tzinfo=UTC),
            ),
        ]
    )

    buy = summary["pairs"][0]["buy"]
    assert buy["pending_qty_fmt"] == "25.000 HIVE"
    assert buy["pending_quote_fmt"] == "12,000 sats"
    assert buy["transaction_count"] == 5
    assert buy["transaction_ids"] == ["buy-a", "buy-b"]
    assert buy["min_qty_fmt"] == "100.000 HIVE"
    assert buy["progress_pct"] == 25.0
    assert summary["pairs"][0]["sell"]["present"] is False
    assert summary["pairs"][0]["sell"]["status"] == "No orders"
    assert summary["closest"]["direction"] == "buy"


def test_order_lines_carry_the_hive_account_and_value():
    summary = dashboard_api.summarize_pending_rebalances(
        [
            _pending(
                direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
                qty="12.345",
                quote="0.0001",
                transactions=1,
                transaction_ids=["4521_ab12cd_0"],
            )
        ],
        order_details={"4521_ab12cd_0": {"account": "alice", "value_fmt": "12.345 HIVE"}},
    )
    assert summary["pairs"][0]["sell"]["orders"] == [
        {
            "short_id": "4521_ab12cd_0",
            "account": "alice",
            "value_fmt": "12.345 HIVE",
            "age": "",
            "rate_fmt": "",
        }
    ]


def _ledger(ledger_type: LedgerType, **kwargs) -> LedgerEntry:
    data = {
        "short_id": "4521_ab12cd_0",
        "cust_id": "alice",
        "ledger_type": ledger_type,
        "debit_amount": Decimal(1),
        "debit_unit": Currency.HIVE,
        "credit_amount": Decimal(1),
        "credit_unit": Currency.HIVE,
    }
    data.update(kwargs)
    return LedgerEntry(**data)


def test_hive_to_keepsats_order_uses_the_hive_amount_and_account():
    entry = _ledger(
        LedgerType.CONV_HIVE_TO_KEEPSATS,
        credit_amount=Decimal("12.345"),
        credit_unit=Currency.HIVE,
        debit_amount=Decimal(12345000),
        debit_unit=Currency.MSATS,
        timestamp=ORDERED_AT,
        debit_conv=CryptoConv(sats_hive=Decimal("312.5")),
    )
    assert dashboard_api.describe_ledger_order(entry, now=AS_OF) == {
        "account": "alice",
        "value_fmt": "12.345 HIVE",
        "age": "2h 14m",
        "rate_fmt": "312.5 sats/HIVE",
    }


def test_keepsats_to_hive_order_uses_the_hive_bought():
    entry = _ledger(
        LedgerType.CONV_KEEPSATS_TO_HIVE,
        cust_id="bob",
        debit_amount=Decimal("8.5"),
        debit_unit=Currency.HBD,
        credit_amount=Decimal(8500000),
        credit_unit=Currency.MSATS,
        timestamp=ORDERED_AT,
        debit_conv=CryptoConv(sats_hive=Decimal("81.565"), sats_hbd=Decimal(280)),
    )
    detail = dashboard_api.describe_ledger_order(entry, now=AS_OF)
    assert detail["account"] == "bob"
    assert detail["value_fmt"] == "8.500 HBD"
    assert detail["age"] == "2h 14m"
    assert detail["rate_fmt"] == "81.565 sats/HIVE"


def test_order_age_uses_hours_and_minutes():
    assert dashboard_api._format_order_age(AS_OF, now=AS_OF) == "<1m"
    assert (
        dashboard_api._format_order_age(datetime(2026, 11, 11, 11, 59, tzinfo=UTC), now=AS_OF)
        == "45m"
    )
    assert (
        dashboard_api._format_order_age(datetime(2026, 11, 11, 9, 44, tzinfo=UTC), now=AS_OF)
        == "3h"
    )


def test_conversion_row_wins_over_the_customer_deposit():
    conversion = _ledger(
        LedgerType.CONV_HIVE_TO_KEEPSATS,
        credit_amount=Decimal(10),
        credit_unit=Currency.HIVE,
    )
    deposit = _ledger(
        LedgerType.CONV_CUSTOMER,
        cust_id="someone-else",
        debit_amount=Decimal(9),
        debit_unit=Currency.HIVE,
        credit_amount=Decimal(9),
        credit_unit=Currency.HIVE,
    )
    chosen = dashboard_api.choose_order_entry([deposit, conversion])
    assert chosen is conversion


def test_hive_op_fallback_names_the_customer_and_amount():
    detail = dashboard_api.describe_hive_op(
        {
            "type": "transfer",
            "from": "alice",
            "to": "someaccount",
            "amount": {"amount": "12345", "nai": "@@000000021", "precision": 3},
            "timestamp": "2026-11-11T12:00:00+00:00",
            "conv": {"sats_hive": "300.25"},
        },
        now=AS_OF,
    )
    assert detail["account"] == "alice"
    assert detail["value_fmt"] == "12.345 HIVE"
    assert detail["age"] == "44m"
    assert detail["rate_fmt"] == "300.25 sats/HIVE"


def test_summary_is_empty_when_nothing_is_pending():
    summary = dashboard_api.summarize_pending_rebalances([])
    assert summary == {"pairs": [], "closest": None, "error": None}


def test_dashboard_template_puts_rebalances_above_the_moved_links():
    html = TEMPLATE.read_text()
    pending = html.index('id="pending-rebalances-section"')
    accounts = html.index("Account Balances")
    reports = html.index("Financial Reports")
    health = html.index("System Health")
    started = html.index("Getting Started")
    assert pending < accounts < started
    assert reports < started
    assert health < started
    assert "/admin/api/dashboard/pending-rebalances" in html


@pytest.mark.asyncio
async def test_pending_rebalances_endpoint_returns_the_summary(monkeypatch):
    record = _pending(
        direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        qty="40",
        quote="0.0004",
        transactions=1,
        transaction_ids=["sell-a"],
    )

    async def _records():
        return [record]

    async def _details(_short_ids):
        return {"sell-a": {"account": "alice", "value_fmt": "40.000 HIVE"}}

    monkeypatch.setattr(dashboard_api, "list_pending_rebalances", _records)
    monkeypatch.setattr(dashboard_api, "lookup_rebalance_orders", _details)
    response = await dashboard_api.dashboard_pending_rebalances()
    body = json.loads(response.body)
    assert body["pairs"][0]["sell"]["pending_qty_fmt"] == "40.000 HIVE"
    assert body["pairs"][0]["sell"]["can_execute"] is False
    assert body["pairs"][0]["sell"]["orders"][0]["account"] == "alice"
    assert body["pairs"][0]["sell"]["orders"][0]["value_fmt"] == "40.000 HIVE"
    assert body["closest"]["label"] == "SELL HIVE/BTC"


@pytest.mark.asyncio
async def test_pending_rebalances_endpoint_reports_a_read_failure(monkeypatch):
    async def _boom():
        raise RuntimeError("mongo down")

    monkeypatch.setattr(dashboard_api, "list_pending_rebalances", _boom)
    response = await dashboard_api.dashboard_pending_rebalances()
    body = json.loads(response.body)
    assert body["pairs"] == []
    assert body["closest"] is None
    assert "mongo down" in body["error"]
