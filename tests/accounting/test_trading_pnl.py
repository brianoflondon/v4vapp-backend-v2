import json
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.trading_pnl import (
    generate_trading_pnl_report,
    trading_pnl_printout,
)

SAMPLE = (
    Path(__file__).parent.parent / "data" / "trading_peformance" / "exchange_holdings_sample.json"
)


def _compute_from_sample_balance(balance_json: dict):
    hive_ledger = balance_json["balances"]["hive"]
    trades = [t for t in hive_ledger if t.get("ledger_type") == "exc_conv"]

    sell_count = 0
    buy_count = 0
    total_hive_sold = 0.0
    total_sats_received = 0.0
    total_hive_bought = 0.0
    total_sats_spent = 0.0
    last_price_sats = 0.0

    for trade in trades:
        desc = (trade.get("description") or "").upper()
        # sample uses conv_signed.sats and conv_signed.hive
        conv = trade.get("conv_signed") or trade.get("conv") or {}
        hive_amt = float(conv.get("hive", 0))
        sats_amt = float(conv.get("sats", 0))
        price = float(conv.get("sats_hive", 0) or conv.get("sats_hive", 0) or 0)

        # Use absolute amounts for aggregation (entries can be signed in the balance rows)
        hive_amt = abs(hive_amt)
        sats_amt = abs(sats_amt)

        if "SELL" in desc:
            sell_count += 1
            total_hive_sold += hive_amt
            total_sats_received += sats_amt
            last_price_sats = price
        elif "BUY" in desc:
            buy_count += 1
            total_hive_bought += hive_amt
            total_sats_spent += sats_amt
            last_price_sats = price

    net_hive_change = total_hive_bought - total_hive_sold
    net_sats_cashflow = total_sats_received - total_sats_spent
    inventory_value_sats = net_hive_change * last_price_sats
    total_pnl_sats = net_sats_cashflow + inventory_value_sats

    return {
        "summary": {
            "sells": sell_count,
            "buys": buy_count,
            "hive_sold": total_hive_sold,
            "hive_bought": total_hive_bought,
            "sats_received": total_sats_received,
            "sats_spent": total_sats_spent,
        },
        "performance": {
            "net_hive_inventory_change": net_hive_change,
            "net_sats_cash_generated": net_sats_cashflow,
            "last_price_used": last_price_sats,
            "inventory_valuation_sats": inventory_value_sats,
            "total_trading_pnl_sats": total_pnl_sats,
        },
    }


@pytest.mark.asyncio
async def test_sample_trading_pnl_matches_expected(monkeypatch):
    if not SAMPLE.exists():
        pytest.skip("sample trading data not available")
    data = json.loads(SAMPLE.read_text())

    # compute expected results using the helper (absolute amounts, same logic as
    # we expect in production)
    expected = _compute_from_sample_balance(data)

    # fake one_account_balance to return the sample data structure
    async def fake_one_account_balance(account, as_of_date=None, age=None):
        # convert the raw dict entries into objects with attribute access to mimic
        # AccountBalanceLine.  make nested dicts AttrDict too so conv_signed.hive
        # works.
        class AttrDict(dict):
            def __getattr__(self, item):
                v = self.get(item)
                if isinstance(v, dict):
                    return AttrDict(v)
                return v

        class Dummy:
            balances = {"hive": [AttrDict(t) for t in data["balances"]["hive"]]}

        return Dummy()

    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.trading_pnl.one_account_balance",
        fake_one_account_balance,
    )

    report = await generate_trading_pnl_report(subs=["binance_convert"])
    subrep = report["by_sub"]["binance_convert"]

    # verify counts and amounts match the manual calculation
    assert subrep["summary"]["sells"] == expected["summary"]["sells"]
    assert subrep["summary"]["buys"] == expected["summary"]["buys"]
    assert (
        pytest.approx(subrep["summary"]["hive_sold"], rel=1e-6) == expected["summary"]["hive_sold"]
    )
    assert (
        pytest.approx(subrep["summary"]["hive_bought"], rel=1e-6)
        == expected["summary"]["hive_bought"]
    )
    assert (
        pytest.approx(subrep["summary"]["sats_received"], rel=1e-6)
        == expected["summary"]["sats_received"]
    )
    assert (
        pytest.approx(subrep["summary"]["sats_spent"], rel=1e-6)
        == expected["summary"]["sats_spent"]
    )

    # and verify PnL calculation roughly agrees as well
    assert (
        pytest.approx(subrep["performance"]["total_trading_pnl_sats"], rel=1e-2)
        == expected["performance"]["total_trading_pnl_sats"]
    )

    # also ensure summary values are non-negative
    for k in ("hive_sold", "hive_bought", "sats_received", "sats_spent"):
        assert subrep["summary"][k] >= 0


def test_trading_pnl_printout_contains_total():
    simple_report = {"totals": {"total_trading_pnl_sats": 12345.678}, "by_sub": {}}
    out = trading_pnl_printout(simple_report)
    assert "TOTAL TRADING P&L" in out
    # sats values are printed as integers (no decimal places)
    assert "12,346" in out
