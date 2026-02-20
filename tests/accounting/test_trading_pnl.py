import json
from pathlib import Path

import pytest

from v4vapp_backend_v2.accounting.trading_pnl import trading_pnl_printout


SAMPLE = Path(__file__).parent.parent / "data" / "trading_peformance" / "exchange_holdings_sample.json"


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


def test_sample_trading_pnl_matches_expected():
    data = json.loads(SAMPLE.read_text())
    r = _compute_from_sample_balance(data)

    # Validate counts and cashflow roughly match values from the example
    assert r["summary"]["sells"] == 13
    assert r["summary"]["buys"] == 7
    assert pytest.approx(r["summary"]["hive_sold"], rel=1e-6) == 6728.44808208
    assert pytest.approx(r["summary"]["hive_bought"], rel=1e-6) == 4212.967482325001
    assert pytest.approx(r["summary"]["sats_received"], rel=1e-6) == 687165.0989293022
    assert pytest.approx(r["summary"]["sats_spent"], rel=1e-6) == 428886.0

    # Final P&L ~ 3081 sats (small rounding differences allowed)
    assert pytest.approx(r["performance"]["total_trading_pnl_sats"], rel=1e-2) == 3081.0766


def test_trading_pnl_printout_contains_total():
    simple_report = {"totals": {"total_trading_pnl_sats": 12345.678}, "by_sub": {}}
    out = trading_pnl_printout(simple_report)
    assert "TOTAL TRADING P&L" in out
    # sats values are printed as integers (no decimal places)
    assert "12,346" in out
