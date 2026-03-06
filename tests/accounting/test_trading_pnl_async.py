import pytest

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.trading_pnl import generate_trading_pnl_report


@pytest.mark.asyncio
async def test_generate_trading_pnl_accepts_sub(monkeypatch):
    # Fake one_account_balance to avoid DB calls; return object with minimal shape
    async def fake_one_account_balance(account, as_of_date=None, age=None):
        class Dummy:
            balances = {"hive": []}

        return Dummy()

    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.trading_pnl.one_account_balance",
        fake_one_account_balance,
    )

    res = await generate_trading_pnl_report(subs=["binance_convert"])
    assert "by_sub" in res
    assert "binance_convert" in res["by_sub"]
    assert "totals" in res


@pytest.mark.asyncio
async def test_negative_signed_entries_are_normalized(monkeypatch):
    # create a fake ledger entry with a negative hive amount but no BUY/SELL
    class FakeLine:
        ledger_type = LedgerType.EXCHANGE_CONVERSION.value
        description = ""
        # conv_signed with negative hive indicates a sell in production data
        conv_signed = type("c", (), {"hive": -100.0, "sats": 5000.0, "sats_hive": 50.0})

    async def fake_one_account_balance(account, as_of_date=None, age=None):
        class Dummy:
            balances = {"hive": [FakeLine()]}

        return Dummy()

    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.trading_pnl.one_account_balance",
        fake_one_account_balance,
    )

    report = await generate_trading_pnl_report(subs=["binance_convert"])
    subrep = report["by_sub"]["binance_convert"]

    # we should have counted one sell (not a buy) and totals should be positive
    assert subrep["summary"]["sells"] == 1
    assert subrep["summary"]["buys"] == 0
    assert subrep["summary"]["hive_sold"] == 100.0
    assert subrep["summary"]["sats_received"] == 5000.0
    # net hive change = buys - sells = -100
    assert subrep["performance"]["net_hive_inventory_change"] == -100.0
