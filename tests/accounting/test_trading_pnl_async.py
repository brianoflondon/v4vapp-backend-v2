import pytest

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
