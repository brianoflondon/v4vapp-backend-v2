import pytest

from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults
from v4vapp_backend_v2.admin.data_helpers import admin_data_helper


@pytest.mark.asyncio
async def test_admin_data_helper_includes_pnl(monkeypatch):
    # patch profit/loss and trading pnl generators to return predictable values
    async def fake_pl():
        return {"Net Income": {"Total": {"usd": 123.45}}}

    async def fake_tp():
        return {"totals": {"total_trading_pnl_usd": 67.89}}

    # patch both the source modules and the local names imported into data_helpers
    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.profit_and_loss.generate_profit_and_loss_report",
        fake_pl,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.data_helpers.generate_profit_and_loss_report",
        fake_pl,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.trading_pnl.generate_trading_pnl_report",
        fake_tp,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.data_helpers.generate_trading_pnl_report",
        fake_tp,
    )

    # stub out other dependencies so the helper completes quickly
    # async stub for sanity checks
    async def fake_sanity(*args, **kwargs):
        return SanityCheckResults()

    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.data_helpers.log_all_sanity_checks",
        fake_sanity,
    )

    async def fake_pending():
        return []

    monkeypatch.setattr(
        "v4vapp_backend_v2.hive_models.pending_transaction_class.PendingTransaction.list_all_str",
        fake_pending,
    )

    async def fake_fetch(self=None):
        return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.data_helpers.NodeBalances.fetch_balances",
        fake_fetch,
    )

    async def fake_one_balance(*args, **kwargs):
        return None

    monkeypatch.setattr(
        "v4vapp_backend_v2.accounting.account_balances.one_account_balance",
        fake_one_balance,
    )

    async def fake_account_hive_balances_async(acc):
        return {}

    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.data_helpers.account_hive_balances_async",
        fake_account_hive_balances_async,
    )

    helper = await admin_data_helper()
    # ensure the computed summary values match the fake reports
    assert helper.profit_loss_usd == 123.45
    assert helper.trading_pnl_usd == 67.89
