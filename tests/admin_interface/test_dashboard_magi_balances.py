"""Magi exchange rows on the admin dashboard balance card."""

from v4vapp_backend_v2.admin.routers.dashboard_api import magi_exchange_rows


class _MagiProvider:
    def __init__(self, accounts: list[str]):
        self.hive_accounts = accounts


class _Exchange:
    def __init__(self, accounts: list[str]):
        self.magi = _MagiProvider(accounts)


class _Config:
    def __init__(self, accounts: list[str]):
        self.exchange_config = _Exchange(accounts)


class _InternalConfig:
    def __init__(self, accounts: list[str]):
        self.config = _Config(accounts)


class _MagiBalance:
    def __init__(self, sats: int, error: str | None = None):
        self.balance_sats = sats
        self.error = error


async def test_magi_exchange_rows_reports_sats_hive_and_hbd(monkeypatch):
    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.routers.dashboard_api.InternalConfig",
        lambda: _InternalConfig(["v4vapp.exc"]),
    )

    async def fake_hive(account: str):
        assert account == "v4vapp.exc"
        return {"HIVE_fmt": "12.500", "HBD_fmt": "3.250"}

    async def fake_sats(account: str):
        assert account == "v4vapp.exc"
        return _MagiBalance(1_250_000)

    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.routers.dashboard_api.account_hive_balances_async",
        fake_hive,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.routers.dashboard_api.get_magi_btc_balance_by_account",
        fake_sats,
    )

    rows = await magi_exchange_rows()

    assert rows == [
        {
            "account": "v4vapp.exc",
            "HIVE": "12.500",
            "HBD": "3.250",
            "SATS": "1,250,000",
            "error": None,
        }
    ]


async def test_magi_exchange_rows_empty_when_unconfigured(monkeypatch):
    monkeypatch.setattr(
        "v4vapp_backend_v2.admin.routers.dashboard_api.InternalConfig",
        lambda: _InternalConfig([]),
    )

    assert await magi_exchange_rows() == []
