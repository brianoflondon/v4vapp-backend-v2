from decimal import Decimal

import pytest

from v4vapp_backend_v2.conversion.exchange_protocol import ExchangeConnectionError
from v4vapp_backend_v2.conversion.magi_adapter import MagiAdapter


class FakeMagiBalance:
    balance_sats = Decimal("123456789")

    @property
    def balance_msats(self) -> Decimal:
        return self.balance_sats * Decimal("1000")


@pytest.mark.parametrize(
    "asset, expected",
    [
        ("BTC", Decimal("1.23456789")),
        ("SATS", Decimal("123456789")),
        ("MSATS", Decimal("123456789000")),
    ],
)
def test_magi_adapter_get_balance_returns_expected(asset: str, expected: Decimal, monkeypatch):
    """MagiAdapter should convert MAGI BTC balance to the requested asset unit."""

    async def fake_get_balance(account: str):
        assert account == "v4vapp.vsc"
        return FakeMagiBalance()

    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.magi_adapter.get_magi_btc_balance_by_account",
        fake_get_balance,
    )

    adapter = MagiAdapter(server_name="v4vapp.vsc")

    assert adapter.get_balance(asset) == expected


def test_magi_adapter_get_balance_invalid_asset_raises(monkeypatch):
    """MagiAdapter should raise an error for unsupported asset lookups."""

    async def fake_get_balance(account: str):
        return FakeMagiBalance()

    monkeypatch.setattr(
        "v4vapp_backend_v2.conversion.magi_adapter.get_magi_btc_balance_by_account",
        fake_get_balance,
    )

    adapter = MagiAdapter(server_name="v4vapp.vsc")

    with pytest.raises(ExchangeConnectionError, match="only supports BTC/SATS/MSATS"):
        adapter.get_balance("HIVE")


def test_magi_adapter_market_methods_raise_not_implemented():
    """MagiAdapter market operations should be stubbed until implemented."""

    adapter = MagiAdapter(server_name="v4vapp.vsc")

    with pytest.raises(NotImplementedError, match="market_sell is not implemented"):
        adapter.market_sell("HIVE", "BTC", Decimal("1"))

    with pytest.raises(NotImplementedError, match="market_buy is not implemented"):
        adapter.market_buy("HIVE", "BTC", Decimal("1"))


def test_magi_adapter_exchange_name():
    """MagiAdapter should expose the configured exchange name."""

    adapter = MagiAdapter()
    assert adapter.exchange_name == "MagiSwap"
