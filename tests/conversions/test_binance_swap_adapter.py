from decimal import Decimal

from v4vapp_backend_v2.conversion.binance_swap_adapter import BinanceSwapAdapter, ExchangeMinimums


class DummyClient:
    def __init__(self, pairs):
        self._pairs = pairs

    def list_all_convert_pairs(self, fromAsset, toAsset):
        # return same list regardless of parameters for simplicity
        return self._pairs


def test_min_qty_override(monkeypatch):
    """Verify that very small min_qty values are bumped up by the override."""
    adapter = BinanceSwapAdapter(testnet=False)

    # prepare client that reports an impractically low minimum
    tiny = Decimal("0.00000001")
    dummy_pairs = [
        {
            "fromAsset": "HIVE",
            "toAsset": "BTC",
            "fromAssetMinAmount": str(tiny),
            "toAssetMinAmount": "0",
        }
    ]

    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClient(dummy_pairs))

    mins = adapter.get_min_order_requirements("HIVE", "BTC")

    # override defined in adapter should raise min_qty to at least 50
    assert isinstance(mins, ExchangeMinimums)
    assert mins.min_qty >= Decimal("50"), "min_qty should be overridden to 50 HIVE"


def test_min_qty_no_override_for_other_asset(monkeypatch):
    """When base asset isn't configured for override, reported minimum should pass through."""
    adapter = BinanceSwapAdapter(testnet=False)

    reported = Decimal("0.001")
    dummy_pairs = [
        {
            "fromAsset": "ABC",
            "toAsset": "BTC",
            "fromAssetMinAmount": str(reported),
            "toAssetMinAmount": "0",
        }
    ]
    monkeypatch.setattr(adapter, "_get_client", lambda: DummyClient(dummy_pairs))

    mins = adapter.get_min_order_requirements("ABC", "BTC")
    assert mins.min_qty == reported
