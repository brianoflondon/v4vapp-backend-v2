"""
Live Binance Testnet Trading Tests

These tests execute REAL trades on the Binance testnet.
They are skipped by default and intended for interactive use only.

To run a specific test interactively:
    pytest tests/conversions/test_live_testnet_binance_adapter.py::test_market_buy_hive -v -s
    pytest tests/conversions/test_live_testnet_binance_adapter.py::test_market_sell_hive -v -s

Or set the environment variable to run all:
    BINANCE_LIVE_TEST=1 pytest tests/conversions/test_live_testnet_binance_adapter.py -v -s
"""

import os
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse

# Skip all tests unless explicitly enabled
LIVE_TEST_ENABLED = os.getenv("BINANCE_LIVE_TEST", "").lower() in ("1", "true", "yes")


def _create_mock_quote() -> QuoteResponse:
    """Create a realistic mock quote for testing when CoinMarketCap is unavailable."""
    return QuoteResponse(
        hive_usd=Decimal("0.15"),
        hbd_usd=Decimal("0.98"),
        btc_usd=Decimal("105000"),
        hive_hbd=Decimal("0.153"),
        source="mock_for_testing",
        fetch_date=datetime.now(tz=timezone.utc),
    )


@pytest.fixture(scope="module")
def mock_all_quotes():
    """
    Mock the AllQuotes.get_all_quotes to avoid CoinMarketCap API calls.

    This provides realistic market data without requiring a valid API key.
    """
    mock_quote = _create_mock_quote()

    async def mock_get_all_quotes(self, *args, **kwargs):
        self.quote = mock_quote
        return mock_quote

    with patch(
        "v4vapp_backend_v2.helpers.crypto_prices.AllQuotes.get_all_quotes",
        mock_get_all_quotes,
    ):
        yield mock_quote


@pytest.fixture(scope="module")
def binance_testnet_adapter(mock_all_quotes):
    """
    Create a Binance adapter configured from devhive.config.yaml.

    This loads the testnet configuration and creates an adapter ready
    for live testnet trading.
    """
    # Load the devhive config which has testnet settings
    InternalConfig(config_filename="config/devhive.config.yaml")

    # Get the adapter from the factory (will use testnet based on config)
    adapter = get_exchange_adapter("binance")

    # Verify we're using testnet
    assert adapter.testnet, "Expected testnet mode - check devhive.config.yaml"

    return adapter


@pytest.mark.skip(
    reason="Interactive test - remove skip or run with: pytest -k test_market_buy_hive -v -s"
)
def test_market_buy_hive(binance_testnet_adapter):
    """
    Execute a REAL market BUY order for HIVE on Binance testnet.

    This buys HIVE using BTC.

    Run interactively:
        pytest tests/conversions/test_live_testnet_binance_adapter.py::test_market_buy_hive -v -s
    """
    adapter = binance_testnet_adapter

    # Amount of HIVE to buy (adjust as needed)
    hive_amount = Decimal("5000")

    print(f"\n{'=' * 60}")
    print("EXECUTING LIVE TESTNET MARKET BUY")
    print(f"{'=' * 60}")
    print(f"Exchange: {adapter.exchange_name}")
    print(f"Testnet: {adapter.testnet}")
    print(f"Action: BUY {hive_amount} HIVE with BTC")
    print(f"{'=' * 60}\n")

    # Check current price first
    current_price = adapter.get_current_price("HIVE", "BTC")
    print(f"Current HIVE/BTC price: {current_price}")

    # Check BTC balance
    btc_balance = adapter.get_balance("BTC")
    print(f"BTC balance: {btc_balance}")

    # Execute the buy
    result = adapter.market_buy("HIVE", "BTC", hive_amount)

    print(f"\n{'=' * 60}")
    print("ORDER RESULT")
    print(f"{'=' * 60}")
    print(f"Order ID: {result.order_id}")
    print(f"Status: {result.status}")
    print(f"Side: {result.side}")
    print(f"Executed Qty: {result.executed_qty} HIVE")
    print(f"Quote Qty (BTC spent): {result.quote_qty}")
    print(f"Average Price: {result.avg_price}")
    print(f"Fee: {result.fee_original} {result.fee_asset}")
    print(f"Fee (msats): {result.fee_msats}")
    print(f"{'=' * 60}\n")

    if result.trade_quote:
        print("Trade Quote:")
        print(f"  hive_usd: {result.trade_quote.hive_usd}")
        print(f"  btc_usd: {result.trade_quote.btc_usd}")
        print(f"  sats_hive: {result.trade_quote.sats_hive}")
        print(f"{'=' * 60}\n")

    # Basic assertions
    assert result.status == "FILLED", f"Expected FILLED, got {result.status}"
    assert result.executed_qty > 0, "Expected some HIVE to be bought"
    assert result.side == "BUY"


@pytest.mark.skip(
    reason="Interactive test - remove skip or run with: pytest -k test_market_sell_hive -v -s"
)
def test_market_sell_hive(binance_testnet_adapter):
    """
    Execute a REAL market SELL order for HIVE on Binance testnet.

    This sells HIVE for BTC.

    Run interactively:
        pytest tests/conversions/test_live_testnet_binance_adapter.py::test_market_sell_hive -v -s
    """
    adapter = binance_testnet_adapter

    # Amount of HIVE to sell (adjust as needed)
    hive_amount = Decimal("100")

    print(f"\n{'=' * 60}")
    print("EXECUTING LIVE TESTNET MARKET SELL")
    print(f"{'=' * 60}")
    print(f"Exchange: {adapter.exchange_name}")
    print(f"Testnet: {adapter.testnet}")
    print(f"Action: SELL {hive_amount} HIVE for BTC")
    print(f"{'=' * 60}\n")

    # Check current price first
    current_price = adapter.get_current_price("HIVE", "BTC")
    print(f"Current HIVE/BTC price: {current_price}")

    # Check HIVE balance
    hive_balance = adapter.get_balance("HIVE")
    print(f"HIVE balance: {hive_balance}")

    if hive_balance < hive_amount:
        pytest.skip(f"Insufficient HIVE balance: {hive_balance} < {hive_amount}")

    # Execute the sell
    result = adapter.market_sell("HIVE", "BTC", hive_amount)

    print(f"\n{'=' * 60}")
    print("ORDER RESULT")
    print(f"{'=' * 60}")
    print(f"Order ID: {result.order_id}")
    print(f"Status: {result.status}")
    print(f"Side: {result.side}")
    print(f"Executed Qty: {result.executed_qty} HIVE")
    print(f"Quote Qty (BTC received): {result.quote_qty}")
    print(f"Average Price: {result.avg_price}")
    print(f"Fee: {result.fee_original} {result.fee_asset}")
    print(f"Fee (msats): {result.fee_msats}")
    print(f"{'=' * 60}\n")

    if result.trade_quote:
        print("Trade Quote:")
        print(f"  hive_usd: {result.trade_quote.hive_usd}")
        print(f"  btc_usd: {result.trade_quote.btc_usd}")
        print(f"  sats_hive: {result.trade_quote.sats_hive}")
        print(f"{'=' * 60}\n")

    # Basic assertions
    assert result.status == "FILLED", f"Expected FILLED, got {result.status}"
    assert result.executed_qty > 0, "Expected some HIVE to be sold"
    assert result.side == "SELL"


@pytest.mark.skip(
    reason="Interactive test - remove skip or run with: pytest -k test_check_balances -v -s"
)
def test_check_balances(binance_testnet_adapter):
    """
    Check current balances on Binance testnet without trading.

    Run interactively:
        pytest tests/conversions/test_live_testnet_binance_adapter.py::test_check_balances -v -s
    """
    adapter = binance_testnet_adapter

    print(f"\n{'=' * 60}")
    print("BINANCE TESTNET BALANCES")
    print(f"{'=' * 60}")
    print(f"Exchange: {adapter.exchange_name}")
    print(f"Testnet: {adapter.testnet}")
    print(f"{'=' * 60}\n")

    # Check various balances
    for asset in ["BTC", "HIVE", "BNB", "USDT"]:
        try:
            balance = adapter.get_balance(asset)
            print(f"{asset}: {balance}")
        except Exception as e:
            print(f"{asset}: Error - {e}")

    print(f"\n{'=' * 60}")
    print("CURRENT PRICES")
    print(f"{'=' * 60}")

    current_price = adapter.get_current_price("HIVE", "BTC")
    print(f"HIVE/BTC: {current_price}")

    minimums = adapter.get_min_order_requirements("HIVE", "BTC")
    print("\nMin order requirements for HIVE/BTC:")
    print(f"  Min Qty: {minimums.min_qty}")
    print(f"  Min Notional: {minimums.min_notional}")
    print(f"  Step Size: {minimums.step_size}")
    print(f"{'=' * 60}\n")
