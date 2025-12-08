"""
Unit tests for exchange_rebalance module.

Tests the pending amount tracking and rebalancing system.
"""

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeMinimums,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    PendingRebalance,
    RebalanceDirection,
    RebalanceResult,
    add_pending_rebalance,
    execute_rebalance_trade,
    force_execute_pending,
    get_pending_rebalances,
)


class TestRebalanceDirection:
    """Tests for RebalanceDirection enum."""

    def test_sell_direction(self):
        """Test SELL direction value."""
        assert RebalanceDirection.SELL_BASE_FOR_QUOTE == "sell"
        assert RebalanceDirection.SELL_BASE_FOR_QUOTE.value == "sell"

    def test_buy_direction(self):
        """Test BUY direction value."""
        assert RebalanceDirection.BUY_BASE_WITH_QUOTE == "buy"
        assert RebalanceDirection.BUY_BASE_WITH_QUOTE.value == "buy"

    def test_direction_is_str_enum(self):
        """Test that direction can be used as string."""
        direction = RebalanceDirection.SELL_BASE_FOR_QUOTE
        assert f"Action: {direction}" == "Action: sell"


class TestPendingRebalance:
    """Tests for PendingRebalance model."""

    def test_create_pending_rebalance(self):
        """Test creating a pending rebalance."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        assert pending.base_asset == "HIVE"
        assert pending.quote_asset == "BTC"
        assert pending.direction == RebalanceDirection.SELL_BASE_FOR_QUOTE
        assert pending.exchange == "binance"
        assert pending.pending_qty == Decimal("0")
        assert pending.pending_quote_value == Decimal("0")
        assert pending.transaction_count == 0
        assert pending.transaction_ids == []

    def test_symbol_property(self):
        """Test symbol property returns correct pair."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )
        assert pending.symbol == "HIVEBTC"

    def test_add_pending_increases_amounts(self):
        """Test add_pending increases accumulated amounts."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        pending.add_pending(
            qty=Decimal("100"),
            quote_value=Decimal("0.00123"),
            transaction_id="trx-001",
        )

        assert pending.pending_qty == Decimal("100")
        assert pending.pending_quote_value == Decimal("0.00123")
        assert pending.transaction_count == 1
        assert pending.transaction_ids == ["trx-001"]

    def test_add_pending_accumulates(self):
        """Test multiple add_pending calls accumulate."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        pending.add_pending(qty=Decimal("50"), quote_value=Decimal("0.0005"))
        pending.add_pending(qty=Decimal("75"), quote_value=Decimal("0.00075"))
        pending.add_pending(qty=Decimal("25"), quote_value=Decimal("0.00025"))

        assert pending.pending_qty == Decimal("150")
        assert pending.pending_quote_value == Decimal("0.0015")
        assert pending.transaction_count == 3

    def test_add_pending_without_transaction_id(self):
        """Test add_pending works without transaction ID."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        pending.add_pending(qty=Decimal("100"), quote_value=Decimal("0.001"))

        assert pending.transaction_count == 1
        assert pending.transaction_ids == []

    def test_can_execute_with_no_pending(self):
        """Test can_execute returns False when no pending amount."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            min_qty_threshold=Decimal("1"),
            min_notional_threshold=Decimal("0.0001"),
        )

        can_execute, reason = pending.can_execute()

        assert can_execute is False
        assert "No pending quantity" in reason

    def test_can_execute_below_qty_threshold(self):
        """Test can_execute returns False when below qty threshold."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            min_qty_threshold=Decimal("100"),
            min_notional_threshold=Decimal("0.0001"),
        )
        pending.add_pending(qty=Decimal("50"), quote_value=Decimal("0.0005"))

        can_execute, reason = pending.can_execute()

        assert can_execute is False
        assert "below minimum" in reason

    def test_can_execute_below_notional_threshold(self):
        """Test can_execute returns False when below notional threshold."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            min_qty_threshold=Decimal("1"),
            min_notional_threshold=Decimal("0.001"),
        )
        pending.add_pending(qty=Decimal("100"), quote_value=Decimal("0.0005"))

        can_execute, reason = pending.can_execute()

        assert can_execute is False
        assert "notional" in reason.lower()

    def test_can_execute_meets_thresholds(self):
        """Test can_execute returns True when thresholds are met."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            min_qty_threshold=Decimal("100"),
            min_notional_threshold=Decimal("0.001"),
        )
        pending.add_pending(qty=Decimal("150"), quote_value=Decimal("0.0015"))

        can_execute, reason = pending.can_execute()

        assert can_execute is True
        assert reason == "OK"

    def test_reset_after_execution_full_fill(self):
        """Test reset_after_execution with full fill."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )
        pending.add_pending(
            qty=Decimal("100"), quote_value=Decimal("0.001"), transaction_id="trx-001"
        )
        pending.add_pending(
            qty=Decimal("50"), quote_value=Decimal("0.0005"), transaction_id="trx-002"
        )

        pending.reset_after_execution(executed_qty=Decimal("150"))

        assert pending.pending_qty == Decimal("0")
        assert pending.pending_quote_value == Decimal("0")
        assert pending.transaction_count == 0
        assert pending.transaction_ids == []
        assert pending.total_executed_qty == Decimal("150")
        assert pending.execution_count == 1
        assert pending.last_executed_at is not None

    def test_reset_after_execution_partial_fill(self):
        """Test reset_after_execution with partial fill keeps remainder."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )
        pending.add_pending(qty=Decimal("200"), quote_value=Decimal("0.002"))

        pending.reset_after_execution(executed_qty=Decimal("150"))

        assert pending.pending_qty == Decimal("50")
        assert pending.total_executed_qty == Decimal("150")
        # Transaction tracking should be cleared even for partial
        assert pending.transaction_count == 0

    def test_reset_after_execution_multiple_times(self):
        """Test multiple execution resets accumulate totals."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        # First execution
        pending.add_pending(qty=Decimal("100"), quote_value=Decimal("0.001"))
        pending.reset_after_execution(executed_qty=Decimal("100"))
        assert pending.total_executed_qty == Decimal("100")
        assert pending.execution_count == 1

        # Second execution
        pending.add_pending(qty=Decimal("200"), quote_value=Decimal("0.002"))
        pending.reset_after_execution(executed_qty=Decimal("200"))
        assert pending.total_executed_qty == Decimal("300")
        assert pending.execution_count == 2


class TestRebalanceResult:
    """Tests for RebalanceResult model."""

    def test_create_not_executed_result(self):
        """Test creating a non-executed result."""
        result = RebalanceResult(
            executed=False,
            reason="Below minimum",
            pending_qty=Decimal("50"),
            pending_notional=Decimal("0.0005"),
        )

        assert result.executed is False
        assert result.reason == "Below minimum"
        assert result.order_result is None
        assert result.error is None

    def test_create_executed_result(self):
        """Test creating an executed result."""
        order = ExchangeOrderResult(
            exchange="binance",
            symbol="HIVEBTC",
            order_id="12345",
            side="SELL",
            status="FILLED",
            requested_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            quote_qty=Decimal("0.00123"),
            avg_price=Decimal("0.0000123"),
            fee=Decimal("0.0000001"),
            fee_asset="BTC",
            raw_response={},
        )

        result = RebalanceResult(
            executed=True,
            reason="Trade executed successfully",
            pending_qty=Decimal("0"),
            pending_notional=Decimal("0"),
            order_result=order,
        )

        assert result.executed is True
        assert result.order_result is not None
        assert result.order_result.symbol == "HIVEBTC"

    def test_create_error_result(self):
        """Test creating an error result."""
        result = RebalanceResult(
            executed=False,
            reason="Exchange connection error",
            error="Connection timeout",
        )

        assert result.executed is False
        assert result.error == "Connection timeout"

    def test_result_has_timestamp(self):
        """Test that RebalanceResult has a timestamp."""
        result = RebalanceResult(
            executed=True,
            reason="Test",
        )
        assert result.timestamp is not None

    @pytest.mark.asyncio
    async def test_save_executed_result(self, mock_rebalance_results_collection):
        """Test saving an executed result to database."""
        order = ExchangeOrderResult(
            exchange="binance",
            symbol="HIVEBTC",
            order_id="12345",
            side="SELL",
            status="FILLED",
            requested_qty=Decimal("100"),
            executed_qty=Decimal("100"),
            quote_qty=Decimal("0.00123"),
            avg_price=Decimal("0.0000123"),
            fee=Decimal("0.0000001"),
            fee_asset="BTC",
            raw_response={},
        )

        result = RebalanceResult(
            executed=True,
            reason="Trade executed successfully",
            pending_qty=Decimal("0"),
            pending_notional=Decimal("0"),
            order_result=order,
        )

        await result.save()

        # Verify insert_one was called
        assert mock_rebalance_results_collection.insert_one.called

    @pytest.mark.asyncio
    async def test_save_not_executed_result_does_not_save(self, mock_rebalance_results_collection):
        """Test that non-executed results are not saved."""
        result = RebalanceResult(
            executed=False,
            reason="Below minimum",
            pending_qty=Decimal("50"),
            pending_notional=Decimal("0.0005"),
        )

        await result.save()

        # Verify insert_one was NOT called
        assert not mock_rebalance_results_collection.insert_one.called


# Mock exchange adapter for testing
class MockExchangeAdapter(BaseExchangeAdapter):
    """Mock exchange adapter for testing."""

    def __init__(self, testnet: bool = False):
        super().__init__(testnet)
        self.sell_calls = []
        self.buy_calls = []
        self.get_min_calls = []
        self.get_price_calls = []
        self._price = Decimal("0.00001")
        self._minimums = ExchangeMinimums(
            min_qty=Decimal("1"),
            min_notional=Decimal("0.0001"),
        )
        self._should_fail = False
        self._fail_with = None

    @property
    def exchange_name(self) -> str:
        return "mock_exchange"

    def market_sell(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        if self._should_fail:
            raise self._fail_with or ExchangeConnectionError("Mock failure")
        self.sell_calls.append((base_asset, quote_asset, quantity))
        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=f"{base_asset}{quote_asset}",
            order_id=str(len(self.sell_calls)),
            side="SELL",
            status="FILLED",
            requested_qty=quantity,
            executed_qty=quantity,
            quote_qty=quantity * self._price,
            avg_price=self._price,
            fee=Decimal("0"),
            fee_asset=quote_asset,
            raw_response={},
        )

    def market_buy(
        self, base_asset: str, quote_asset: str, quantity: Decimal
    ) -> ExchangeOrderResult:
        if self._should_fail:
            raise self._fail_with or ExchangeConnectionError("Mock failure")
        self.buy_calls.append((base_asset, quote_asset, quantity))
        return ExchangeOrderResult(
            exchange=self.exchange_name,
            symbol=f"{base_asset}{quote_asset}",
            order_id=str(len(self.buy_calls)),
            side="BUY",
            status="FILLED",
            requested_qty=quantity,
            executed_qty=quantity,
            quote_qty=quantity * self._price,
            avg_price=self._price,
            fee=Decimal("0"),
            fee_asset=quote_asset,
            raw_response={},
        )

    def get_min_order_requirements(self, base_asset: str, quote_asset: str) -> ExchangeMinimums:
        if self._should_fail:
            raise self._fail_with or ExchangeConnectionError("Mock failure")
        self.get_min_calls.append((base_asset, quote_asset))
        return self._minimums

    def get_current_price(self, base_asset: str, quote_asset: str) -> Decimal:
        if self._should_fail:
            raise self._fail_with or ExchangeConnectionError("Mock failure")
        self.get_price_calls.append((base_asset, quote_asset))
        return self._price

    def get_balance(self, asset: str) -> Decimal:
        return Decimal("1000")

    def set_price(self, price: Decimal):
        self._price = price

    def set_minimums(self, min_qty: Decimal, min_notional: Decimal):
        self._minimums = ExchangeMinimums(min_qty=min_qty, min_notional=min_notional)

    def set_failure(self, should_fail: bool, fail_with: Exception | None = None):
        self._should_fail = should_fail
        self._fail_with = fail_with


@pytest.fixture
def mock_exchange():
    """Create a mock exchange adapter."""
    return MockExchangeAdapter()


@pytest.fixture
def mock_rebalance_results_collection():
    """Mock the MongoDB collection for RebalanceResult."""
    mock_collection = MagicMock()

    async def async_insert_one(*args, **kwargs):
        return MagicMock(inserted_id="mock_id")

    mock_collection.insert_one = MagicMock(side_effect=lambda *a, **kw: async_insert_one(*a, **kw))

    with patch.object(RebalanceResult, "collection", return_value=mock_collection):
        yield mock_collection


@pytest.fixture
def mock_pending_collection(mock_rebalance_results_collection):
    """Mock the MongoDB collection for PendingRebalance."""
    mock_collection = MagicMock()

    # Create async methods that return coroutines when called
    async def async_find_one(*args, **kwargs):
        return None

    async def async_update_one(*args, **kwargs):
        return MagicMock()

    mock_collection.find_one = MagicMock(side_effect=lambda *a, **kw: async_find_one(*a, **kw))
    mock_collection.update_one = MagicMock(side_effect=lambda *a, **kw: async_update_one(*a, **kw))
    mock_collection.find = MagicMock()

    with patch.object(PendingRebalance, "collection", return_value=mock_collection):
        yield mock_collection


class TestExecuteRebalanceTrade:
    """Tests for execute_rebalance_trade function."""

    @pytest.mark.asyncio
    async def test_execute_sell_trade(self, mock_exchange):
        """Test executing a sell trade."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )
        pending.add_pending(qty=Decimal("100"), quote_value=Decimal("0.001"))

        result = await execute_rebalance_trade(
            exchange_adapter=mock_exchange,
            pending=pending,
        )

        assert result.side == "SELL"
        assert result.executed_qty == Decimal("100")
        assert len(mock_exchange.sell_calls) == 1
        assert mock_exchange.sell_calls[0] == ("HIVE", "BTC", Decimal("100"))

    @pytest.mark.asyncio
    async def test_execute_buy_trade(self, mock_exchange):
        """Test executing a buy trade."""
        pending = PendingRebalance(
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
        )
        pending.add_pending(qty=Decimal("200"), quote_value=Decimal("0.002"))

        result = await execute_rebalance_trade(
            exchange_adapter=mock_exchange,
            pending=pending,
        )

        assert result.side == "BUY"
        assert result.executed_qty == Decimal("200")
        assert len(mock_exchange.buy_calls) == 1


class TestAddPendingRebalance:
    """Tests for add_pending_rebalance function."""

    @pytest.mark.asyncio
    async def test_add_pending_below_threshold(self, mock_exchange, mock_pending_collection):
        """Test adding pending amount below threshold."""
        mock_exchange.set_minimums(
            min_qty=Decimal("100"),
            min_notional=Decimal("0.001"),
        )
        mock_exchange.set_price(Decimal("0.00001"))

        result = await add_pending_rebalance(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            qty=Decimal("50"),
            transaction_id="trx-001",
        )

        assert result.executed is False
        assert "below minimum" in result.reason.lower() or "pending" in result.reason.lower()
        # Should NOT have executed a trade
        assert len(mock_exchange.sell_calls) == 0

    @pytest.mark.asyncio
    async def test_add_pending_exceeds_threshold(self, mock_exchange, mock_pending_collection):
        """Test adding pending amount that exceeds threshold triggers trade."""
        mock_exchange.set_minimums(
            min_qty=Decimal("1"),
            min_notional=Decimal("0.00001"),
        )
        mock_exchange.set_price(Decimal("0.00001"))

        result = await add_pending_rebalance(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            qty=Decimal("100"),
            transaction_id="trx-001",
        )

        assert result.executed is True
        assert result.order_result is not None
        assert len(mock_exchange.sell_calls) == 1

    @pytest.mark.asyncio
    async def test_add_pending_with_exchange_error(self, mock_exchange, mock_pending_collection):
        """Test handling exchange error during trade execution."""
        mock_exchange.set_minimums(
            min_qty=Decimal("100"),
            min_notional=Decimal("0.001"),
        )
        mock_exchange.set_price(Decimal("0.00001"))

        # Set up to fail on sell but not on get_minimums/get_price
        def failing_sell(*args, **kwargs):
            raise ExchangeConnectionError("Connection lost")

        mock_exchange.market_sell = failing_sell

        result = await add_pending_rebalance(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            qty=Decimal("100"),
        )

        assert result.executed is False
        assert result.error is not None
        assert "connection" in result.error.lower()

    @pytest.mark.asyncio
    async def test_add_pending_with_below_minimum_error(
        self, mock_exchange, mock_pending_collection
    ):
        """Test handling below minimum error from exchange."""
        mock_exchange.set_minimums(
            min_qty=Decimal("1"),
            min_notional=Decimal("0.00001"),
        )
        mock_exchange.set_price(Decimal("0.00001"))

        # Set up to fail with below minimum
        def failing_sell(*args, **kwargs):
            raise ExchangeBelowMinimumError("Below minimum order")

        mock_exchange.market_sell = failing_sell

        result = await add_pending_rebalance(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            qty=Decimal("100"),
        )

        assert result.executed is False
        assert result.error is not None


class TestForceExecutePending:
    """Tests for force_execute_pending function."""

    @pytest.mark.asyncio
    async def test_force_execute_with_pending(self, mock_exchange, mock_pending_collection):
        """Test force executing pending amount."""
        # Pre-populate the mock to return a pending record
        pending_data = {
            "base_asset": "HIVE",
            "quote_asset": "BTC",
            "direction": "sell",
            "exchange": "mock_exchange",
            "pending_qty": "75",
            "pending_quote_value": "0.00075",
            "min_qty_threshold": "100",
            "min_notional_threshold": "0.001",
            "total_executed_qty": "0",
            "transaction_count": 3,
            "transaction_ids": ["trx-1", "trx-2", "trx-3"],
            "execution_count": 0,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            "last_executed_at": None,
        }

        async def async_find_one_with_data(*args, **kwargs):
            return pending_data

        mock_pending_collection.find_one = MagicMock(
            side_effect=lambda *a, **kw: async_find_one_with_data(*a, **kw)
        )

        result = await force_execute_pending(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        assert result.executed is True
        assert len(mock_exchange.sell_calls) == 1

    @pytest.mark.asyncio
    async def test_force_execute_with_no_pending(self, mock_exchange, mock_pending_collection):
        """Test force execute when no pending amount."""
        result = await force_execute_pending(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        assert result.executed is False
        assert "no pending" in result.reason.lower()

    @pytest.mark.asyncio
    async def test_force_execute_with_failure(self, mock_exchange, mock_pending_collection):
        """Test force execute when trade fails."""
        pending_data = {
            "base_asset": "HIVE",
            "quote_asset": "BTC",
            "direction": "sell",
            "exchange": "mock_exchange",
            "pending_qty": "50",
            "pending_quote_value": "0.0005",
            "min_qty_threshold": "100",
            "min_notional_threshold": "0.001",
            "total_executed_qty": "0",
            "transaction_count": 1,
            "transaction_ids": [],
            "execution_count": 0,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            "last_executed_at": None,
        }

        async def async_find_one_with_data(*args, **kwargs):
            return pending_data

        mock_pending_collection.find_one = MagicMock(
            side_effect=lambda *a, **kw: async_find_one_with_data(*a, **kw)
        )

        def failing_sell(*args, **kwargs):
            raise ExchangeBelowMinimumError("Order too small")

        mock_exchange.market_sell = failing_sell

        result = await force_execute_pending(
            exchange_adapter=mock_exchange,
            base_asset="HIVE",
            quote_asset="BTC",
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        )

        assert result.executed is False
        assert result.error is not None


class TestGetPendingRebalances:
    """Tests for get_pending_rebalances function."""

    @pytest.mark.asyncio
    async def test_get_pending_rebalances_empty(self, mock_pending_collection):
        """Test getting pending rebalances when none exist."""

        # Create async iterator mock
        async def empty_iterator():
            return
            yield  # Make it a generator

        mock_cursor = MagicMock()
        mock_cursor.__aiter__ = lambda self: empty_iterator()
        mock_pending_collection.find = MagicMock(return_value=mock_cursor)

        result = await get_pending_rebalances()

        assert result == []

    @pytest.mark.asyncio
    async def test_get_pending_rebalances_with_records(self, mock_pending_collection):
        """Test getting pending rebalances with existing records."""
        records = [
            {
                "base_asset": "HIVE",
                "quote_asset": "BTC",
                "direction": "sell",
                "exchange": "binance",
                "pending_qty": "150",
                "pending_quote_value": "0.0015",
                "min_qty_threshold": "100",
                "min_notional_threshold": "0.001",
                "total_executed_qty": "0",
                "transaction_count": 5,
                "transaction_ids": [],
                "execution_count": 0,
                "created_at": datetime.now(tz=timezone.utc).isoformat(),
                "updated_at": datetime.now(tz=timezone.utc).isoformat(),
                "last_executed_at": None,
            },
        ]

        async def record_iterator():
            for record in records:
                yield record

        mock_cursor = MagicMock()
        mock_cursor.__aiter__ = lambda self: record_iterator()
        mock_pending_collection.find = MagicMock(return_value=mock_cursor)

        result = await get_pending_rebalances()

        assert len(result) == 1
        assert result[0].base_asset == "HIVE"
        assert result[0].pending_qty == Decimal("150")
