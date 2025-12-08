"""
Exchange rebalancing system with pending amount tracking.

This module provides functionality to:
1. Track pending/unconverted amounts in both directions (e.g., HIVE→BTC and BTC→HIVE)
2. Accumulate small transactions that are below exchange minimums
3. Execute trades only when cumulative amounts exceed thresholds
4. Support multiple exchanges through the ExchangeProtocol abstraction

The rebalancing is decoupled from the main conversion flow - it runs as a
background task that doesn't affect customer transactions.
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import ClassVar

from bson.decimal128 import Decimal128
from pydantic import BaseModel, Field, field_validator
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeOrderResult,
)
from v4vapp_backend_v2.database.db_retry import mongo_call
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb


class RebalanceDirection(StrEnum):
    """Direction of the rebalance trade."""

    SELL_BASE_FOR_QUOTE = "sell"  # e.g., Sell HIVE for BTC
    BUY_BASE_WITH_QUOTE = "buy"  # e.g., Buy HIVE with BTC


class PendingRebalance(BaseModel):
    """
    Tracks pending/unconverted amounts for a specific trading pair.

    This model accumulates small transactions that are below exchange minimums
    and triggers a trade when the cumulative amount exceeds the threshold.
    """

    model_config = {"arbitrary_types_allowed": True}
    db_client: ClassVar[AsyncCollection | None] = None

    # Identity fields
    base_asset: str = Field(..., description="Base asset (e.g., 'HIVE')")
    quote_asset: str = Field(..., description="Quote asset (e.g., 'BTC')")
    direction: RebalanceDirection = Field(..., description="Trade direction")
    exchange: str = Field(default="binance", description="Exchange to use")

    # Accumulated amounts
    pending_qty: Decimal = Field(
        default=Decimal("0"),
        description="Accumulated quantity of base asset pending conversion",
    )
    pending_quote_value: Decimal = Field(
        default=Decimal("0"),
        description="Estimated value in quote asset (for tracking)",
    )

    # Threshold tracking
    min_qty_threshold: Decimal = Field(
        default=Decimal("0"),
        description="Minimum quantity required by exchange",
    )
    min_notional_threshold: Decimal = Field(
        default=Decimal("0"),
        description="Minimum notional value required by exchange",
    )

    # Transaction tracking
    transaction_count: int = Field(
        default=0,
        description="Number of transactions accumulated",
    )
    transaction_ids: list[str] = Field(
        default_factory=list,
        description="IDs of accumulated transactions for audit trail",
    )

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))
    last_executed_at: datetime | None = Field(
        default=None,
        description="Timestamp of last successful trade execution",
    )

    # Execution history
    total_executed_qty: Decimal = Field(
        default=Decimal("0"),
        description="Total quantity executed across all trades",
    )
    execution_count: int = Field(
        default=0,
        description="Number of successful trade executions",
    )

    @field_validator(
        "pending_qty",
        "pending_quote_value",
        "min_qty_threshold",
        "min_notional_threshold",
        "total_executed_qty",
        mode="before",
    )
    @classmethod
    def convert_to_decimal(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        if isinstance(v, Decimal128):
            return Decimal(str(v))
        return v

    @classmethod
    def collection(cls) -> AsyncCollection:
        """Get the MongoDB collection for pending rebalances."""
        if cls.db_client is None:
            cls.db_client = InternalConfig.db["pending_rebalances"]
        return cls.db_client

    @classmethod
    async def get_or_create(
        cls,
        base_asset: str,
        quote_asset: str,
        direction: RebalanceDirection,
        exchange: str = "binance",
    ) -> "PendingRebalance":
        """
        Get existing pending rebalance or create a new one.

        Args:
            base_asset: Base asset (e.g., 'HIVE')
            quote_asset: Quote asset (e.g., 'BTC')
            direction: Trade direction
            exchange: Exchange name

        Returns:
            PendingRebalance instance
        """
        collection = cls.collection()
        filter_query = {
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "direction": direction.value,
            "exchange": exchange,
        }

        existing = await mongo_call(lambda: collection.find_one(filter_query))
        if existing:
            return cls.model_validate(existing)

        # Create new pending rebalance
        pending = cls(
            base_asset=base_asset,
            quote_asset=quote_asset,
            direction=direction,
            exchange=exchange,
        )
        await pending.save()
        return pending

    async def save(self) -> None:
        """Save/update the pending rebalance in MongoDB."""
        self.updated_at = datetime.now(tz=timezone.utc)
        collection = self.collection()

        filter_query = {
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "direction": self.direction.value,
            "exchange": self.exchange,
        }

        # Convert Decimals to MongoDB-compatible types (Decimal128 or int)
        data = convert_decimals_for_mongodb(self.model_dump())

        await mongo_call(lambda: collection.update_one(filter_query, {"$set": data}, upsert=True))

    def add_pending(
        self,
        qty: Decimal,
        quote_value: Decimal,
        transaction_id: str | None = None,
    ) -> None:
        """
        Add a pending amount to be converted.

        Args:
            qty: Quantity of base asset to add
            quote_value: Estimated value in quote asset
            transaction_id: Optional transaction ID for audit trail
        """
        self.pending_qty += qty
        self.pending_quote_value += quote_value
        self.transaction_count += 1
        if transaction_id:
            self.transaction_ids.append(transaction_id)
        self.updated_at = datetime.now(tz=timezone.utc)

    def can_execute(self) -> tuple[bool, str]:
        """
        Check if accumulated amount meets thresholds for execution.

        Returns:
            tuple: (can_execute: bool, reason: str)
        """
        if self.pending_qty <= Decimal("0"):
            return False, "No pending quantity"

        if self.pending_qty < self.min_qty_threshold:
            return (
                False,
                f"Pending qty {self.pending_qty} below minimum {self.min_qty_threshold}",
            )

        if self.pending_quote_value < self.min_notional_threshold:
            return (
                False,
                f"Pending notional {self.pending_quote_value} below minimum {self.min_notional_threshold}",
            )

        return True, "OK"

    def reset_after_execution(self, executed_qty: Decimal) -> None:
        """
        Reset pending amounts after successful execution.

        Args:
            executed_qty: Quantity that was executed
        """
        self.total_executed_qty += executed_qty
        self.execution_count += 1
        self.last_executed_at = datetime.now(tz=timezone.utc)

        # Reset pending amounts (keep any remainder if partial fill)
        remainder = self.pending_qty - executed_qty
        if remainder > Decimal("0"):
            self.pending_qty = remainder
            # Estimate remaining quote value proportionally
            if executed_qty > Decimal("0"):
                ratio = remainder / (remainder + executed_qty)
                self.pending_quote_value = self.pending_quote_value * ratio
        else:
            self.pending_qty = Decimal("0")
            self.pending_quote_value = Decimal("0")

        # Clear transaction tracking
        self.transaction_ids = []
        self.transaction_count = 0

    @property
    def symbol(self) -> str:
        """Get the trading pair symbol."""
        return f"{self.base_asset}{self.quote_asset}"


class RebalanceResult(BaseModel):
    """Result of a rebalance attempt."""

    model_config = {"arbitrary_types_allowed": True}
    db_client: ClassVar[AsyncCollection | None] = None

    executed: bool = Field(default=False, description="Whether a trade was executed")
    reason: str = Field(default="", description="Reason for result")
    pending_qty: Decimal = Field(default=Decimal("0"), description="Current pending quantity")
    pending_notional: Decimal = Field(
        default=Decimal("0"), description="Current pending notional value"
    )
    order_result: ExchangeOrderResult | None = Field(
        default=None, description="Order result if executed"
    )
    error: str | None = Field(default=None, description="Error message if failed")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=timezone.utc))

    @classmethod
    def collection(cls) -> AsyncCollection:
        """Get the MongoDB collection for rebalance results."""
        if cls.db_client is None:
            cls.db_client = InternalConfig.db["rebalance_results"]
        return cls.db_client

    async def save(self) -> None:
        """Save the rebalance result to MongoDB (only for executed trades)."""
        if not self.executed:
            return  # Only save successful executions

        collection = self.collection()
        data = convert_decimals_for_mongodb(self.model_dump())

        await mongo_call(lambda: collection.insert_one(data))


async def add_pending_rebalance(
    exchange_adapter: BaseExchangeAdapter,
    base_asset: str,
    quote_asset: str,
    direction: RebalanceDirection,
    qty: Decimal,
    transaction_id: str | None = None,
) -> RebalanceResult:
    """
    Add a pending amount and attempt to execute if threshold is met.

    This is the main entry point for the rebalancing system. It:
    1. Gets or creates a PendingRebalance record
    2. Updates the exchange minimum thresholds
    3. Adds the new pending amount
    4. Checks if threshold is met and executes if so

    Args:
        exchange_adapter: Exchange adapter implementing ExchangeProtocol
        base_asset: Base asset (e.g., 'HIVE')
        quote_asset: Quote asset (e.g., 'BTC')
        direction: Trade direction (SELL_BASE_FOR_QUOTE or BUY_BASE_WITH_QUOTE)
        qty: Quantity of base asset to add
        transaction_id: Optional transaction ID for audit trail

    Returns:
        RebalanceResult with execution details
    """
    try:
        # Get or create pending rebalance record
        pending = await PendingRebalance.get_or_create(
            base_asset=base_asset,
            quote_asset=quote_asset,
            direction=direction,
            exchange=exchange_adapter.exchange_name,
        )

        # Update thresholds from exchange
        try:
            minimums = exchange_adapter.get_min_order_requirements(base_asset, quote_asset)
            pending.min_qty_threshold = minimums.min_qty
            pending.min_notional_threshold = minimums.min_notional
        except ExchangeConnectionError as e:
            logger.warning(f"Could not update minimums from exchange: {e}")

        # Estimate quote value for the new quantity
        try:
            price = exchange_adapter.get_current_price(base_asset, quote_asset)
            quote_value = qty * price
        except ExchangeConnectionError:
            # Use a rough estimate if we can't get current price
            quote_value = Decimal("0")
            logger.warning(
                f"Could not get price for {base_asset}/{quote_asset}, "
                "pending quote value may be inaccurate"
            )

        # Add the pending amount
        pending.add_pending(qty=qty, quote_value=quote_value, transaction_id=transaction_id)

        # Check if we can execute
        can_execute, reason = pending.can_execute()

        if not can_execute:
            await pending.save()
            logger.info(
                f"Rebalance pending: {pending.pending_qty} {base_asset} "
                f"(~{pending.pending_quote_value:.8f} {quote_asset}) - {reason}"
            )
            return RebalanceResult(
                executed=False,
                reason=reason,
                pending_qty=pending.pending_qty,
                pending_notional=pending.pending_quote_value,
            )

        # Execute the trade
        logger.info(
            f"Executing rebalance: {direction.value} {pending.pending_qty:.3f} {base_asset} "
            f"for {pending.pending_quote_value:.8f} {quote_asset}"
        )

        order_result = await execute_rebalance_trade(
            exchange_adapter=exchange_adapter,
            pending=pending,
        )

        # Update pending record after execution
        pending.reset_after_execution(order_result.executed_qty)
        await pending.save()

        logger.info(
            f"Rebalance executed: {order_result.executed_qty} {base_asset} "
            f"@ avg price {order_result.avg_price}"
        )

        result = RebalanceResult(
            executed=True,
            reason="Trade executed successfully",
            pending_qty=pending.pending_qty,
            pending_notional=pending.pending_quote_value,
            order_result=order_result,
        )
        await result.save()
        return result

    except ExchangeBelowMinimumError as e:
        logger.warning(f"Rebalance below minimum: {e}")
        return RebalanceResult(
            executed=False,
            reason=str(e),
            error=str(e),
        )
    except ExchangeConnectionError as e:
        logger.error(f"Exchange connection error during rebalance: {e}")
        return RebalanceResult(
            executed=False,
            reason="Exchange connection error",
            error=str(e),
        )
    except Exception as e:
        logger.error(f"Unexpected error during rebalance: {e}", exc_info=True)
        return RebalanceResult(
            executed=False,
            reason="Unexpected error",
            error=str(e),
        )


async def execute_rebalance_trade(
    exchange_adapter: BaseExchangeAdapter,
    pending: PendingRebalance,
) -> ExchangeOrderResult:
    """
    Execute the actual trade on the exchange.

    Args:
        exchange_adapter: Exchange adapter
        pending: PendingRebalance with accumulated amount

    Returns:
        ExchangeOrderResult with trade details
    """
    if pending.direction == RebalanceDirection.SELL_BASE_FOR_QUOTE:
        exchange_result = exchange_adapter.market_sell(
            base_asset=pending.base_asset,
            quote_asset=pending.quote_asset,
            quantity=pending.pending_qty,
        )
    else:
        exchange_result = exchange_adapter.market_buy(
            base_asset=pending.base_asset,
            quote_asset=pending.quote_asset,
            quantity=pending.pending_qty,
        )
    logger.info(exchange_result.log_str, extra={"notification": True, **exchange_result.log_extra})
    return exchange_result


async def get_pending_rebalances() -> list[PendingRebalance]:
    """
    Get all pending rebalances with non-zero amounts.

    Returns:
        List of PendingRebalance records
    """
    collection = PendingRebalance.collection()
    cursor = collection.find({"pending_qty": {"$ne": "0"}})
    results = []
    async for doc in cursor:
        results.append(PendingRebalance.model_validate(doc))
    return results


async def force_execute_pending(
    exchange_adapter: BaseExchangeAdapter,
    base_asset: str,
    quote_asset: str,
    direction: RebalanceDirection,
) -> RebalanceResult:
    """
    Force execute a pending rebalance regardless of minimums.

    Use with caution - this will fail if the exchange rejects the order.

    Args:
        exchange_adapter: Exchange adapter
        base_asset: Base asset
        quote_asset: Quote asset
        direction: Trade direction

    Returns:
        RebalanceResult with execution details
    """
    pending = await PendingRebalance.get_or_create(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=direction,
        exchange=exchange_adapter.exchange_name,
    )

    if pending.pending_qty <= Decimal("0"):
        return RebalanceResult(
            executed=False,
            reason="No pending quantity to execute",
            pending_qty=pending.pending_qty,
            pending_notional=pending.pending_quote_value,
        )

    try:
        order_result = await execute_rebalance_trade(
            exchange_adapter=exchange_adapter,
            pending=pending,
        )

        pending.reset_after_execution(order_result.executed_qty)
        await pending.save()

        result = RebalanceResult(
            executed=True,
            reason="Forced execution successful",
            pending_qty=pending.pending_qty,
            pending_notional=pending.pending_quote_value,
            order_result=order_result,
        )
        await result.save()
        return result
    except Exception as e:
        return RebalanceResult(
            executed=False,
            reason=f"Forced execution failed: {e}",
            error=str(e),
            pending_qty=pending.pending_qty,
            pending_notional=pending.pending_quote_value,
        )
