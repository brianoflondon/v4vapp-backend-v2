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
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pymongo.asynchronous.collection import AsyncCollection

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    BaseExchangeAdapter,
    ExchangeBelowMinimumError,
    ExchangeConnectionError,
    ExchangeOrderResult,
    format_base_asset,
    format_quote_asset,
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

    model_config = ConfigDict(arbitrary_types_allowed=True)
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
            pending_qty_str = format_base_asset(self.pending_qty, self.base_asset)
            min_qty_str = format_base_asset(self.min_qty_threshold, self.base_asset)
            return (
                False,
                f"Pending qty {pending_qty_str} below minimum {min_qty_str}",
            )

        if self.pending_quote_value < self.min_notional_threshold:
            quote_value_str = format_quote_asset(self.pending_quote_value, self.quote_asset)
            min_notional_str = format_quote_asset(self.min_notional_threshold, self.quote_asset)
            return (
                False,
                f"Pending notional {quote_value_str} below minimum {min_notional_str}",
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

    @property
    def log_str(self) -> str:
        """Formatted string for logging the pending rebalance."""
        can_exec, reason = self.can_execute()
        status = "ready" if can_exec else "pending"
        qty_str = format_base_asset(self.pending_qty, self.base_asset)
        quote_str = format_quote_asset(self.pending_quote_value, self.quote_asset)
        return (
            f"PendingRebalance [{status}]: {self.direction.value} "
            f"{qty_str} (~{quote_str}) "
            f"on {self.exchange}, {self.transaction_count} txns accumulated"
        )

    @property
    def log_extra(self) -> dict:
        """Dictionary of pending rebalance details for structured logging."""
        return {
            "pending_rebalance": self.model_dump(
                by_alias=True, exclude_none=True, exclude_unset=True
            )
        }

    @property
    def notification_str(self) -> str:
        """Formatted string for user notifications (Telegram, etc.)."""
        can_exec, _ = self.can_execute()
        qty_str = format_base_asset(self.pending_qty, self.base_asset)
        quote_str = format_quote_asset(self.pending_quote_value, self.quote_asset)
        if can_exec:
            return f"🔄 Ready to {self.direction.value}: {qty_str} (~{quote_str})"
        else:
            pct_qty = (
                (self.pending_qty / self.min_qty_threshold * 100)
                if self.min_qty_threshold > 0
                else Decimal("0")
            )
            min_str = format_base_asset(self.min_qty_threshold, self.base_asset)
            return (
                f"⏳ Accumulating {self.direction.value}: "
                f"{qty_str} ({pct_qty:.0f}% of min {min_str})"
            )


class NetPosition(BaseModel):
    """
    Represents the net position after netting buy and sell sides.

    When customers convert HIVE→Lightning (sells HIVE for BTC on exchange)
    and Lightning→HIVE (buys HIVE with BTC on exchange), these can partially
    offset each other. This model captures the net position.
    """

    base_asset: str = Field(..., description="Base asset (e.g., 'HIVE')")
    quote_asset: str = Field(..., description="Quote asset (e.g., 'BTC')")
    exchange: str = Field(default="binance", description="Exchange name")

    # Raw pending amounts from each side
    sell_pending_qty: Decimal = Field(
        default=Decimal("0"),
        description="Pending quantity to sell (from HIVE→Lightning conversions)",
    )
    sell_pending_notional: Decimal = Field(
        default=Decimal("0"),
        description="Pending notional value for sells",
    )
    buy_pending_qty: Decimal = Field(
        default=Decimal("0"),
        description="Pending quantity to buy (from Lightning→HIVE conversions)",
    )
    buy_pending_notional: Decimal = Field(
        default=Decimal("0"),
        description="Pending notional value for buys",
    )

    # Net position after offsetting
    net_qty: Decimal = Field(
        default=Decimal("0"),
        description="Net quantity (positive = need to sell, negative = need to buy)",
    )
    net_direction: RebalanceDirection | None = Field(
        default=None,
        description="Direction of net position (None if balanced)",
    )

    # Threshold info
    min_qty_threshold: Decimal = Field(default=Decimal("0"))
    min_notional_threshold: Decimal = Field(default=Decimal("0"))

    # Status
    can_execute: bool = Field(
        default=False,
        description="Whether net position meets thresholds for execution",
    )
    reason: str = Field(default="", description="Explanation of status")

    @property
    def is_balanced(self) -> bool:
        """Check if buy and sell sides are perfectly balanced."""
        return self.net_qty == Decimal("0")

    @property
    def abs_net_qty(self) -> Decimal:
        """Get absolute value of net quantity."""
        return abs(self.net_qty)


class RebalanceResult(BaseModel):
    """Result of a rebalance attempt."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
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

    @property
    def log_str(self) -> str:
        """Formatted string for logging the rebalance result."""
        if self.executed and self.order_result:
            base, quote = self.order_result._get_assets()
            qty_str = format_base_asset(self.order_result.executed_qty, base)
            fee_str = format_quote_asset(
                self.order_result.fee_original, self.order_result.fee_asset
            )
            return (
                f"Rebalance executed: {self.order_result.side} "
                f"{qty_str} @ {self.order_result.avg_price:.8f}, fee: {fee_str}"
            )
        elif self.error:
            return f"Rebalance failed: {self.error}"
        else:
            pending_notional_str = format_quote_asset(
                self.pending_notional,
                self.order_result.quote_asset if self.order_result else "BTC",
            )
            return (
                f"Rebalance pending: {self.pending_qty:.3f} qty, "
                f"{pending_notional_str} notional - {self.reason}"
            )

    @property
    def ledger_description(self) -> str:
        """
        Generate a description for ledger entries based on the rebalance result.
        Used in the exchange_accounting ledger entry.

        """
        if self.executed and self.order_result:
            base, quote = self.order_result._get_assets()
            qty_str = format_base_asset(self.order_result.executed_qty, base)
            fee_str = format_quote_asset(
                self.order_result.fee_original, self.order_result.fee_asset
            )
            return (
                f"{self.order_result.side} "
                f"{qty_str} @ {self.order_result.avg_price:.8f}, fee: {fee_str}"
            )
        return "Rebalance not executed"

    @property
    def log_extra(self) -> dict:
        """Dictionary of rebalance details for structured logging."""

        return {
            "rebalance_result": self.model_dump(
                by_alias=True, exclude_none=True, exclude_unset=True
            )
        }

        # extra = {
        #     "rebalance_result": {
        #         "executed": self.executed,
        #         "reason": self.reason,
        #         "pending_qty": str(self.pending_qty),
        #         "pending_notional": str(self.pending_notional),
        #         "error": self.error,
        #         "timestamp": self.timestamp.isoformat(),
        #     }
        # }
        # if self.order_result:
        #     extra["rebalance_result"]["order"] = self.order_result.model_dump()
        # return extra

    @property
    def notification_str(self) -> str:
        """Formatted string for user notifications (Telegram, etc.)."""
        if self.executed and self.order_result:
            base, quote = self.order_result._get_assets()
            qty_str = format_base_asset(self.order_result.executed_qty, base)
            quote_str = format_quote_asset(self.order_result.quote_qty, quote)
            return f"✅ Rebalance {self.order_result.side}: {qty_str} for {quote_str}"
        elif self.error:
            return f"❌ Rebalance error: {self.error}"
        else:
            return (
                f"⏳ Rebalance pending: {self.pending_qty:.3f} qty "
                f"({self.pending_notional:.8f} notional)"
            )

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
    transaction_id: str,
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
        transaction_id: Transaction ID for audit trail

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
                f"pending quote value may be inaccurate {transaction_id}"
            )

        # Add the pending amount
        pending.add_pending(qty=qty, quote_value=quote_value, transaction_id=transaction_id)
        logger.debug(pending.log_str, extra={"notification": True, **pending.log_extra})

        # Check if we can execute
        can_execute, reason = pending.can_execute()

        if not can_execute:
            await pending.save()
            return RebalanceResult(
                executed=False,
                reason=reason,
                pending_qty=pending.pending_qty,
                pending_notional=pending.pending_quote_value,
            )

        order_result = await execute_rebalance_trade(
            exchange_adapter=exchange_adapter,
            pending=pending,
        )

        # Update pending record after execution
        pending.reset_after_execution(order_result.executed_qty)
        await pending.save()

        logger.debug(order_result.log_str, extra={"notification": True, **order_result.log_extra})

        result = RebalanceResult(
            executed=True,
            reason="Trade executed successfully",
            pending_qty=pending.pending_qty,
            pending_notional=pending.pending_quote_value,
            order_result=order_result,
        )
        await result.save()

        logger.debug(result.log_str, extra={"notification": True, **result.log_extra})
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
    # Use the last transaction_id as the client order ID for tracking
    client_order_id = pending.transaction_ids[-1] if pending.transaction_ids else None

    if pending.direction == RebalanceDirection.SELL_BASE_FOR_QUOTE:
        exchange_result = exchange_adapter.market_sell(
            base_asset=pending.base_asset,
            quote_asset=pending.quote_asset,
            quantity=pending.pending_qty,
            client_order_id=client_order_id,
        )
    else:
        exchange_result = exchange_adapter.market_buy(
            base_asset=pending.base_asset,
            quote_asset=pending.quote_asset,
            quantity=pending.pending_qty,
            client_order_id=client_order_id,
        )
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


async def get_net_position(
    exchange_adapter: BaseExchangeAdapter,
    base_asset: str,
    quote_asset: str,
) -> NetPosition:
    """
    Calculate the net position by netting buy and sell pending amounts.

    This function looks up both the SELL and BUY pending rebalances for a
    trading pair and calculates the net position. If we have 100 HIVE pending
    to sell and 60 HIVE pending to buy, the net is 40 HIVE to sell.

    Args:
        exchange_adapter: Exchange adapter for getting thresholds
        base_asset: Base asset (e.g., 'HIVE')
        quote_asset: Quote asset (e.g., 'BTC')

    Returns:
        NetPosition with detailed breakdown and net calculation
    """
    exchange = exchange_adapter.exchange_name

    # Get both sell and buy pending records
    sell_pending = await PendingRebalance.get_or_create(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        exchange=exchange,
    )

    buy_pending = await PendingRebalance.get_or_create(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
        exchange=exchange,
    )

    # Get exchange minimums
    try:
        minimums = exchange_adapter.get_min_order_requirements(base_asset, quote_asset)
        min_qty = minimums.min_qty
        min_notional = minimums.min_notional
    except ExchangeConnectionError:
        min_qty = Decimal("0")
        min_notional = Decimal("0")
        logger.warning(f"Could not get minimums for {base_asset}/{quote_asset}")

    # Calculate net position
    # Positive net_qty = need to sell (more sells than buys)
    # Negative net_qty = need to buy (more buys than sells)
    net_qty = sell_pending.pending_qty - buy_pending.pending_qty

    # Determine direction and check thresholds
    if net_qty > Decimal("0"):
        net_direction = RebalanceDirection.SELL_BASE_FOR_QUOTE
        # Estimate notional for the net quantity
        try:
            price = exchange_adapter.get_current_price(base_asset, quote_asset)
            net_notional = net_qty * price
        except ExchangeConnectionError:
            net_notional = sell_pending.pending_quote_value - buy_pending.pending_quote_value
    elif net_qty < Decimal("0"):
        net_direction = RebalanceDirection.BUY_BASE_WITH_QUOTE
        net_qty_abs = abs(net_qty)
        try:
            price = exchange_adapter.get_current_price(base_asset, quote_asset)
            net_notional = net_qty_abs * price
        except ExchangeConnectionError:
            net_notional = buy_pending.pending_quote_value - sell_pending.pending_quote_value
    else:
        net_direction = None
        net_notional = Decimal("0")

    # Check if we can execute the net position
    abs_net_qty = abs(net_qty)
    abs_net_notional = abs(net_notional) if net_notional else Decimal("0")

    if abs_net_qty == Decimal("0"):
        can_execute = False
        reason = "Balanced: no net position to execute"
    elif abs_net_qty < min_qty:
        can_execute = False
        reason = f"Net qty {abs_net_qty} below minimum {min_qty}"
    elif abs_net_notional < min_notional:
        can_execute = False
        reason = f"Net notional {abs_net_notional:.8f} below minimum {min_notional}"
    else:
        can_execute = True
        reason = f"Ready to {net_direction.value if net_direction else 'N/A'} {abs_net_qty} {base_asset}"

    return NetPosition(
        base_asset=base_asset,
        quote_asset=quote_asset,
        exchange=exchange,
        sell_pending_qty=sell_pending.pending_qty,
        sell_pending_notional=sell_pending.pending_quote_value,
        buy_pending_qty=buy_pending.pending_qty,
        buy_pending_notional=buy_pending.pending_quote_value,
        net_qty=net_qty,
        net_direction=net_direction,
        min_qty_threshold=min_qty,
        min_notional_threshold=min_notional,
        can_execute=can_execute,
        reason=reason,
    )


async def execute_net_rebalance(
    exchange_adapter: BaseExchangeAdapter,
    base_asset: str,
    quote_asset: str,
) -> RebalanceResult:
    """
    Calculate net position and execute a trade if thresholds are met.

    This is the preferred way to execute rebalancing as it nets buy and sell
    sides before executing, minimizing unnecessary trades.

    Args:
        exchange_adapter: Exchange adapter
        base_asset: Base asset (e.g., 'HIVE')
        quote_asset: Quote asset (e.g., 'BTC')

    Returns:
        RebalanceResult with execution details
    """
    net_position = await get_net_position(
        exchange_adapter=exchange_adapter,
        base_asset=base_asset,
        quote_asset=quote_asset,
    )

    logger.debug(
        f"Net position: sell={net_position.sell_pending_qty} "
        f"buy={net_position.buy_pending_qty} "
        f"net={net_position.net_qty} {base_asset} "
        f"direction={net_position.net_direction}"
    )

    if not net_position.can_execute:
        return RebalanceResult(
            executed=False,
            reason=net_position.reason,
            pending_qty=net_position.abs_net_qty,
            pending_notional=abs(
                net_position.sell_pending_notional - net_position.buy_pending_notional
            ),
        )

    # At this point net_direction must be set (can_execute=True implies non-zero net)
    if net_position.net_direction is None:
        return RebalanceResult(
            executed=False,
            reason="No net direction (balanced position)",
            pending_qty=Decimal("0"),
            pending_notional=Decimal("0"),
        )

    net_direction = net_position.net_direction

    # Execute the net trade
    try:
        abs_net_qty = net_position.abs_net_qty

        if net_direction == RebalanceDirection.SELL_BASE_FOR_QUOTE:
            order_result = exchange_adapter.market_sell(
                base_asset=base_asset,
                quote_asset=quote_asset,
                quantity=abs_net_qty,
            )
        else:
            order_result = exchange_adapter.market_buy(
                base_asset=base_asset,
                quote_asset=quote_asset,
                quantity=abs_net_qty,
            )

        logger.info(order_result.log_str, extra={"notification": True, **order_result.log_extra})

        # Update both pending records to reflect the netting
        await _update_pending_after_net_execution(
            base_asset=base_asset,
            quote_asset=quote_asset,
            exchange=exchange_adapter.exchange_name,
            executed_qty=order_result.executed_qty,
            net_direction=net_direction,
        )

        result = RebalanceResult(
            executed=True,
            reason=f"Net rebalance executed: {net_direction.value} {order_result.executed_qty} {base_asset}",
            pending_qty=Decimal("0"),
            pending_notional=Decimal("0"),
            order_result=order_result,
        )
        await result.save()
        return result

    except ExchangeBelowMinimumError as e:
        logger.warning(f"Net rebalance below minimum: {e}")
        return RebalanceResult(
            executed=False,
            reason=str(e),
            error=str(e),
            pending_qty=net_position.abs_net_qty,
        )
    except ExchangeConnectionError as e:
        logger.error(f"Exchange connection error during net rebalance: {e}")
        return RebalanceResult(
            executed=False,
            reason="Exchange connection error",
            error=str(e),
            pending_qty=net_position.abs_net_qty,
        )
    except Exception as e:
        logger.error(f"Unexpected error during net rebalance: {e}", exc_info=True)
        return RebalanceResult(
            executed=False,
            reason="Unexpected error",
            error=str(e),
            pending_qty=net_position.abs_net_qty,
        )


async def _update_pending_after_net_execution(
    base_asset: str,
    quote_asset: str,
    exchange: str,
    executed_qty: Decimal,
    net_direction: RebalanceDirection,
) -> None:
    """
    Update both pending records after a net execution.

    When we execute a net trade, we need to:
    1. Clear the side that was completely consumed
    2. Reduce the other side by the offset amount
    3. Reset the executed side by the net amount

    Example: sell_pending=100, buy_pending=60, net=40 SELL
    After executing sell of 40:
    - buy_pending should be cleared (60 was "used" to offset)
    - sell_pending should be reduced by 100 (60 offset + 40 executed)
    """
    sell_pending = await PendingRebalance.get_or_create(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        exchange=exchange,
    )

    buy_pending = await PendingRebalance.get_or_create(
        base_asset=base_asset,
        quote_asset=quote_asset,
        direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
        exchange=exchange,
    )

    if net_direction == RebalanceDirection.SELL_BASE_FOR_QUOTE:
        # We sold the net amount
        # The buy side is completely consumed (used for offsetting)
        offset_qty = buy_pending.pending_qty
        buy_pending.pending_qty = Decimal("0")
        buy_pending.pending_quote_value = Decimal("0")
        buy_pending.transaction_ids = []
        buy_pending.transaction_count = 0

        # The sell side loses (offset + executed)
        total_consumed = offset_qty + executed_qty
        sell_pending.reset_after_execution(total_consumed)

    else:  # BUY_BASE_WITH_QUOTE
        # We bought the net amount
        # The sell side is completely consumed (used for offsetting)
        offset_qty = sell_pending.pending_qty
        sell_pending.pending_qty = Decimal("0")
        sell_pending.pending_quote_value = Decimal("0")
        sell_pending.transaction_ids = []
        sell_pending.transaction_count = 0

        # The buy side loses (offset + executed)
        total_consumed = offset_qty + executed_qty
        buy_pending.reset_after_execution(total_consumed)

    await sell_pending.save()
    await buy_pending.save()
