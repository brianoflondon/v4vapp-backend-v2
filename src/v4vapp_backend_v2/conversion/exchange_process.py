from decimal import Decimal

from v4vapp_backend_v2.actions.tracked_any import TrackedTransferKeepsatsToHive
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    RebalanceDirection,
    RebalanceResult,
    add_pending_rebalance,
)
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase


async def rebalance_queue_task(
    direction: RebalanceDirection,
    currency: Currency,
    hive_qty: Decimal,
    tracked_op: TransferBase | TrackedTransferKeepsatsToHive,
) -> None:
    # When HIVE/HBD is deposited, we accumulate the amount for eventual sale to BTC
    # This runs in background and doesn't affect customer transaction
    # Note: Exchange selection is driven by config (default_exchange setting)
    if currency.name in ("HIVE", "HBD"):
        try:
            # Always use HIVE for exchange - Binance doesn't trade HBD
            # Get exchange adapter based on config (uses default_exchange)
            exchange_adapter = get_exchange_adapter()
            rebalance_result = await add_pending_rebalance(
                exchange_adapter=exchange_adapter,
                base_asset="HIVE",  # Always HIVE - Binance doesn't trade HBD
                quote_asset="BTC",
                direction=direction,
                qty=hive_qty,
                transaction_id=str(tracked_op.short_id),
            )
            logger.info(
                f"{rebalance_result.log_str}",
                extra={**rebalance_result.log_extra, "group_id": tracked_op.group_id},
            )

            if rebalance_result.error:
                logger.warning(
                    f"Rebalance queuing encountered an error (non-critical): {rebalance_result.error}",
                    extra={**rebalance_result.log_extra, "group_id": tracked_op.group_id},
                )
            if rebalance_result.executed:
                await exchange_accounting(rebalance_result)

        except Exception as e:
            # Rebalance errors should not fail the customer transaction
            logger.warning(
                f"Rebalance queuing failed (non-critical): {e}",
                extra={"error": str(e), "group_id": tracked_op.group_id},
            )


async def exchange_accounting(rebalance_result: RebalanceResult) -> None:
    """Perform any accounting updates after a rebalance trade has executed."""
    


    pass
