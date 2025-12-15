from datetime import datetime, timezone
from decimal import Decimal

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, ExpenseAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedTransferKeepsatsToHive
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    RebalanceDirection,
    RebalanceResult,
    add_pending_rebalance,
)
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes
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
            await exchange_accounting(rebalance_result, tracked_op=tracked_op)

    except Exception as e:
        # Rebalance errors should not fail the customer transaction
        logger.warning(
            f"Rebalance queuing failed (non-critical): {e}",
            extra={"error": str(e), "group_id": tracked_op.group_id},
        )


async def exchange_accounting(
    rebalance_result: RebalanceResult, tracked_op: TransferBase | TrackedTransferKeepsatsToHive
) -> None:
    """Perform any accounting updates after a rebalance trade has executed."""
    if not rebalance_result.executed or rebalance_result.order_result is None:
        return

    order_result = rebalance_result.order_result

    # Use trade_quote from order_result - it now contains complete market rates
    # with the actual trade execution rate for sats_hive
    # Fall back to fetching current quote if trade_quote is not available
    if order_result.trade_quote and order_result.trade_quote.btc_usd > 0:
        trade_quote = order_result.trade_quote
    else:
        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes()
        trade_quote = all_quotes.quote
        logger.warning(
            f"trade_quote not available or incomplete, using market quote. "
            f"Order: {order_result.client_order_id}"
        )

    # Create CryptoConversion using the trade_quote
    # SELL: We sold HIVE (executed_qty) and received BTC (quote_qty)
    # BUY: We spent BTC (quote_qty) and received HIVE (executed_qty)
    if order_result.side.upper() == "BUY":
        # BUY HIVE: Start from msats spent, derive HIVE received
        msats_value = order_result.quote_qty * Decimal("100_000_000_000")
        crypto_conversion = CryptoConversion(
            conv_from=Currency.MSATS, value=msats_value, quote=trade_quote
        )
        conv = crypto_conversion.conversion
        debit_unit = Currency.MSATS
        credit_unit = Currency.HIVE
        debit_amount = conv.msats
        credit_amount = conv.hive
    else:  # SELL
        # SELL HIVE: Start from HIVE sold, derive msats received
        crypto_conversion = CryptoConversion(
            conv_from=Currency.HIVE, value=order_result.executed_qty, quote=trade_quote
        )
        conv = crypto_conversion.conversion
        debit_unit = Currency.HIVE
        credit_unit = Currency.MSATS
        debit_amount = conv.hive
        credit_amount = conv.msats

    # Create fee conversion from fee_msats using trade_quote for consistent rates
    fee_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=order_result.fee_msats,
        quote=trade_quote,
    ).conversion

    ledger_type = LedgerType.EXCHANGE_CONVERSION
    group_id_base = (
        f"{rebalance_result.order_result.exchange}_{rebalance_result.order_result.client_order_id}"
    )
    exchange_entry = LedgerEntry(
        ledger_type=ledger_type,
        short_id=rebalance_result.order_result.client_order_id,
        op_type="exchange_trade",
        cust_id=tracked_op.cust_id,
        group_id=f"{group_id_base}_{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=rebalance_result.log_str,
        debit=AssetAccount(name="Exchange Holdings", sub=rebalance_result.order_result.exchange),
        debit_unit=debit_unit,
        debit_amount=debit_amount,
        debit_conv=conv,
        credit=AssetAccount(name="Exchange Holdings", sub=rebalance_result.order_result.exchange),
        credit_unit=credit_unit,
        credit_amount=credit_amount,
        credit_conv=conv,
    )
    await exchange_entry.save()

    # Record fee if there is one (fee_msats > 0)
    if order_result.fee_msats > 0:
        logger.info(f"Exchange fee conversion details: {fee_conv}")
        ledger_type = LedgerType.EXCHANGE_FEES
        fee_entry = LedgerEntry(
            ledger_type=ledger_type,
            short_id=rebalance_result.order_result.client_order_id,
            op_type="exchange_fee",
            cust_id=tracked_op.cust_id,
            group_id=f"{group_id_base}_{ledger_type.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Exchange fee for {rebalance_result.log_str}",
            debit=ExpenseAccount(
                name="Exchange Fees Paid", sub=rebalance_result.order_result.exchange
            ),
            debit_unit=Currency.MSATS,
            debit_amount=fee_conv.msats,
            debit_conv=fee_conv,
            credit=AssetAccount(
                name="Exchange Holdings", sub=rebalance_result.order_result.exchange
            ),
            credit_unit=Currency.MSATS,
            credit_amount=fee_conv.msats,
            credit_conv=fee_conv,
        )
        await fee_entry.save()

    return
