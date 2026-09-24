import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, ExpenseAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    ExchangeOrderResult,
    get_exchange_adapter,
)
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    RebalanceDirection,
    RebalanceResult,
    add_pending_rebalance,
)
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency


class RebalanceTrackedOp(Protocol):
    """Minimum fields needed to queue and book an exchange rebalance."""

    group_id: str
    short_id: str
    cust_id: str

    @property
    def log_extra(self) -> dict[str, Any]: ...


async def rebalance_queue_task(
    direction: RebalanceDirection,
    currency: Currency,
    hive_qty: Decimal,
    tracked_op: RebalanceTrackedOp,
    *,
    base_asset: str = "HIVE",
    quote_asset: str = "BTC",
) -> None:
    """Accumulate and maybe execute a Convert swap. Default pair is HIVE/BTC.

    ``hive_qty`` is the quantity of ``base_asset`` (HIVE for Hive flows, DASH
    for Dash inbound). Hive callers that pass HBD still send the HIVE equivalent.
    """
    try:
        app_config = InternalConfig().config
        provider_name = app_config.exchange_config.default_exchange
        provider_config = app_config.exchange_config.get_provider(provider_name)
        if provider_config.active_network.no_trade:
            logger.info(
                "Skipping exchange rebalance: active exchange network has no_trade enabled",
                extra={
                    "notification": False,
                    "exchange_provider": provider_name,
                    "direction": direction.value,
                    "group_id": tracked_op.group_id,
                },
            )
            return

        logger.info(
            f"Queuing rebalance task with delay: {direction.value} {currency} "
            f"{hive_qty} {base_asset} for tracked operation {tracked_op.short_id}",
        )
        await asyncio.sleep(10)
        exchange_adapter = get_exchange_adapter()
        rebalance_result = await add_pending_rebalance(
            exchange_adapter=exchange_adapter,
            base_asset=base_asset,
            quote_asset=quote_asset,
            direction=direction,
            qty=hive_qty,
            transaction_id=str(tracked_op.short_id),
        )
        logger.info(
            f"{tracked_op.short_id} {rebalance_result.log_str}",
            extra={
                "notification": True,
                "group_id": tracked_op.group_id,
                "short_id": tracked_op.short_id,
                **rebalance_result.log_extra,
                **tracked_op.log_extra,
            },
        )

        if rebalance_result.error:
            return

        if rebalance_result.executed:
            await exchange_accounting(rebalance_result, tracked_op=tracked_op)

    except Exception as e:
        # Rebalance errors should not fail the customer transaction
        logger.error(
            f"Unexpected rebalance queuing failed: {e}",
            extra={"error": str(e), "group_id": tracked_op.group_id},
        )


def _order_base_asset(order_result: ExchangeOrderResult) -> str:
    if order_result.base_asset:
        return order_result.base_asset.upper()
    base, _quote = order_result._get_assets()
    return base.upper()


def _trade_legs(
    order_result: ExchangeOrderResult, trade_quote: QuoteResponse
) -> tuple[CryptoConv, Currency, Currency, Decimal, Decimal]:
    """Debit/credit units and amounts for an executed Convert/spot trade."""
    base = _order_base_asset(order_result)
    is_buy = order_result.side.upper() == "BUY"
    if base == "DASH":
        if is_buy:
            msats_value = order_result.quote_qty * Decimal(100_000_000_000)
            conv = CryptoConversion(
                conv_from=Currency.MSATS, value=msats_value, quote=trade_quote
            ).conversion
            return conv, Currency.DUFFS, Currency.MSATS, conv.duffs, conv.msats
        conv = CryptoConversion(
            conv_from=Currency.DASH, value=order_result.executed_qty, quote=trade_quote
        ).conversion
        return conv, Currency.MSATS, Currency.DUFFS, conv.msats, conv.duffs

    if is_buy:
        msats_value = order_result.quote_qty * Decimal(100_000_000_000)
        conv = CryptoConversion(
            conv_from=Currency.MSATS, value=msats_value, quote=trade_quote
        ).conversion
        return conv, Currency.HIVE, Currency.MSATS, conv.hive, conv.msats
    conv = CryptoConversion(
        conv_from=Currency.HIVE, value=order_result.executed_qty, quote=trade_quote
    ).conversion
    return conv, Currency.MSATS, Currency.HIVE, conv.msats, conv.hive


async def exchange_accounting(
    rebalance_result: RebalanceResult, tracked_op: RebalanceTrackedOp
) -> None:
    """Perform any accounting updates after a rebalance trade has executed."""
    if not rebalance_result.executed or rebalance_result.order_result is None:
        return

    order_result = rebalance_result.order_result

    if order_result.executed_qty < Decimal("0.0"):
        # Binance testnet no longer supports test trades.
        logger.warning(
            "Executed quantity is zero or negative, likely due to Binance testnet limitations. Skipping accounting entries.",
            extra={
                "notification": False,
            },
        )
        return

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

    # Asset increase is debited; asset decrease is credited.
    # HIVE trades use HIVE/MSATS; DASH trades use DUFFS/MSATS.
    conv, debit_unit, credit_unit, debit_amount, credit_amount = _trade_legs(
        order_result, trade_quote
    )

    # Create fee conversion from fee_msats using trade_quote for consistent rates
    fee_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=order_result.fee_msats,
        quote=trade_quote,
    ).conversion

    ledger_type = LedgerType.EXCHANGE_CONVERSION
    # Use order_id (unique per trade from exchange) as the primary key for group_id.
    # client_order_id (short_id of the triggering op) is kept as short_id for audit.
    group_id_base = f"{order_result.exchange}_{order_result.order_id}"
    short_id = order_result.client_order_id or order_result.order_id
    exchange_entry = LedgerEntry(
        ledger_type=ledger_type,
        short_id=short_id,
        op_type="exchange_trade",
        cust_id=tracked_op.cust_id,
        group_id=f"{group_id_base}_{ledger_type.value}",
        timestamp=datetime.now(tz=UTC),
        description=rebalance_result.ledger_description,
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
        logger.debug(f"Exchange fee conversion details: {fee_conv.log_str}")
        ledger_type = LedgerType.EXCHANGE_FEES
        fee_entry = LedgerEntry(
            ledger_type=ledger_type,
            short_id=short_id,
            op_type="exchange_fee",
            cust_id=tracked_op.cust_id,
            group_id=f"{group_id_base}_{ledger_type.value}",
            timestamp=datetime.now(tz=UTC),
            description=f"Exchange Fee for {rebalance_result.ledger_description}",
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
