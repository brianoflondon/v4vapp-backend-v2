"""
Internal conversions of Keepsats to Hive or HBD.
Operates by moving funds between the Server and VSC Liability accounts:

->  pre performed Step 1: Customer's balance debited of sats
        Debit: Liability VSC Liability (customer) - SATS
        Credit: Liability VSC Liability (server) - SATS

    No net value change
    LedgerType.CONV_KEEPSATS_TO_HIVE k_conv_h
Step 2: Convert the keepsats into Hive or HBD Server's Asset account.
        Debit: Asset Customer Deposits Hive (server) - HIVE/HBD
        Credit: Asset Treasury Lightning (from_keepsats) - SATS

    No net value change
    LedgerType.CONTRA_KEEPSATS_TO_HIVE k_contra_h
Step 3: Contra entry to keep Asset Customer Deposits Hive (server) balanced:
        Debit: Asset Converted Keepsats Offset (from_keepsats) - HIVE/HBD
        Credit: Asset Customer Deposits Hive (server) - HIVE/HBD

    Net income change no change to DEA = LER
    LedgerType.FEE_INCOME fee_inc
Step 4: Fee Income
        Debit: Liability VSC Liability (customer) - SATS
        Credit: Revenue Fee Income Keepsats (from_keepsats) - SATS

    No net value change (conversion to Keepsats on VSC)
    LedgerType.DEPOSIT_HIVE deposit_h
Step 5: Deposit Hive into SERVER's Liability account:
        Debit: Liability VSC Liability (server) - HIVE/HBD
        Credit: Liability VSC Liability (customer) - HIVE/HBD





    No net value change but net sats owned to customer
Then Send hive Transfer from Server to Customer:
        Debit: Liability VSC Liability (server) - HIVE/HBD
        Credit: Liability VSC Liability (customer) - HIVE/HBD


"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedTransferKeepsatsToHive
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.calculate import ConversionResult, calc_keepsats_to_hive
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.conversion.exchange_rebalance import (
    RebalanceDirection,
    add_pending_rebalance,
)
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import is_clean_memo, process_clean_memo
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.process.hive_notification import reply_with_hive


async def conversion_keepsats_to_hive(
    server_id: str,
    cust_id: str,
    tracked_op: TrackedTransferKeepsatsToHive,
    msats: Decimal | None = None,
    amount: Amount | None = None,
    to_currency: Currency = Currency.HIVE,
    nobroadcast: bool = False,
    quote: QuoteResponse | None = None,
) -> None:
    """ """

    # Inbound Invoices contain the msats amount
    if not msats and not amount and isinstance(tracked_op, Invoice):
        msats = Decimal(tracked_op.value_msat)

    conv_result: ConversionResult | None = None

    if isinstance(tracked_op, Invoice):
        fixed_hive_quote = tracked_op.fixed_quote
        if fixed_hive_quote:
            quote = fixed_hive_quote.quote_response
            conv_result = fixed_hive_quote.conversion_result

    if not conv_result:
        conv_result = await calc_keepsats_to_hive(
            timestamp=tracked_op.timestamp,
            msats=msats,
            amount=amount,
            quote=quote,
            to_currency=to_currency,
        )
    to_currency = conv_result.to_currency
    logger.info(f"{tracked_op.group_id} {conv_result.log_str}")
    logger.info(f"Conversion result: \n{conv_result}")

    ledger_entries: List[LedgerEntry] = []
    # MARK: 2. Convert
    ledger_type = LedgerType.CONV_KEEPSATS_TO_HIVE
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=(
            f"Convert {conv_result.to_convert_conv.sats_rounded:,.0f} "
            f"into {conv_result.to_convert_amount} for {cust_id}"
        ),
        debit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,
        ),
        debit_unit=to_currency,
        debit_amount=conv_result.to_convert_conv.value_in(to_currency),
        debit_conv=conv_result.to_convert_conv,
        credit=AssetAccount(name="Treasury Lightning", sub="from_keepsats"),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.to_convert_conv.msats,
        credit_conv=conv_result.to_convert_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(conversion_ledger_entry)
    await conversion_ledger_entry.save()

    # MARK: 3. Contra
    ledger_type = LedgerType.CONTRA_KEEPSATS_TO_HIVE
    contra_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Contra Conversion: {conv_result.to_convert_conv.sats_rounded:,.0f} sats for {cust_id} Keepsats",
        debit=AssetAccount(name="Converted Keepsats Offset", sub="from_keepsats", contra=True),
        debit_unit=to_currency,
        debit_amount=conv_result.to_convert_conv.value_in(to_currency),
        debit_conv=conv_result.to_convert_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,  # This is the Server
            contra=False,
        ),
        credit_unit=to_currency,
        credit_amount=conv_result.to_convert_conv.value_in(to_currency),
        credit_conv=conv_result.to_convert_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(contra_ledger_entry)
    await contra_ledger_entry.save()

    # MARK: 4 Fee Income From Customer
    # The Fee is ALREADY to the server as part of the start of the conversion
    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Keepsats {conv_result.fee_conv.sats_rounded:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,  # Changed from server_id to cust_id for direct conversions
        ),
        debit_unit=Currency.MSATS,
        debit_amount=conv_result.fee_conv.msats,
        debit_conv=conv_result.fee_conv,
        credit=RevenueAccount(
            name="Fee Income Keepsats",
            sub="from_keepsats",  # This is the Server
        ),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.fee_conv.msats,
        credit_conv=conv_result.fee_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(fee_ledger_entry)
    await fee_ledger_entry.save()

    # MARK: Consume Customer SATS for Conversion
    # This is only necessary for direct sats to Hive conversions
    if isinstance(tracked_op, Invoice) and tracked_op.is_lndtohive:
        logger.info(f"Direct sats to Hive conversion {tracked_op.group_id}")
        ledger_type = LedgerType.CONSUME_CUSTOMER_KEEPSATS  # Add this to LedgerType
        consume_entry = LedgerEntry(
            short_id=tracked_op.short_id,
            op_type=tracked_op.op_type,
            cust_id=cust_id,
            ledger_type=ledger_type,
            group_id=f"{tracked_op.group_id}-{ledger_type.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Consume customer SATS for Keepsats-to-{to_currency} conversion {conv_result.net_to_receive_conv.sats_rounded:,.0f} msats for {cust_id}",  # Updated description
            debit=LiabilityAccount(name="VSC Liability", sub=cust_id),
            debit_unit=Currency.MSATS,
            debit_amount=conv_result.net_to_receive_conv.msats,  # Changed from to_convert_conv.msats to net_to_receive_conv.msats
            debit_conv=conv_result.net_to_receive_conv,
            credit=AssetAccount(name="Converted Keepsats Offset", sub="from_keepsats"),
            credit_unit=Currency.MSATS,
            credit_amount=conv_result.net_to_receive_conv.msats,  # Changed from to_convert_conv.msats to net_to_receive_conv.msats
            credit_conv=conv_result.net_to_receive_conv,
            link=tracked_op.link,
        )
        ledger_entries.append(consume_entry)
        await consume_entry.save()

    # MARK: 5 Hive to Keepsats Customer Deposit
    ledger_type = LedgerType.DEPOSIT_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Convert {conv_result.net_to_receive_amount} to {conv_result.net_to_receive_conv.sats_rounded:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=to_currency,
        debit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        debit_conv=conv_result.net_to_receive_conv,
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,
        ),
        credit_unit=to_currency,
        credit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        credit_conv=conv_result.net_to_receive_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    # MARK: Reclassify VSC sats Liability
    # Skip for direct conversions to avoid imbalance
    if not (isinstance(tracked_op, Invoice) and tracked_op.is_lndtohive):
        ledger_type = LedgerType.RECLASSIFY_VSC_SATS
        reclassify_sats_entry = LedgerEntry(
            short_id=tracked_op.short_id,
            op_type=tracked_op.op_type,
            cust_id=cust_id,
            ledger_type=ledger_type,
            group_id=f"{tracked_op.group_id}-{ledger_type.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Reclassify positive SATS from VSC {server_id} to Converted Keepsats Offset for Keepsats-to-Hive inflow",
            debit=LiabilityAccount(name="VSC Liability", sub=server_id),
            debit_unit=Currency.MSATS,
            debit_amount=conv_result.to_convert_conv.msats,
            debit_conv=conv_result.to_convert_conv,
            credit=AssetAccount(
                name="Converted Keepsats Offset", sub="from_keepsats", contra=True
            ),
            credit_unit=Currency.MSATS,
            credit_amount=conv_result.to_convert_conv.msats,
            credit_conv=conv_result.to_convert_conv,
            link=tracked_op.link,
        )
        ledger_entries.append(reclassify_sats_entry)
        await reclassify_sats_entry.save()

    lightning_memo = getattr(tracked_op, "lightning_memo", "")
    if not lightning_memo:
        lightning_memo = tracked_op.d_memo

    tracked_op.change_memo = process_clean_memo(tracked_op.d_memo)
    end_memo = f" | {tracked_op.change_memo}" if tracked_op.change_memo else lightning_memo
    if not is_clean_memo(lightning_memo):
        tracked_op.change_memo = (
            f"Converted {conv_result.to_convert_conv.sats_rounded:,.0f} sats to "
            f"{conv_result.net_to_receive_amount} "
            f"with fee: {conv_result.fee_conv.sats_rounded:,.0f} sats for {cust_id}"
            f"{end_memo}"
        )

    await tracked_op.update_conv(quote=quote)
    tracked_op.change_amount = AmountPyd(
        amount=conv_result.net_to_receive_amount,
    )
    tracked_op.change_conv = conv_result.net_to_receive_conv
    await tracked_op.save()

    details = HiveReturnDetails(
        tracked_op=tracked_op,
        original_memo=tracked_op.d_memo,
        reason_str=tracked_op.change_memo,
        action=ReturnAction.CONVERSION,
        pay_to_cust_id=cust_id,
        amount=tracked_op.change_amount,
        nobroadcast=nobroadcast,
        clean=is_clean_memo(lightning_memo),
    )

    await reply_with_hive(details, nobroadcast=nobroadcast)

    # MARK: Reclassify VSC Hive
    # This reclassification should happen AFTER Hive is successfully SENT.
    ledger_type = LedgerType.RECLASSIFY_VSC_HIVE
    reclassify_hive_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Reclassify negative {to_currency} from VSC {server_id} to Converted Keepsats Offset for Keepsats-to-Hive outflow",
        debit=AssetAccount(name="Converted Keepsats Offset", sub="from_keepsats", contra=True),
        debit_unit=to_currency,
        debit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        debit_conv=conv_result.net_to_receive_conv,
        credit=LiabilityAccount(name="VSC Liability", sub=server_id),
        credit_unit=to_currency,
        credit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        credit_conv=conv_result.net_to_receive_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(reclassify_hive_entry)
    await reclassify_hive_entry.save()

    # MARK: Queue Exchange Rebalance (BTC -> HIVE)
    # When converting keepsats to HIVE/HBD, we need to buy HIVE with BTC
    # This runs in background and doesn't affect customer transaction
    # Note: Exchange selection is driven by config (default_exchange setting)
    if to_currency.name in ("HIVE", "HBD"):
        try:
            # Always use HIVE for exchange - Binance doesn't trade HBD
            # The conv_result.net_to_receive_conv.hive contains the HIVE equivalent
            hive_qty = conv_result.net_to_receive_conv.hive

            # Get exchange adapter based on config (uses default_exchange)
            exchange_adapter = get_exchange_adapter()
            rebalance_result = await add_pending_rebalance(
                exchange_adapter=exchange_adapter,
                base_asset="HIVE",  # Always HIVE - Binance doesn't trade HBD
                quote_asset="BTC",
                direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
                qty=hive_qty,
                transaction_id=str(tracked_op.short_id),
            )
            logger.info(
                f"Rebalance queued: BTC->HIVE ({hive_qty:.3f} HIVE for {to_currency.name}), "
                f"executed={rebalance_result.executed}, "
                f"pending_qty={rebalance_result.pending_qty}",
                extra={
                    "rebalance_executed": rebalance_result.executed,
                    "rebalance_reason": rebalance_result.reason,
                    "pending_qty": str(rebalance_result.pending_qty),
                    "target_currency": to_currency.name,
                    "hive_equivalent": str(hive_qty),
                    "group_id": tracked_op.group_id,
                },
            )
        except Exception as e:
            # Rebalance errors should not fail the customer transaction
            logger.warning(
                f"Rebalance queuing failed (non-critical): {e}",
                extra={"error": str(e), "group_id": tracked_op.group_id},
            )


# Last line

# Last line
