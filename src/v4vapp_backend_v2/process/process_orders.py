from typing import List, Union

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate


async def process_create_fill_order_op(
    limit_fill_order: Union[LimitOrderCreate, FillOrder], nobroadcast: bool = False
) -> List[LedgerEntry]:
    ledger_entries = []
    tracked_customers = [InternalConfig().server_id]  # Replace with dynamic query if needed

    if (
        isinstance(limit_fill_order, LimitOrderCreate)
        and limit_fill_order.owner not in tracked_customers
    ):
        logger.info(f"Limit order create: {limit_fill_order.orderid}")
        if not limit_fill_order.conv or limit_fill_order.conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.conv = CryptoConv(
                conv_from=limit_fill_order.amount_to_sell.unit,  # HIVE
                value=limit_fill_order.amount_to_sell.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.min_to_receive.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        ledger_entry = LedgerEntry(
            group_id=limit_fill_order.group_id,
            short_id=limit_fill_order.short_id,
            timestamp=limit_fill_order.timestamp,
            op_type=limit_fill_order.op_type,
            link=limit_fill_order.link,
        )
        ledger_entry.debit = AssetAccount(name="Escrow Hive", sub=limit_fill_order.owner)
        ledger_entry.credit = AssetAccount(
            name="Customer Deposits Hive", sub=limit_fill_order.owner
        )
        ledger_entry.description = limit_fill_order.ledger_str
        ledger_entry.ledger_type = LedgerType.LIMIT_ORDER_CREATE
        ledger_entry.debit_unit = ledger_entry.credit_unit = limit_fill_order.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = (
            limit_fill_order.amount_to_sell.amount_decimal
        )
        ledger_entry.debit_conv = ledger_entry.credit_conv = limit_fill_order.conv
        ledger_entry.cust_id = limit_fill_order.cust_id
        ledger_entries.append(ledger_entry)
    elif isinstance(limit_fill_order, FillOrder):
        logger.info(
            f"Fill order operation: {limit_fill_order.open_orderid} {limit_fill_order.current_owner}"
        )
        if not limit_fill_order.debit_conv or limit_fill_order.debit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.debit_conv = CryptoConv(
                conv_from=limit_fill_order.open_pays.unit,  # HIVE
                value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        if not limit_fill_order.credit_conv or limit_fill_order.credit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.credit_conv = CryptoConv(
                conv_from=limit_fill_order.current_pays.unit,  # HBD
                value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                converted_value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        seller_tracked = limit_fill_order.open_owner in tracked_customers
        buyer_tracked = limit_fill_order.current_owner in tracked_customers

        if buyer_tracked and seller_tracked:
            # Both tracked: Create two entries as before
            ledger_type = LedgerType.FILL_ORDER_BUY
            buyer_entry = LedgerEntry(
                group_id=f"{limit_fill_order.group_id}-{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"Fill order buyer: {limit_fill_order.current_owner} pays {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} for {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit}",
                debit=AssetAccount(
                    name="Customer Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer debits their deposits for HIVE paid
                debit_unit=limit_fill_order.open_pays.unit,  # HIVE
                debit_amount=limit_fill_order.open_pays.amount_decimal,
                debit_conv=limit_fill_order.debit_conv,
                credit=AssetAccount(
                    name="Escrow Hive", sub=limit_fill_order.current_owner
                ),  # Buyer credits escrow for HBD received
                credit_unit=limit_fill_order.current_pays.unit,  # HBD
                credit_amount=limit_fill_order.current_pays.amount_decimal,
                credit_conv=limit_fill_order.credit_conv,
                link=limit_fill_order.link,
            )
            ledger_entries.append(buyer_entry)

            ledger_type = LedgerType.FILL_ORDER_SELL
            seller_entry = LedgerEntry(
                group_id=f"{limit_fill_order.group_id}-{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"Fill order seller: {limit_fill_order.open_owner} receives {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} for {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit}",
                debit=AssetAccount(
                    name="Escrow Hive", sub=limit_fill_order.open_owner
                ),  # Seller debits escrow for HBD delivered
                debit_unit=limit_fill_order.current_pays.unit,  # HBD
                debit_amount=limit_fill_order.current_pays.amount_decimal,
                debit_conv=limit_fill_order.credit_conv,  # Use credit_conv for seller's debit (HBD)
                credit=AssetAccount(
                    name="Customer Deposits Hive", sub=limit_fill_order.open_owner
                ),  # Seller credits deposits for HIVE received
                credit_unit=limit_fill_order.open_pays.unit,  # HIVE
                credit_amount=limit_fill_order.open_pays.amount_decimal,
                credit_conv=limit_fill_order.debit_conv,  # Use debit_conv for seller's credit (HIVE)
                link=limit_fill_order.link,
            )
            ledger_entries.append(seller_entry)
        elif buyer_tracked and not seller_tracked:
            # Buyer tracked, seller external: Single net entry for buyer, use suspense for external
            ledger_type = LedgerType.FILL_ORDER_NET
            net_entry = LedgerEntry(
                group_id=f"{limit_fill_order.group_id}-{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"Net fill order: {limit_fill_order.current_owner} trades {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit} for {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} (external seller)",
                debit=AssetAccount(
                    name="Customer Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer debits deposits for HIVE paid
                debit_unit=limit_fill_order.open_pays.unit,
                debit_amount=limit_fill_order.open_pays.amount_decimal,
                debit_conv=limit_fill_order.debit_conv,
                credit=AssetAccount(
                    name="Customer Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer credits deposits for HBD received (net effect)
                credit_unit=limit_fill_order.current_pays.unit,
                credit_amount=limit_fill_order.current_pays.amount_decimal,
                credit_conv=limit_fill_order.credit_conv,
                link=limit_fill_order.link,
            )
            # Optionally, add a suspense entry for the external side (but don't save it if netting)
            # suspense_entry = LedgerEntry(... LiabilityAccount("External Market Suspense", sub="untracked") ...)
            ledger_entries.append(net_entry)
        else:
            # Neither tracked: Skip or log (not relevant to your entity)
            logger.info(
                f"Fill order between untracked parties: {limit_fill_order.current_owner} and {limit_fill_order.open_owner}. Skipping."
            )
            return []

    # Save all entries
    for entry in ledger_entries:
        await entry.save()

    return ledger_entries  # Return the list
