from typing import List, Union

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate


async def process_create_fill_order_op(
    limit_fill_order: Union[LimitOrderCreate, FillOrder, LimitOrderCancelled],
    nobroadcast: bool = False,
) -> List[LedgerEntry]:
    """
    Process limit order creation and fill order operations, creating appropriate ledger entries.
    This function handles three types of operations:
    1. LimitOrderCreate: Records the creation of a limit order with conversion data
    2. FillOrder: Records the execution of a fill order, handling various tracking scenarios
    3. LimitOrderCancelled: Records the cancellation of a limit order, handling appropriate ledger adjustments
    For LimitOrderCreate operations:
    - Only processes orders from customers other than the server itself
    - Fetches and stores conversion data if not already set
    - Creates a single ledger entry recording the escrow movement
    For FillOrder operations:
    - Calculates conversion data for both debit and credit sides if not already set
    - Handles four tracking scenarios:
        - Both parties tracked: Creates separate buyer and seller entries
        - Only buyer tracked: Creates a net entry for the buyer
        - Only seller tracked: Creates a net entry for the seller
        - Neither tracked: Skips the operation
    - Properly attributes debit/credit and conversion data based on party tracking status
    Args:
            limit_fill_order (Union[LimitOrderCreate, FillOrder]): The order operation to process.
                    Can be either a limit order creation or a fill order execution.
            nobroadcast (bool, optional): If True, prevents broadcasting of the operation.
                    Defaults to False.
    Returns:
            List[LedgerEntry]: A list of created and saved ledger entries. Empty list if the
                    operation is skipped (e.g., both parties untracked for FillOrder).
    Raises:
            Any exceptions from database save operations or quote retrieval.
    """
    ledger_entries = []
    tracked_customers = [InternalConfig().server_id]  # Replace with dynamic query if needed

    # Limit order creation - send value to escrow, this needs to be reversed when order fills or cancels.
    # TODO: Reverse or cancel escrow entry when order fills or cancels.
    if (
        isinstance(limit_fill_order, LimitOrderCreate)
        and limit_fill_order.owner in tracked_customers
    ):
        description = (
            f"Orderid: {limit_fill_order.orderid} selling {limit_fill_order.amount_to_sell.amount_decimal}"
            f"{limit_fill_order.amount_to_sell.unit} for at least "
            f"{limit_fill_order.min_to_receive.amount_decimal} {limit_fill_order.min_to_receive.unit}"
        )
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
        ledger_entry.description = description
        ledger_entry.ledger_type = LedgerType.LIMIT_ORDER_CREATE
        ledger_entry.debit_unit = ledger_entry.credit_unit = limit_fill_order.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = (
            limit_fill_order.amount_to_sell.amount_decimal
        )
        ledger_entry.debit_conv = ledger_entry.credit_conv = limit_fill_order.conv
        ledger_entry.cust_id = limit_fill_order.cust_id
        ledger_entries.append(ledger_entry)
    elif isinstance(limit_fill_order, LimitOrderCancelled):
        logger.info(f"Limit order cancelled: {limit_fill_order.orderid}")
        # For cancellations, we need to reverse the original escrow entry created by the LimitOrderCreate
        # This assumes we have a way to find that original entry, which might require a query
        order_ids = (
            LimitOrderCreate.check_hive_open_orders()
        )  # This will also clean up any missing orders from cache
        original_entry = await LedgerEntry().load_one_by_description_regex(
            regex=f"{limit_fill_order.orderid}", ledger_type=LedgerType.LIMIT_ORDER_CREATE.value
        )
        if original_entry:
            # Check if the full amount is being cancelled (i.e., no partial fills) by comparing the amount_back to the original amount_to_sell
            if limit_fill_order.amount_back.amount_decimal == original_entry.debit_amount:
                logger.info(
                    f"Reversing original LimitOrderCreate entry {original_entry.log_str} for cancelled order {limit_fill_order.orderid}",
                    extra={**limit_fill_order.log_extra, **original_entry.log_extra},
                )
                await original_entry.save(upsert=True, reverse=True)
            else:
                logger.warning(
                    f"Partial cancellation detected for order {limit_fill_order.orderid}. Amount back: {limit_fill_order.amount_back.amount_decimal}, Original amount: {original_entry.debit_amount}. Manual review may be needed.",
                    extra={**limit_fill_order.log_extra, **original_entry.log_extra},
                )
                # TODO: #297 Handle partial cancellation scenario if needed (e.g., create a new entry for the cancelled portion)

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
                group_id=f"{limit_fill_order.group_id}_{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"pays {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} for {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit} Orderid: {limit_fill_order.open_orderid}",
                debit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer debits their deposits for HIVE paid
                debit_unit=limit_fill_order.open_pays.unit,  # HIVE
                debit_amount=limit_fill_order.open_pays.amount_decimal,
                debit_conv=limit_fill_order.debit_conv,
                credit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer credits their deposits for HBD received
                credit_unit=limit_fill_order.current_pays.unit,  # HBD
                credit_amount=limit_fill_order.current_pays.amount_decimal,
                credit_conv=limit_fill_order.credit_conv,
                link=limit_fill_order.link,
            )
            ledger_entries.append(buyer_entry)

            ledger_type = LedgerType.FILL_ORDER_SELL
            seller_entry = LedgerEntry(
                group_id=f"{limit_fill_order.group_id}_{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"receives {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} for {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit} Orderid: {limit_fill_order.open_orderid}",
                debit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.open_owner
                ),  # Seller debits escrow for HBD delivered
                debit_unit=limit_fill_order.current_pays.unit,  # HBD
                debit_amount=limit_fill_order.current_pays.amount_decimal,
                debit_conv=limit_fill_order.credit_conv,  # Use credit_conv for seller's debit (HBD)
                credit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.open_owner
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
                group_id=f"{limit_fill_order.group_id}_{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"trades {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit} for {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} (external seller) Orderid: {limit_fill_order.open_orderid}",
                debit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer debits deposits for HIVE paid
                debit_unit=limit_fill_order.open_pays.unit,
                debit_amount=limit_fill_order.open_pays.amount_decimal,
                debit_conv=limit_fill_order.debit_conv,
                credit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.current_owner
                ),  # Buyer credits deposits for HBD received (net effect)
                credit_unit=limit_fill_order.current_pays.unit,
                credit_amount=limit_fill_order.current_pays.amount_decimal,
                credit_conv=limit_fill_order.credit_conv,
                link=limit_fill_order.link,
            )
            # Optionally, add a suspense entry for the external side (but don't save it if netting)
            # suspense_entry = LedgerEntry(... LiabilityAccount("External Market Suspense", sub="untracked") ...)
            ledger_entries.append(net_entry)
        elif seller_tracked and not buyer_tracked:
            # Seller tracked, buyer external: Single net entry for seller
            # Seller (open_owner) delivers open_pays and receives current_pays
            ledger_type = LedgerType.FILL_ORDER_NET
            net_entry = LedgerEntry(
                group_id=f"{limit_fill_order.group_id}_{ledger_type}",
                short_id=limit_fill_order.short_id,
                timestamp=limit_fill_order.timestamp,
                op_type=limit_fill_order.op_type,
                ledger_type=ledger_type,
                cust_id=limit_fill_order.cust_id,
                description=f"trades {limit_fill_order.open_pays.amount_decimal} {limit_fill_order.open_pays.unit} for {limit_fill_order.current_pays.amount_decimal} {limit_fill_order.current_pays.unit} (external buyer) Orderid: {limit_fill_order.open_orderid}",
                debit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.open_owner
                ),  # Seller debits deposits for what they receive (current_pays)
                debit_unit=limit_fill_order.current_pays.unit,
                debit_amount=limit_fill_order.current_pays.amount_decimal,
                debit_conv=limit_fill_order.credit_conv,
                credit=AssetAccount(
                    name="Traded Deposits Hive", sub=limit_fill_order.open_owner
                ),  # Seller credits deposits for what they deliver (open_pays)
                credit_unit=limit_fill_order.open_pays.unit,
                credit_amount=limit_fill_order.open_pays.amount_decimal,
                credit_conv=limit_fill_order.debit_conv,
                link=limit_fill_order.link,
            )
            ledger_entries.append(net_entry)
        else:
            # Neither tracked: Skip or log (not relevant to your entity)
            logger.info(
                f"Fill order between untracked parties: {limit_fill_order.current_owner} and {limit_fill_order.open_owner}. Skipping. Orderid: {limit_fill_order.open_orderid}"
            )
            return []

        # Now we need to mark the original limit order create entry as reversed since it's effectively closed by this fill
        # This assumes we have a way to find that original entry, which might require a query
        logger.info(
            f"Checking for completed {limit_fill_order.completed_order} LimitOrderCreate {limit_fill_order.log_str} to reverse",
            extra={**limit_fill_order.log_extra},
        )
        order_ids = (
            LimitOrderCreate.check_hive_open_orders()
        )  # This will also clean up any missing orders from cache
        if limit_fill_order.completed_order or limit_fill_order.open_orderid not in order_ids:
            original_entry = await LedgerEntry().load_one_by_op_type(
                short_id=limit_fill_order.short_id_p, op_type="limit_order_create"
            )
            if original_entry:
                logger.info(
                    f"Reversing original LimitOrderCreate entry {original_entry.log_str} for order {limit_fill_order.open_orderid}",
                    extra={**limit_fill_order.log_extra, **original_entry.log_extra},
                )
                await original_entry.save(upsert=True, reverse=True)
    # Save all entries
    for entry in ledger_entries:
        await entry.save()

    return ledger_entries  # Return the list
