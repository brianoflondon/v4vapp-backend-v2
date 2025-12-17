from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta
from v4vapp_backend_v2.process.lock_str_class import CustIDType


async def hold_keepsats(
    amount_msats: Decimal, cust_id: str, tracked_op: TrackedAny, fee: bool = False
) -> LedgerEntry:
    """
    Creates and saves a ledger entry representing the withdrawal of Keepsats from a customer's liability account to the treasury.
    Args:
        amount_msats (Decimal): The amount to withdraw, in millisatoshis.
        cust_id (str): The customer identifier.
        tracked_op (TrackedAny): The tracked operation associated with this withdrawal.
    Returns:
        LedgerEntry: The created and saved ledger entry for the withdrawal.
    Raises:
        Any exceptions raised by CryptoConversion.get_quote() or LedgerEntry.save().
    """
    fee_str = "_fee" if fee else ""
    amount_sats = (amount_msats / Decimal(1000)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    debit_conversion = CryptoConversion(conv_from=Currency.MSATS, value=amount_msats)
    await debit_conversion.get_quote()
    ledger_type = LedgerType.HOLD_KEEPSATS
    withdraw_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}_{ledger_type.value}{fee_str}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Hold Keepsats {amount_sats:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,  # This is the CUSTOMER
        ),
        debit_unit=Currency.MSATS,
        debit_amount=amount_msats,
        debit_conv=debit_conversion.conversion,
        credit=LiabilityAccount(name="VSC Liability", sub="keepsats"),
        credit_unit=Currency.MSATS,
        credit_amount=amount_msats,
        credit_conv=debit_conversion.conversion,
    )
    await withdraw_ledger_entry.save()
    return withdraw_ledger_entry


async def release_keepsats(tracked_op: TrackedAny, fee: bool = False) -> LedgerEntry | None:
    """
    Asynchronously releases keepsats by creating a release ledger entry based on an existing hold entry.

    This function searches for an existing HOLD_KEEPSATS ledger entry associated with the provided tracked operation.
    If found, it validates the entry and creates a corresponding RELEASE_KEEPSATS entry to reverse the hold,
    transferring the amount back to the customer's liability account. The lock duration is calculated and included
    in the description.

    Args:
        tracked_op (TrackedAny): The tracked operation containing group_id, short_id, and op_type.
        fee (bool, optional): If True, modifies the group_id to include a fee suffix for the hold entry lookup.
                             Defaults to False.

    Returns:
        LedgerEntry | None: The newly created release ledger entry if successful, or None if no matching hold entry
                            is found or validation fails.

    Raises:
        None explicitly, but may propagate exceptions from database operations or model validation.
    """
    ledger_type = LedgerType.HOLD_KEEPSATS
    fee_str = "-fee" if fee else ""
    group_id = f"{tracked_op.group_id}-{ledger_type.value}{fee_str}"
    existing_entry_raw = await LedgerEntry.collection().find_one(
        filter={"group_id": group_id},
    )
    if existing_entry_raw is None:
        logger.warning(f"No ledger entry found for group_id: {group_id}")
        return None
    existing_entry = LedgerEntry.model_validate(existing_entry_raw)
    if existing_entry is None:
        logger.warning(f"Failed to validate ledger entry for group_id: {group_id}")
        return None

    timestamp = datetime.now(tz=timezone.utc)
    lock_time = timestamp - existing_entry.timestamp

    ledger_type = LedgerType.RELEASE_KEEPSATS
    group_id = f"{tracked_op.group_id}-{ledger_type.value}"
    release_ledger_entry = LedgerEntry(
        cust_id=existing_entry.cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=group_id,
        timestamp=timestamp,
        description=f"Release Keepsats for {existing_entry.cust_id} after {format_time_delta(lock_time)}",
        debit=LiabilityAccount(name="VSC Liability", sub="keepsats"),
        debit_unit=Currency.MSATS,
        debit_amount=existing_entry.debit_amount,
        debit_conv=existing_entry.debit_conv,
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=existing_entry.cust_id,  # This is the CUSTOMER
        ),
        credit_unit=Currency.MSATS,
        credit_amount=existing_entry.credit_amount,
        credit_conv=existing_entry.credit_conv,
    )
    await release_ledger_entry.save()
    return release_ledger_entry


async def get_held_keepsats_balance(cust_id: CustIDType) -> Decimal:
    """
    Calculates the net keepsats balance currently on hold for a given customer ID.

    Args:
        cust_id (CustIDType): The customer ID to check held balance for.

    Returns:
        Decimal: The net held amount in msats (millisatoshis). Positive means held; 0 means none.
    """
    # Aggregate held amounts (HOLD_KEEPSATS)
    held_pipeline = [
        {"$match": {"cust_id": cust_id, "ledger_type": LedgerType.HOLD_KEEPSATS.value}},
        {"$group": {"_id": None, "total_held": {"$sum": "$debit_amount"}}},
    ]
    held_result = await LedgerEntry.collection().aggregate(held_pipeline).to_list(length=1)
    total_held = held_result[0]["total_held"] if held_result else Decimal(0)

    # Aggregate released amounts (RELEASE_KEEPSATS)
    released_pipeline = [
        {"$match": {"cust_id": cust_id, "ledger_type": LedgerType.RELEASE_KEEPSATS.value}},
        {"$group": {"_id": None, "total_released": {"$sum": "$debit_amount"}}},
    ]
    released_result = await LedgerEntry.collection().aggregate(released_pipeline).to_list(length=1)
    total_released = released_result[0]["total_released"] if released_result else Decimal(0)

    # Net held = held - released
    net_held_msats = total_held - total_released
    return max(net_held_msats, Decimal(0))  # Ensure non-negative
