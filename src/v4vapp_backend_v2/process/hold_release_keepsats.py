import inspect
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Mapping, Sequence

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
    fee_str = "_fee" if fee else ""
    group_id = f"{tracked_op.group_id}_{ledger_type.value}{fee_str}"
    existing_entry_raw = await LedgerEntry.collection().find_one(
        filter={"group_id": group_id},
    )
    if existing_entry_raw is None:
        logger.info(
            f"No ledger entry found {tracked_op.short_id} group_id: {group_id}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        return None
    existing_entry = LedgerEntry.model_validate(existing_entry_raw)
    if existing_entry is None:
        logger.warning(
            f"Failed to validate ledger entry for group_id: {group_id}",
            extra={"notification": True, **tracked_op.log_extra},
        )
        return None

    timestamp = datetime.now(tz=timezone.utc)
    lock_time = timestamp - existing_entry.timestamp

    ledger_type = LedgerType.RELEASE_KEEPSATS
    group_id = f"{tracked_op.group_id}_{ledger_type.value}"
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
    if net_held_msats != Decimal(0):
        logger.warning(
            f"net held keepsats for cust_id {cust_id}: {net_held_msats} msats. This indicates a data inconsistency.",
            extra={"notification": True, "cust_id": cust_id},
        )
    return max(net_held_msats, Decimal(0))  # Ensure non-negative


async def archive_old_hold_release_keepsats_entries(
    older_than_days: int = 30, reverse_archive: bool = False
) -> int:
    """
    Archives old Reversed, HOLD_KEEPSATS and RELEASE_KEEPSATS ledger entries that are older than a specified number of days.

    When ``reverse_archive`` is False, matching entries are moved from the main ledger collection to the archived collection.
    When ``reverse_archive`` is True, matching entries are restored from the archived collection back into the main ledger collection.
    The restore path is non-destructive: entries are copied/merged back into the main collection and are not deleted from
    the archived collection.

    Args:
        older_than_days (int, optional): The age in days beyond which entries should be archived or restored. Defaults to 30.
        reverse_archive (bool, optional): If True, restore matching entries from the archived collection back into the main
            collection without removing them from the archive. Defaults to False.
    Returns:
        int: The number of matching entries processed (archived when ``reverse_archive`` is False, restored when it is True).
    """
    threshold_date = datetime.now(tz=timezone.utc) - timedelta(days=older_than_days)
    match_filter = {
        "$or": [
            {
                "ledger_type": {
                    "$in": [LedgerType.HOLD_KEEPSATS.value, LedgerType.RELEASE_KEEPSATS.value]
                }
            },
            {"reversed": {"$exists": True}},
        ],
        "timestamp": {"$lt": threshold_date},
    }
    logger.info(
        f"Archiving old Reversed, HOLD_KEEPSATS and RELEASE_KEEPSATS "
        f"entries older than {older_than_days} days. {threshold_date}",
        extra={"notification": False, "threshold_date": threshold_date.isoformat()},
    )

    to_collection_name = (
        LedgerEntry.archived_collection_name()
        if not reverse_archive
        else LedgerEntry.collection_name()
    )

    pipeline: Sequence[Mapping[str, Any]] = [
        {"$match": match_filter},
        {
            "$merge": {
                "into": to_collection_name,
                "whenMatched": "keepExisting",
                "whenNotMatched": "insert",
            }
        },
    ]

    from_collection = (
        LedgerEntry.collection() if not reverse_archive else LedgerEntry.archived_collection()
    )

    count = await from_collection.count_documents(match_filter)
    ledger_type_count = await from_collection.count_documents(
        {
            "ledger_type": {
                "$in": [LedgerType.HOLD_KEEPSATS.value, LedgerType.RELEASE_KEEPSATS.value]
            },
            "timestamp": {"$lt": threshold_date},
        }
    )
    if count == 0:
        logger.info(
            f"No Reversed, HOLD_KEEPSATS or RELEASE_KEEPSATS entries found older than {older_than_days} days.",
            extra={"notification": False},
        )
        return 0
    # if the ledger-type count is not even, abort to avoid partial archiving
    if ledger_type_count % 2 != 0:
        logger.warning(
            f"Expected an even number of HOLD_KEEPSATS and RELEASE_KEEPSATS entries to archive, but found {ledger_type_count}. Aborting to avoid partial archiving.",
            extra={"notification": True},
        )
        return 0
    try:
        agg = from_collection.aggregate(pipeline=pipeline)
        cursor = await agg if inspect.isawaitable(agg) else agg
        await cursor.to_list(length=None)
        if hasattr(cursor, "close"):
            close_result = cursor.close()
            if inspect.isawaitable(close_result):
                await close_result
    except Exception as e:
        logger.error(
            f"Error during archiving process: {e}",
            extra={"notification": True, "pipeline": pipeline},
        )
        return 0

    logger.info(
        f"Archived {count} Reversed, HOLD_KEEPSATS or RELEASE_KEEPSATS entries to archived_ledger.",
        extra={"notification": False},
    )
    # Now delete the original entries after archiving
    if not reverse_archive:
        try:
            delete_result = await from_collection.delete_many(match_filter)
            logger.info(
                f"Moved {delete_result.deleted_count} original Reversed, HOLD_KEEPSATS or RELEASE_KEEPSATS entries after archiving.",
                extra={"notification": False},
            )
        except Exception as e:
            logger.error(
                f"Error during deletion of original entries after archiving: {e}",
                extra={"notification": True, "match_filter": match_filter},
            )
            return 0

    return count
