from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal

from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency


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
    fee_str = "-fee" if fee else ""
    amount_sats = amount_msats.quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    debit_conversion = CryptoConversion(conv_from=Currency.MSATS, value=amount_msats)
    await debit_conversion.get_quote()
    ledger_type = LedgerType.HOLD_KEEPSATS
    withdraw_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}{fee_str}",
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

    ledger_type = LedgerType.RELEASE_KEEPSATS
    group_id = f"{tracked_op.group_id}-{ledger_type.value}"
    release_ledger_entry = LedgerEntry(
        cust_id=existing_entry.cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=group_id,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Release Keepsats for {existing_entry.cust_id}",
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
