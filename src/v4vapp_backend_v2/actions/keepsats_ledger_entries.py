from datetime import datetime, timezone

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency


async def withdraw_keepsats_to_treasury(
    amount_msats: int, cust_id: str, hive_transfer: TrackedTransfer
) -> LedgerEntry:
    """
    Creates and saves a ledger entry representing the withdrawal of Keepsats from a customer's liability account to the treasury.
    Args:
        amount_msats (int): The amount to withdraw, in millisatoshis.
        cust_id (str): The customer identifier.
        hive_transfer (TrackedTransfer): The tracked transfer operation associated with this withdrawal.
    Returns:
        LedgerEntry: The created and saved ledger entry for the withdrawal.
    Raises:
        Any exceptions raised by CryptoConversion.get_quote() or LedgerEntry.save().
    """

    debit_conversion = CryptoConversion(conv_from=Currency.MSATS, value=amount_msats)
    await debit_conversion.get_quote()
    ledger_type = LedgerType.WITHDRAW_KEEPSATS
    withdraw_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"hold_{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        op=hive_transfer,
        description=f"Withdrawal Keepsats {amount_msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the CUSTOMER
        ),
        debit_unit=Currency.MSATS,
        debit_amount=amount_msats,
        debit_conv=debit_conversion.conversion,
        credit=AssetAccount(name="Treasury Lightning", sub="keepsats"),
        credit_unit=Currency.MSATS,
        credit_amount=amount_msats,
        credit_conv=debit_conversion.conversion,
    )
    await withdraw_ledger_entry.save()
    return withdraw_ledger_entry
