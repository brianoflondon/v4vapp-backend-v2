from datetime import timedelta
from typing import List, Tuple

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import HiveToLightningError
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import timestamp_inc
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd


async def hive_to_keepsats_deposit(
    hive_transfer: TrackedTransfer, nobroadcast: bool = False
) -> Tuple[List[LedgerEntry], str, Amount]:
    """
    Handle a deposit to Keepsats from Hive, returns the ledger entries for this operation and
    the message and amount to be sent back to the customer as change.

    Args:
        hive_transfer (TrackedTransfer): The Hive transfer operation that was successful.

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the deposit operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    if hive_transfer.conv is None or hive_transfer.conv.is_unset():
        await hive_transfer.update_conv()

    if not hive_transfer.conv:
        logger.error(
            "Conversion details not found for operation, failed to update conversion.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError("Conversion details not found for operation")

    ledger_entries_list: list[LedgerEntry] = []
    quote = await TrackedBaseModel.nearest_quote(timestamp=hive_transfer.timestamp)

    # Identify the customer and server
    cust_id = hive_transfer.from_account
    server_id = hive_transfer.to_account

    # The hive_transfer is already locked from within process_hive_to_lightning in hive_to_lightning.py
    return_hive_amount: Amount = Amount("0.001 HIVE")  # Default return amount
    if hive_transfer.amount.unit == Currency.HIVE:
        return_hive_amount = Amount("0.001 HIVE")
    else:
        return_hive_amount = Amount("0.001 HBD")
    hive_transfer.change_amount = AmountPyd(amount=return_hive_amount)
    hive_transfer.change_conv = CryptoConversion(
        conv_from=hive_transfer.amount.unit,
        value=return_hive_amount.amount,
        quote=quote,
    ).conversion

    amount_to_deposit_before_fee = hive_transfer.amount.beam - return_hive_amount

    timestamp = timestamp_inc(hive_transfer.timestamp, inc=timedelta(seconds=0.01))

    amount_to_deposit_before_fee_conv = CryptoConversion(
        conv_from=hive_transfer.amount.unit,
        value=amount_to_deposit_before_fee.amount,
        quote=quote,
    ).conversion

    amount_to_deposit_msats = (
        amount_to_deposit_before_fee_conv.msats - amount_to_deposit_before_fee_conv.msats_fee
    )
    if amount_to_deposit_msats <= 0:
        logger.error(
            "Deposit amount is zero or negative after conversion, cannot proceed.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError("Deposit amount is zero or negative after conversion")

    amount_to_deposit_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=amount_to_deposit_msats,
        quote=quote,
    ).conversion
    hive_deposit_value = getattr(amount_to_deposit_conv, hive_transfer.unit.lower())

    # MARK: 2 Conversion of Hive to Sats
    ledger_type = LedgerType.CONV_HIVE_TO_KEEPSATS
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        ledger_type=ledger_type,
        group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Convert {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub="keepsats",  # This is the Customer Keepsats Lightning balance
        ),
        debit_unit=Currency.MSATS,
        debit_amount=amount_to_deposit_before_fee_conv.msats,
        debit_conv=amount_to_deposit_before_fee_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,  # This is the Server
        ),
        credit_unit=hive_transfer.unit,
        credit_amount=amount_to_deposit_before_fee.amount,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries_list.append(conversion_ledger_entry)
    # NOTE: The Treasury Lightning account now holds converted sats but in reality these are
    # Probably need a contra asset account for the Treasury Lightning account to track the conversion

    # MARK: 3 Contra Asset Account

    contra_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        ledger_type=LedgerType.CONTRA_HIVE_TO_KEEPSATS,
        group_id=f"{hive_transfer.group_id}-{LedgerType.CONTRA_HIVE_TO_KEEPSATS.value}",
        timestamp=next(timestamp),
        description=f"Contra asset for Keepsats {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id, contra=False),
        debit_unit=hive_transfer.unit,
        debit_amount=amount_to_deposit_before_fee.amount,
        debit_conv=amount_to_deposit_before_fee_conv,
        credit=AssetAccount(
            name="Converted Keepsats Offset",
            sub=server_id,  # This is the Server
            contra=True,
        ),
        credit_unit=hive_transfer.unit,
        credit_amount=amount_to_deposit_before_fee,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries_list.append(contra_ledger_entry)

    # MARK: 4 Fee Income
    ledger_type = LedgerType.FEE_INCOME
    fee_debit_conv = fee_credit_conv = CryptoConversion(
        conv_from=Currency.MSATS, value=amount_to_deposit_before_fee_conv.msats_fee, quote=quote
    ).conversion
    fee_debit_amount_float = getattr(fee_debit_conv, hive_transfer.unit.lower())
    fee_ledger_entry = LedgerEntry(
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Fee for Keepsats deposit {hive_transfer.amount_str} to {amount_to_deposit_msats / 1000:,.0f} sats deposit",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the Customer Keepsats Lightning balance
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=fee_debit_amount_float,
        debit_conv=fee_debit_conv,
        credit=RevenueAccount(
            name="Fee Income Keepsats",
            sub="keepsats",  # This is the Server
        ),
        credit_unit=Currency.MSATS,
        credit_amount=amount_to_deposit_before_fee_conv.msats_fee,
        credit_conv=fee_credit_conv,
    )
    ledger_entries_list.append(fee_ledger_entry)

    # MARK: 5 Convert to Keepsats in customer account into the Keepsats
    ledger_type = LedgerType.DEPOSIT_KEEPSATS
    deposit_ledger_entry = LedgerEntry(
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Deposit Keepsats {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the CUSTOMER
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=hive_deposit_value,
        debit_conv=amount_to_deposit_conv,
        credit=LiabilityAccount(name="Customer Liability", sub=cust_id),
        credit_unit=Currency.MSATS,
        credit_amount=amount_to_deposit_msats,
        credit_conv=amount_to_deposit_conv,
    )
    ledger_entries_list.append(deposit_ledger_entry)

    reason = f"Keepsats deposit of {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}"

    return ledger_entries_list, reason, return_hive_amount
