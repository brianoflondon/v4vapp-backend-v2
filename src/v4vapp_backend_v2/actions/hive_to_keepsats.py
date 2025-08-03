from datetime import datetime, timezone
from typing import List, Tuple

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import HiveToLightningError
from v4vapp_backend_v2.actions.hive_notification import send_transfer_custom_json
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer


async def hive_to_keepsats_deposit(
    hive_transfer: TrackedTransfer, msats_to_deposit: int, nobroadcast: bool = False
) -> Tuple[List[LedgerEntry], str, Amount]:
    """
    Handle a deposit to Keepsats from Hive, returns the ledger entries for this operation and
    the message and amount to be sent back to the customer as change.

    This is similar in its first 5 steps to
        actions.payment_success.hive_to_lightning_payment_success
        actions.hive_to_keepsats.hive_to_keepsats_deposit
    which runs after a Lightning Payment is found. or after receiving a Hive transfer.

    Args:
        hive_transfer (TrackedTransfer): The Hive transfer operation that was successful.
        msats_to_deposit (int): The amount in millisatoshis to deposit if we are not converting the
            entire Hive/HBD amount into Keepsats. (This is used for payments with change.)

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the deposit operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    quote = await TrackedBaseModel.nearest_quote(timestamp=hive_transfer.timestamp)
    if hive_transfer.conv is None or hive_transfer.conv.is_unset():
        await hive_transfer.update_conv(quote=quote)

    if not hive_transfer.conv:
        logger.error(
            "Conversion details not found for operation, failed to update conversion.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError("Conversion details not found for operation")

    ledger_entries_list: list[LedgerEntry] = []

    # If msats_to_deposit is provided, ensure it is not more than the transfer amount
    # Raise error if it exceeds the transfer amount
    if msats_to_deposit and msats_to_deposit > hive_transfer.conv.msats:
        logger.error(
            "msats_to_deposit exceeds the transfer amount, adjusting to transfer amount.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(
            f"msats_to_deposit {msats_to_deposit} exceeds the transfer amount {hive_transfer.conv.msats}."
        )

    # Identify the customer and server
    cust_id = hive_transfer.from_account
    server_id = hive_transfer.to_account

    return_hive_amount = (
        Amount("0.001 HIVE") if hive_transfer.amount.unit == Currency.HIVE else Amount("0.001 HBD")
    )

    if not msats_to_deposit or msats_to_deposit <= 0:
        # If no msats_to_deposit is provided, use the full amount of the transfer
        hive_amount_to_deposit_before_fee = hive_transfer.amount.beam - return_hive_amount
        amount_to_deposit_before_fee_conv = CryptoConversion(
            conv_from=hive_transfer.amount.unit,
            value=hive_amount_to_deposit_before_fee.amount,
            quote=quote,
        ).conversion
        msats_to_deposit = hive_transfer.conv.msats
    else:
        amount_to_deposit_before_fee_conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=msats_to_deposit,
            quote=quote,
        ).conversion

        hive_amount_to_deposit_before_fee = (
            amount_to_deposit_before_fee_conv.amount_hive
            if hive_transfer.unit == Currency.HIVE
            else amount_to_deposit_before_fee_conv.amount_hbd
        )

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
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Convert {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id} after fee {amount_to_deposit_conv.msats_fee / 1000:,.0f} sats",
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
        credit_amount=hive_amount_to_deposit_before_fee.amount,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries_list.append(conversion_ledger_entry)
    await conversion_ledger_entry.save()

    # MARK: 3 Contra Reconciliation Entry
    ledger_type = LedgerType.CONTRA_HIVE_TO_KEEPSATS
    contra_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        ledger_type=ledger_type,
        group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Contra asset for Keepsats {hive_amount_to_deposit_before_fee} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id, contra=False),
        debit_unit=hive_transfer.unit,
        debit_amount=hive_amount_to_deposit_before_fee.amount,
        debit_conv=amount_to_deposit_before_fee_conv,
        credit=AssetAccount(
            name="Converted Keepsats Offset",
            sub=server_id,  # This is the Server
            contra=True,
        ),
        credit_unit=hive_transfer.unit,
        credit_amount=hive_amount_to_deposit_before_fee.amount,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries_list.append(contra_ledger_entry)
    await contra_ledger_entry.save()

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
        timestamp=datetime.now(tz=timezone.utc),
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
    await fee_ledger_entry.save()

    # MARK: 5 Deposit Keepsats
    ledger_type = LedgerType.WITHDRAW_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=hive_transfer.short_id,
        op_type=hive_transfer.op_type,
        user_memo=hive_transfer.user_memo,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Deposit Keepsats {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the Customer Keepsats Lightning balance
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=hive_deposit_value,
        debit_conv=amount_to_deposit_conv,
        credit=LiabilityAccount(
            name="Customer Liability",
            sub=server_id,  # This is the asset account for the server, where keepsats are held
        ),
        credit_unit=Currency.MSATS,
        credit_amount=amount_to_deposit_msats,
        credit_conv=amount_to_deposit_conv,
    )
    ledger_entries_list.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    # TODO: INSTEAD of this transaction, this should be a custom_json and that will transfer from `server_id` to `cust_id`
    # Want a two step process.... first deposit to the server then the customer.

    # MARK: 6  Deposit to Customer

    transfer = KeepsatsTransfer(
        from_account=server_id,
        to_account=cust_id,
        msats=amount_to_deposit_msats,
        memo=f"Deposit {hive_transfer.amount_str} to Keepsats {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
        parent_id=hive_transfer.group_id,  # This is the group_id of the original transfer
    )
    transfer.memo += f" | § {hive_transfer.short_id}"
    trx = await send_transfer_custom_json(transfer=transfer, nobroadcast=nobroadcast)

    # ledger_type = LedgerType.CUSTOM_JSON_TRANSFER
    # deposit_ledger_entry = LedgerEntry(
    #     short_id=hive_transfer.short_id,
    #     op_type=hive_transfer.op_type,
    #     user_memo=hive_transfer.user_memo,
    #     cust_id=cust_id,
    #     ledger_type=ledger_type,
    #     group_id=f"{hive_transfer.group_id}-{ledger_type.value}",
    #     timestamp=datetime.now(tz=timezone.utc),
    #     description=f"Deposit Keepsats {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}",
    #     debit=LiabilityAccount(
    #         name="Customer Liability",
    #         sub=server_id,  # This is the Customer Keepsats Lightning balance
    #     ),
    #     debit_unit=Currency.MSATS,
    #     debit_amount=amount_to_deposit_msats,
    #     debit_conv=amount_to_deposit_conv,
    #     credit=LiabilityAccount(
    #         name="Customer Liability",
    #         sub=cust_id,  # This is the asset account for the server, where keepsats are held
    #     ),
    #     credit_unit=Currency.MSATS,
    #     credit_amount=amount_to_deposit_msats,
    #     credit_conv=amount_to_deposit_conv,
    # )
    # ledger_entries_list.append(deposit_ledger_entry)
    # await deposit_ledger_entry.save()

    reason = f"Keepsats deposit of {hive_transfer.amount_str} deposit to {amount_to_deposit_msats / 1000:,.0f} sats for {cust_id}"

    return ledger_entries_list, reason, return_hive_amount
