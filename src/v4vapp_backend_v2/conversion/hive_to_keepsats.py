"""
Internal conversions of Hive or HBD to Keepsats.
Operates by moving funds between the Server and VSC Liability accounts:

    Net value received into Assets (Debit Asset and Credit Liability).
->  pre performed Step 1: Receive Hive or HBD:
        Debit: Asset Customer Deposits Hive (server) - HIVE/HBD
        Credit: Liability VSC Liability (customer) - HIVE/HBD

    No net value change
    LedgerType.CONV_HIVE_TO_KEEPSATS h_conv_k
Step 2: Convert the received Hive or HBD to Keepsats in the Server's Asset account.
        Debit: Asset Treasury Lightning (server) - SATS
        Credit: Asset Customer Deposits Hive (server) - HIVE/HBD

    No net value change
    LedgerType.CONTRA_HIVE_TO_KEEPSATS h_contra_k
Step 3: Contra entry to keep Asset Customer Deposits Hive (server) balanced:
        Debit: Asset Customer Deposits Hive (server) - HIVE/HBD
        Credit: Asset Converted Keepsats Offset (server) - HIVE/HBD

    Net income change no change to DEA = LER
    LedgerType.FEE_INCOME fee_inc
Step 4: Fee Income
        Debit: Liability VSC Liability (customer) - HIVE/HBD
        Credit: Revenue Fee Income Keepsats (keepsats) - SATS

    No net value change (conversion to Keepsats on VSC)
Step 5: Deposit Keepsats into SERVER's Liability account:
        Debit: Liability VSC Liability (customer) - HIVE/HBD
        Credit: Liability VSC Liability (server) - SATS

    No net value change but net sats owned to customer
Then Send custom_json Transfer from Server to Customer:
        Debit: Liability VSC Liability (server) - SATS
        Credit: Liability VSC Liability (customer) - SATS


"""

import asyncio
from datetime import datetime, timedelta, timezone
from pprint import pprint
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedTransferWithCustomJson
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json


class HiveToKeepsatsConversionError(Exception):
    """Custom exception for Hive to Keepsats conversion errors."""

    pass


class WrongCurrencyError(HiveToKeepsatsConversionError):
    """Custom exception for wrong currency errors."""

    pass


async def conversion_hive_to_keepsats(
    server_id: str,
    cust_id: str,
    tracked_op: TrackedTransferWithCustomJson,
    convert_amount: Amount,
    msats: int = 0,
    nobroadcast: bool = False,
) -> None:
    """
    Converts a HIVE or HBD deposit to Keepsats (Lightning msats) and records the corresponding ledger entries.
    This function performs the following steps:
    1. Determines the appropriate conversion quote based on the timestamp of the tracked operation.
    2. Validates the currency for conversion (must be HIVE or HBD).
    3. Calculates the converted amount and associated fee in msats.
    4. Creates and saves ledger entries for:
        - The conversion transaction.
        - The contra asset entry.
        - The fee income.
        - The deposit of Keepsats.
    5. Initiates a Keepsats transfer from the server to the customer.
    Args:
         server_id (str): The identifier for the server handling the conversion.
         cust_id (str): The customer identifier receiving the Keepsats deposit.
         tracked_op (TrackedTransferWithCustomJson): The tracked transfer operation containing metadata and timestamp.
         convert_amount (Amount): The amount of HIVE or HBD to convert.
         msats (int, optional): The amount in millisatoshis (msats) for the conversion. Defaults to 0. If given
            it will override the convert_amount (but uses the Hive currency symbol from convert_amount)
         nobroadcast (bool, optional): If True, the transfer will not be broadcasted. Defaults to False.
    Raises:
         WrongCurrencyError: If the currency for conversion is not HIVE or HBD.
    Returns:
         None
    """

    if datetime.now(tz=timezone.utc) - tracked_op.timestamp > timedelta(minutes=5):
        quote = await TrackedBaseModel.nearest_quote(tracked_op.timestamp)
    else:
        quote = TrackedBaseModel.last_quote

    if convert_amount.symbol not in ["HIVE", "HBD"]:
        raise WrongCurrencyError("Invalid currency for conversion")

    from_currency = Currency(convert_amount.symbol.lower())
    if msats == 0:
        amount_to_deposit_conv = CryptoConversion(amount=convert_amount, quote=quote).conversion
    else:
        amount_to_deposit_conv = CryptoConversion(
            value=msats, conv_from=Currency.MSATS, quote=quote
        ).conversion
        convert_amount = amount_to_deposit_conv.amount(from_currency)

    msats_fee = amount_to_deposit_conv.msats_fee
    msats_fee_conv = CryptoConversion(
        value=msats_fee, conv_from=Currency.MSATS, quote=quote
    ).conversion
    hive_hbd_fee = msats_fee_conv.amount(from_currency)

    amount_to_deposit_before_fee = amount_to_deposit_conv.amount(from_currency) - hive_hbd_fee
    amount_to_deposit_before_fee_conv = CryptoConversion(
        amount=amount_to_deposit_before_fee, quote=quote
    ).conversion

    logger.info(f"{convert_amount=}")
    logger.info(f"{amount_to_deposit_before_fee=}")
    logger.info(f"{msats_fee=}")
    logger.info(f"{hive_hbd_fee}")

    ledger_entries: List[LedgerEntry] = []

    # MARK: 2. Convert
    ledger_type = LedgerType.CONV_HIVE_TO_KEEPSATS
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Convert {amount_to_deposit_conv.amount(from_currency)} deposit to {amount_to_deposit_conv.msats / 1000:,.0f} sats for {cust_id} after fee {amount_to_deposit_conv.msats_fee / 1000:,.0f} sats",
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
        credit_unit=from_currency,
        credit_amount=amount_to_deposit_before_fee.amount,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries.append(conversion_ledger_entry)
    await conversion_ledger_entry.save()

    # MARK: 3. Contra
    ledger_type = LedgerType.CONTRA_HIVE_TO_KEEPSATS
    contra_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Contra asset for Keepsats {amount_to_deposit_conv.amount(from_currency)} deposit to {amount_to_deposit_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id, contra=False),
        debit_unit=from_currency,
        debit_amount=amount_to_deposit_before_fee.amount,
        debit_conv=amount_to_deposit_before_fee_conv,
        credit=AssetAccount(
            name="Converted Keepsats Offset",
            sub=server_id,  # This is the Server
            contra=True,
        ),
        credit_unit=from_currency,
        credit_amount=amount_to_deposit_before_fee.amount,
        credit_conv=amount_to_deposit_before_fee_conv,
    )
    ledger_entries.append(contra_ledger_entry)
    await contra_ledger_entry.save()

    # MARK: 4 Fee Income
    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Keepsats deposit {amount_to_deposit_conv.amount(from_currency)} to {amount_to_deposit_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the Customer Keepsats Lightning balance
        ),
        debit_unit=from_currency,
        debit_amount=hive_hbd_fee.amount,
        debit_conv=msats_fee_conv,
        credit=RevenueAccount(
            name="Fee Income Keepsats",
            sub="keepsats",  # This is the Server
        ),
        credit_unit=Currency.MSATS,
        credit_amount=msats_fee,
        credit_conv=msats_fee_conv,
    )
    ledger_entries.append(fee_ledger_entry)
    await fee_ledger_entry.save()

    # MARK: 5 Deposit Keepsats
    ledger_type = LedgerType.WITHDRAW_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Deposit Keepsats {amount_to_deposit_conv.amount(from_currency)} to {amount_to_deposit_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,
        ),
        debit_unit=from_currency,
        debit_amount=convert_amount.amount,
        debit_conv=amount_to_deposit_conv,
        credit=LiabilityAccount(
            name="Customer Liability",
            sub=server_id,  # This is the asset account for the server, where keepsats are held
        ),
        credit_unit=Currency.MSATS,
        credit_amount=amount_to_deposit_conv.msats,
        credit_conv=amount_to_deposit_conv,
    )
    ledger_entries.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    if tracked_op.d_memo:
        memo = tracked_op.d_memo
    else:
        memo = f"Deposit Keepsats {amount_to_deposit_conv.amount(from_currency)} to {amount_to_deposit_conv.msats / 1000:,.0f} sats for {cust_id}"

    transfer = KeepsatsTransfer(
        from_account=server_id,
        to_account=cust_id,
        msats=amount_to_deposit_conv.msats,
        memo=memo,
        parent_id=tracked_op.group_id,  # This is the group_id of the original transfer
    )
    trx = await send_transfer_custom_json(transfer=transfer, nobroadcast=nobroadcast)
    await asyncio.sleep(1)  # Allow time for the transaction to be processed
    # Check if the transaction was successful
    ledger_entry_raw = await LedgerEntry.collection().find_one({"short_id": trx["id"]})
    pprint(ledger_entry_raw)
