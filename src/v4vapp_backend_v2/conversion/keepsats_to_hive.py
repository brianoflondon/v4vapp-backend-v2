"""
Internal conversions of Hive or HBD to Keepsats.
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
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedTransferWithCustomJson
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.calculate import keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.hive_notification import reply_with_hive


async def conversion_keepsats_to_hive(
    server_id: str,
    cust_id: str,
    tracked_op: TrackedTransferWithCustomJson,
    msats: int | None = None,
    amount: Amount | None = None,
    to_currency: Currency = Currency.HIVE,
    nobroadcast: bool = False,
    quote: QuoteResponse | None = None,
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

         msats (int, optional): The amount in millisatoshis (msats) for the conversion. Defaults to 0. If given
            it will override the convert_amount (but uses the Hive currency symbol from convert_amount)
         nobroadcast (bool, optional): If True, the transfer will not be broadcasted. Defaults to False.
    Raises:
         WrongCurrencyError: If the currency for conversion is not HIVE or HBD.
    Returns:
         None
    """

    conv_result = await keepsats_to_hive(
        timestamp=tracked_op.timestamp,
        msats=msats,
        amount=amount,
        quote=quote,
        to_currency=to_currency,
    )
    to_currency = conv_result.to_currency
    logger.info(f"{conv_result}")

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
            f"Convert {conv_result.to_convert_conv.msats / 1000:,.0f} "
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
        description=f"Contra asset for Keepsats Conversion: {conv_result.to_convert_conv.msats / 1000:,.0f} sats for {cust_id}",
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
    )
    ledger_entries.append(contra_ledger_entry)
    await contra_ledger_entry.save()

    # MARK: 4 Fee Income From Customer
    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Keepsats {conv_result.fee_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,  # This is the Customer Keepsats Lightning balance
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
    )
    ledger_entries.append(fee_ledger_entry)
    await fee_ledger_entry.save()

    # MARK: 5 Hive to Keepsats Customer Deposit
    ledger_type = LedgerType.DEPOSIT_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Move Keepsats {conv_result.net_to_receive_amount} to {conv_result.net_to_receive_conv.msats / 1000:,.0f} sats for {cust_id}",
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
    )
    ledger_entries.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    reason_str = (
        f"Converted {conv_result.to_convert_conv.msats / 1000:,.0f} sats "
        f"{conv_result.to_convert_amount} "
        f"with fee: {conv_result.fee_conv.msats / 1000:,.0f} for {cust_id}"
    )

    details = HiveReturnDetails(
        tracked_op=tracked_op,
        original_memo=tracked_op.d_memo,
        reason_str=reason_str,
        action=ReturnAction.CONVERSION,
        pay_to_cust_id=cust_id,
        amount=AmountPyd(amount=conv_result.net_to_receive_amount),
        nobroadcast=nobroadcast,
    )

    await reply_with_hive(details, nobroadcast=nobroadcast)


# Last line

# Last line
