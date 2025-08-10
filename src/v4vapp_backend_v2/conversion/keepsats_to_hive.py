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

from datetime import datetime, timezone
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.calculate import hive_to_keepsats
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json


async def conversion_hive_to_keepsats(
    server_id: str,
    cust_id: str,
    tracked_op: TransferBase,
    msats: int = 0,
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
    conv_result = await hive_to_keepsats(tracked_op=tracked_op, msats=msats, quote=quote)
    from_currency = conv_result.from_currency
    logger.info(f"{conv_result}")

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
        description=(
            f"Convert {conv_result.to_convert_conv.value_in(from_currency)} "
            f"into {conv_result.to_convert_conv.msats / 1000:,.0f} sats for {cust_id}"
        ),
        debit=AssetAccount(
            name="Treasury Lightning",
            sub="keepsats",  # This is the Customer Keepsats Lightning balance
        ),
        debit_unit=Currency.MSATS,
        debit_amount=conv_result.to_convert_conv.msats,
        debit_conv=conv_result.to_convert_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,  # This is the Server
        ),
        credit_unit=from_currency,
        credit_amount=conv_result.to_convert_conv.value_in(from_currency),
        credit_conv=conv_result.to_convert_conv,
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
        description=f"Contra asset for Keepsats Conversion: {conv_result.to_convert_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id, contra=False),
        debit_unit=from_currency,
        debit_amount=conv_result.to_convert_conv.value_in(from_currency),
        debit_conv=conv_result.to_convert_conv,
        credit=AssetAccount(
            name="Converted Keepsats Offset",
            sub=server_id,  # This is the Server
            contra=True,
        ),
        credit_unit=from_currency,
        credit_amount=conv_result.to_convert_conv.value_in(from_currency),
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
        debit_unit=from_currency,
        debit_amount=conv_result.fee_conv.value_in(from_currency),
        debit_conv=conv_result.fee_conv,
        credit=RevenueAccount(
            name="Fee Income Keepsats",
            sub="keepsats",  # This is the Server
        ),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.fee_conv.msats,
        credit_conv=conv_result.fee_conv,
    )
    ledger_entries.append(fee_ledger_entry)
    await fee_ledger_entry.save()

    # MARK: 5 Hive to Keepsats Customer Deposit
    ledger_type = LedgerType.WITHDRAW_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Move Hive to Keepsats {conv_result.net_to_receive_conv.amount(from_currency)} to {conv_result.net_to_receive_conv.msats / 1000:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,
        ),
        debit_unit=from_currency,
        debit_amount=conv_result.net_to_receive_conv.value_in(from_currency),
        debit_conv=conv_result.net_to_receive_conv,
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,  # This is the asset account for the server, where keepsats are held
        ),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.net_to_receive_conv.msats,
        credit_conv=conv_result.net_to_receive_conv,
    )
    ledger_entries.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    if tracked_op.d_memo:
        memo = tracked_op.d_memo
    else:
        memo = (
            f"Deposit Keepsats {conv_result.to_convert_conv.value_in(from_currency)} to "
            f"{conv_result.net_to_receive_conv.msats / 1000:,.0f} sats "
            f"with fee: {conv_result.fee_conv.msats / 1000:,.0f} for {cust_id}"
        )

    transfer = KeepsatsTransfer(
        from_account=server_id,
        to_account=cust_id,
        msats=conv_result.net_to_receive_conv.msats,
        memo=memo,
        parent_id=tracked_op.group_id,  # This is the group_id of the original transfer
    )
    trx = await send_transfer_custom_json(transfer=transfer, nobroadcast=nobroadcast)

    tracked_op.add_reply(
        reply_id=trx["trx_id"],
        reply_type="custom_json",
        reply_msat=conv_result.net_to_receive_conv.msats,
        reply_message=memo,
    )

    await tracked_op.update_conv(quote=quote)
    tracked_op.change_amount = AmountPyd(
        amount=Amount(amount=f"{conv_result.change:.3f} {from_currency.upper()}")
    )
    tracked_op.change_conv = conv_result.change_conv
    await tracked_op.save()


# Last line

# Last line
