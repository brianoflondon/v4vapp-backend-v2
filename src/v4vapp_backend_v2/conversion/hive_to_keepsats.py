"""
Internal conversion: HIVE/HBD -> Keepsats (msats).

High-level flow:
Precondition (external): Customer deposits HIVE/HBD to Server:
    Debit: Asset Customer Deposits Hive (server) - HIVE/HBD
    Credit: Liability VSC Liability (customer) - HIVE/HBD

1) Reserve fee msats:
    hold_keepsats(...) reserves fee msats to ensure fee collection.

2) Conversion ledger (CONV_HIVE_TO_KEEPSATS):
    Debit: Asset Treasury Lightning (server) - MSATS
    Credit: Asset Customer Deposits Hive (server) - HIVE/HBD
    (Records conversion at the agreed quote; no net balance change until
    transfers settle.)

3) Contra reclassification (CONTRA_HIVE_TO_KEEPSATS):
    Debit: Asset Customer Deposits Hive (server) - HIVE/HBD
    Credit: Asset Converted Keepsats Offset (server) - HIVE/HBD (contra asset)
    (Offsets the original deposit so converted value is not double-counted.)

4) Deposit keepsats into liabilities (WITHDRAW_HIVE):
    Debit: Liability VSC Liability (customer) - HIVE/HBD (reduce customer's
           HIVE liability)
    Credit: Liability VSC Liability (server) - MSATS (increase server's keepsats
           liability)
    (Represents the internal move prior to on-chain/custom_json transfers.)

5) Custom JSON transfers (external moves):
    a) Main keepsats transfer: Server -> Customer for full msats amount
       (includes fee).
    b) Fee transfer: Customer -> Server for fee msats.
    (Fees are collected via the fee custom_json transfer; fee was reserved by
    hold_keepsats.)

6) Post-processing:
    - Tracked op updated with conversion/change values and saved.
    - Rebalance task enqueued to manage base/quote exposure.

Notes:
- Fee income is realized via the fee custom_json transfer (the process that
  handles that transfer should record/recognize fee income). This function
  does not emit a separate fee ledger entry.
- Ledger entries are persisted immediately; the net effect depends on the
  external/custom_json transfers completing.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.calculate import calc_hive_to_keepsats
from v4vapp_backend_v2.conversion.exchange_process import rebalance_queue_task
from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceDirection
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import is_clean_memo, process_clean_memo
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json
from v4vapp_backend_v2.process.hold_release_keepsats import hold_keepsats


async def conversion_hive_to_keepsats(
    server_id: str,
    cust_id: str,
    tracked_op: TransferBase,
    msats: Decimal = Decimal(0),
    nobroadcast: bool = False,
    quote: QuoteResponse | None = None,
    value_sat_rounded: Decimal = Decimal(0),
    fee_sat_rounded: Decimal = Decimal(0),
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
    conv_result = await calc_hive_to_keepsats(tracked_op=tracked_op, msats=msats, quote=quote)

    # Reserve the fees amount
    await hold_keepsats(conv_result.fee_conv.msats, cust_id, tracked_op=tracked_op, fee=True)

    from_currency = conv_result.from_currency
    logger.debug(f"{tracked_op.group_id} {conv_result.log_str}")
    logger.debug(f"Conversion result: \n{conv_result}")

    ledger_entries: List[LedgerEntry] = []
    # MARK: 2. Convert
    ledger_type = LedgerType.CONV_HIVE_TO_KEEPSATS
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}_{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=(
            f"Convert {conv_result.to_convert_conv.value_in(from_currency)} "
            f"into {conv_result.to_convert_conv.sats_rounded:,.0f} sats for {cust_id}"
        ),
        debit=AssetAccount(
            name="Treasury Lightning",
            sub="to_keepsats",
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
        link=tracked_op.link,
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
        group_id=f"{tracked_op.group_id}_{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Contra Conversion: {conv_result.to_convert_conv.sats_rounded:,.0f} sats for {cust_id} Keepsats",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id, contra=False),
        debit_unit=from_currency,
        debit_amount=conv_result.to_convert_conv.value_in(from_currency),
        debit_conv=conv_result.to_convert_conv,
        credit=AssetAccount(
            name="Converted Keepsats Offset",
            sub="to_keepsats",
            contra=True,
        ),
        credit_unit=from_currency,
        credit_amount=conv_result.to_convert_conv.value_in(from_currency),
        credit_conv=conv_result.to_convert_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(contra_ledger_entry)
    await contra_ledger_entry.save()

    # MARK: 5 Hive to Keepsats Customer Deposit

    ledger_type = LedgerType.WITHDRAW_HIVE
    deposit_ledger_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}_{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Withdraw {conv_result.to_convert_amount} from {conv_result.net_to_receive_conv.sats_rounded:,.0f} sats for {cust_id}",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,
        ),
        debit_unit=from_currency,
        debit_amount=conv_result.to_convert_conv.value_in(from_currency),
        debit_conv=conv_result.to_convert_conv,
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,  # This is the asset account for the server, where keepsats are held
        ),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.to_convert_conv.msats,
        credit_conv=conv_result.to_convert_conv,
        link=tracked_op.link,
    )
    ledger_entries.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()

    tracked_op.change_memo = process_clean_memo(tracked_op.d_memo)
    end_memo = f" | {tracked_op.lightning_memo}" if tracked_op.lightning_memo else ""

    if "⚡️" in tracked_op.lightning_memo:
        if value_sat_rounded > 0:
            fee_text = ""
            if fee_sat_rounded > 0:
                fee_text = f" (fee: {fee_sat_rounded:,.0f} sats)"
            lightning_paid = (
                f"Your payment of {value_sat_rounded:,.0f} sats has been paid.{fee_text} | "
            )
        else:
            lightning_paid = f"Your Lightning Invoice of {conv_result.net_to_receive_conv.sats_rounded:,.0f} has been paid. | "
    else:
        lightning_paid = ""

    if not is_clean_memo(tracked_op.lightning_memo):
        tracked_op.change_memo = (
            f"{lightning_paid}"
            f"Deposit {conv_result.to_convert_amount} to "
            f"{conv_result.net_to_receive_conv.sats_rounded:,.0f} sats "
            f"with fee: {conv_result.fee_conv.sats_rounded:,.0f} for {cust_id}"
            f"{end_memo}"
        )

    await tracked_op.update_conv(quote=quote)
    tracked_op.change_amount = AmountPyd(
        amount=Amount(amount=f"{conv_result.change:.3f} {from_currency.upper()}")
    )
    tracked_op.change_conv = conv_result.change_conv
    await tracked_op.save()

    # MARK: Sending Keepsats and Fee
    # This needs to be a custom json transferring Keepsats from devser VSC Liability to customer
    # should be the FULL amount (including the fee)
    # Then the fee will be a separate custom json
    transfer = KeepsatsTransfer(
        from_account=server_id,
        to_account=cust_id,
        msats=int(conv_result.to_convert_conv.msats),
        memo=tracked_op.d_memo,
        parent_id=tracked_op.group_id,  # This is the group_id of the original transfer
    )
    trx = await send_transfer_custom_json(transfer=transfer, nobroadcast=nobroadcast)
    logger.debug(
        f"Sent main transfer custom_json: {trx['trx_id']}",
        extra={"trx": trx, **transfer.log_extra},
    )

    transfer_fee = KeepsatsTransfer(
        from_account=cust_id,
        to_account=server_id,
        msats=conv_result.fee_conv.msats,
        memo=f"Fee for Keepsats {conv_result.fee_conv.sats_rounded:,.0f} sats for {cust_id} #Fee #to_keepsats",
        parent_id=tracked_op.group_id,  # This is the group_id of the original transfer
    )
    trx = await send_transfer_custom_json(transfer=transfer_fee, nobroadcast=nobroadcast)
    logger.debug(
        f"Sent fee custom_json: {trx['trx_id']}", extra={"trx": trx, **transfer_fee.log_extra}
    )

    asyncio.create_task(
        rebalance_queue_task(
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            currency=from_currency,
            hive_qty=conv_result.to_convert_conv.hive,
            tracked_op=tracked_op,
        )
    )


# Last line

# Last line
