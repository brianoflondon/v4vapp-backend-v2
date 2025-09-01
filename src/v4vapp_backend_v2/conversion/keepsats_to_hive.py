"""
Internal conversions of Keepsats to Hive or HBD.
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
from v4vapp_backend_v2.actions.tracked_any import TrackedTransferKeepsatsToHive
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.conversion.calculate import ConversionResult, calc_keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import is_clean_memo, process_clean_memo
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.process.hive_notification import reply_with_hive


async def conversion_keepsats_to_hive(
    server_id: str,
    cust_id: str,
    tracked_op: TrackedTransferKeepsatsToHive,
    msats: int | None = None,
    amount: Amount | None = None,
    to_currency: Currency = Currency.HIVE,
    nobroadcast: bool = False,
    quote: QuoteResponse | None = None,
) -> None:
    """ """

    # Inbound Invoices contain the msats amount
    if not msats and not amount and isinstance(tracked_op, Invoice):
        msats = tracked_op.value_msat

    conv_result: ConversionResult | None = None

    if isinstance(tracked_op, Invoice):
        fixed_hive_quote = tracked_op.fixed_quote
        if fixed_hive_quote:
            quote = fixed_hive_quote.quote_response
            conv_result = fixed_hive_quote.conversion_result

    if not conv_result:
        conv_result = await calc_keepsats_to_hive(
            timestamp=tracked_op.timestamp,
            msats=msats,
            amount=amount,
            quote=quote,
            to_currency=to_currency,
        )
    to_currency = conv_result.to_currency
    logger.info(f"{tracked_op.group_id} {conv_result.log_str}")
    logger.info(f"Conversion result: \n{conv_result}")

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
        description=f"Contra Conversion: {conv_result.to_convert_conv.msats / 1000:,.0f} sats for {cust_id} Keepsats",
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

    # # MARK: 4 Fee Income From Customer
    # The Fee is ALREADY to the server as part of the start of the conversion
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
            sub=server_id,
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

    # MARK: Consume Customer SATS for Conversion
    # This is only necessary for direct sats to Hive conversions
    if isinstance(tracked_op, Invoice) and tracked_op.is_lndtohive:
        logger.info(f"Direct sats to Hive conversion {tracked_op.group_id}")
        ledger_type = LedgerType.CONSUME_CUSTOMER_KEEPSATS  # Add this to LedgerType
        consume_entry = LedgerEntry(
            short_id=tracked_op.short_id,
            op_type=tracked_op.op_type,
            cust_id=cust_id,
            ledger_type=ledger_type,
            group_id=f"{tracked_op.group_id}-{ledger_type.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Consume customer SATS for Keepsats-to-{to_currency} conversion {conv_result.to_convert_conv.msats / 1000:,.0f} msats for {cust_id}",
            debit=LiabilityAccount(name="VSC Liability", sub=cust_id),
            debit_unit=Currency.MSATS,
            debit_amount=conv_result.to_convert_conv.msats,
            debit_conv=conv_result.to_convert_conv,
            credit=AssetAccount(name="Treasury Lightning", sub="from_keepsats"),
            credit_unit=Currency.MSATS,
            credit_amount=conv_result.to_convert_conv.msats,
            credit_conv=conv_result.to_convert_conv,
        )
        ledger_entries.append(consume_entry)
        await consume_entry.save()

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

    # MARK: Reclassify VSC sats Liability

    ledger_type = LedgerType.RECLASSIFY_VSC_SATS
    reclassify_sats_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Reclassify positive SATS from VSC {server_id} to Converted Keepsats Offset for Keepsats-to-Hive inflow",
        debit=LiabilityAccount(name="VSC Liability", sub=server_id),
        debit_unit=Currency.MSATS,
        debit_amount=conv_result.net_to_receive_conv.msats,
        debit_conv=conv_result.net_to_receive_conv,
        credit=AssetAccount(name="Converted Keepsats Offset", sub="from_keepsats", contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=conv_result.net_to_receive_conv.msats,
        credit_conv=conv_result.net_to_receive_conv,
    )
    ledger_entries.append(reclassify_sats_entry)
    await reclassify_sats_entry.save()

    lightning_memo = getattr(tracked_op, "lightning_memo", "")
    if not lightning_memo:
        lightning_memo = tracked_op.d_memo

    tracked_op.change_memo = process_clean_memo(tracked_op.d_memo)
    end_memo = f" | {tracked_op.change_memo}" if tracked_op.change_memo else lightning_memo
    if not is_clean_memo(lightning_memo):
        tracked_op.change_memo = (
            f"Converted {conv_result.to_convert_conv.msats / 1000:,.0f} sats to "
            f"{conv_result.net_to_receive_amount} "
            f"with fee: {conv_result.fee_conv.msats / 1000:,.0f} sats for {cust_id}"
            f"{end_memo}"
        )

    await tracked_op.update_conv(quote=quote)
    tracked_op.change_amount = AmountPyd(
        amount=conv_result.net_to_receive_amount,
    )
    tracked_op.change_conv = conv_result.net_to_receive_conv
    await tracked_op.save()

    details = HiveReturnDetails(
        tracked_op=tracked_op,
        original_memo=tracked_op.d_memo,
        reason_str=tracked_op.change_memo,
        action=ReturnAction.CONVERSION,
        pay_to_cust_id=cust_id,
        amount=tracked_op.change_amount,
        nobroadcast=nobroadcast,
    )

    await reply_with_hive(details, nobroadcast=nobroadcast)

    # Thinking... the fee has already been transferred out of the customer's account by the initial transfer.
    # This is not THE SAME as the hive_to_keepsats because the fee is sent as prt of the initiation of the process
    # So the fee is already in the server_id's VSC Liability account.
    # transfer_fee = KeepsatsTransfer(
    #     from_account=cust_id,
    #     to_account=server_id,
    #     msats=conv_result.fee_conv.msats,
    #     memo=f"Fee for Keepsats {conv_result.fee_conv.msats / 1000:,.0f} sats for {cust_id} #Fee #from_keepsats",
    #     parent_id=tracked_op.group_id,  # This is the group_id of the original transfer
    # )
    # trx = await send_transfer_custom_json(transfer=transfer_fee, nobroadcast=nobroadcast)
    # logger.info(
    #     f"Sent fee custom_json: {trx['trx_id']}", extra={"trx": trx, **transfer_fee.log_extra}
    # )

    # MARK: Reclassify VSC Hive
    # This reclassification should happen AFTER Hive is successfully SENT.
    ledger_type = LedgerType.RECLASSIFY_VSC_HIVE
    reclassify_hive_entry = LedgerEntry(
        short_id=tracked_op.short_id,
        op_type=tracked_op.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{tracked_op.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Reclassify negative {to_currency} from VSC {server_id} to Converted Keepsats Offset for Keepsats-to-Hive outflow",
        debit=AssetAccount(name="Converted Keepsats Offset", sub="from_keepsats", contra=True),
        debit_unit=to_currency,
        debit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        debit_conv=conv_result.net_to_receive_conv,
        credit=LiabilityAccount(name="VSC Liability", sub=server_id),
        credit_unit=to_currency,
        credit_amount=conv_result.net_to_receive_conv.value_in(to_currency),
        credit_conv=conv_result.net_to_receive_conv,
    )
    ledger_entries.append(reclassify_hive_entry)
    await reclassify_hive_entry.save()


# Last line

# Last line
