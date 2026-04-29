from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import ReplyType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    ProcessedMemo,
    received_lightning_message,
)
from v4vapp_backend_v2.hive.hive_extras import get_verified_hive_client, send_custom_json
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.magi.magi_classes import ICON, MagiBTCTransferEvent
from v4vapp_backend_v2.magi.magi_general import send_magi_transaction
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.process.process_transfer import follow_on_transfer


async def process_magi_btc_transfer_event(
    magi_transfer: MagiBTCTransferEvent,
) -> List[LedgerEntry]:
    """
    Process a Magi BTC transfer event by recording it in the database and performing necessary actions.

    Args:
        magi_transfer (MagiBTCTransferEvent): The Magi BTC transfer event object.

    """
    logger.info(f"Processing Magi BTC transfer event: {magi_transfer.log_str}")

    if not magi_transfer.conv or magi_transfer.conv.is_unset():
        quote = await MagiBTCTransferEvent.nearest_quote(timestamp=magi_transfer.timestamp)
        await magi_transfer.update_conv(quote=quote)

    server_id = InternalConfig().server_id
    ledger_entries = []

    try:
        for custom_json in magi_transfer.custom_jsons or []:
            if custom_json.is_watched:
                vsc_call = VSCCall.model_validate(custom_json.json_data)
                if isinstance(vsc_call, VSCCall) and vsc_call.action == "transfer":
                    vsc_payload = VSCCallPayload.model_validate(vsc_call.payload)
                    # Outbound payment from the server to Magi.
                    if vsc_call.caller == f"hive:{server_id}":
                        if vsc_payload.to == vsc_call.caller:
                            logger.warning(
                                f"{ICON} Skipping Magi transfer event {magi_transfer.short_id} with self-transfer in custom JSON {custom_json.short_id}",
                                extra={"notification": False, **vsc_call.log_extra},
                            )
                            continue
                        logger.info(
                            f"{ICON} Found outgoing VSC transfer call from {server_id} in custom JSON {magi_transfer.short_id}",
                            extra={**vsc_call.log_extra},
                        )
                        ledger_entries = await magisats_outbound(
                            magi_transfer=magi_transfer, vsc_call=vsc_call
                        )
                    if vsc_payload.to == AccName(server_id).magi_prefix:
                        logger.info(
                            f"{ICON} Found incoming transfer to {server_id} in custom JSON {magi_transfer.short_id}",
                            extra={**vsc_call.log_extra},
                        )
                        ledger_entries = await magisats_inbound(
                            magi_transfer=magi_transfer, vsc_call=vsc_call
                        )
    except AssertionError as e:
        logger.error(
            f"{ICON} Assertion error while processing Magi BTC transfer event {magi_transfer.short_id}: {e}",
            extra={"error": str(e), **magi_transfer.log_extra},
        )

    except Exception as e:
        logger.error(
            f"{ICON} Unexpected error while processing Magi BTC transfer event {magi_transfer.short_id}: {e}",
            extra={"error": str(e), **magi_transfer.log_extra},
        )

    return ledger_entries


# MARK: FORWARD Magisats


async def forward_magisats(invoice: Invoice) -> None:
    """
    This function is responsible for forwarding #magisats to the appropriate destination.
    The specific logic for forwarding will depend on the requirements of the application.
    For example, it could involve transferring the sats to a specific wallet or account.

    Args:
        invoice (Invoice): The invoice object containing details about the #magisats to be forwarded.

    Returns:
        None

    """
    logger.info("Forwarding #magisats to the designated destination.")
    msats_fee = None
    fixed_quote = invoice.fixed_quote
    if fixed_quote:
        quote = fixed_quote.quote_response
        msats_fee = fixed_quote.msats_fee
    else:
        quote = await Invoice.nearest_quote(timestamp=invoice.timestamp)

    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv(quote=quote)

    if not invoice.conv or invoice.conv.is_unset():
        logger.error("Conversion details are missing for the invoice.")
        return

    if not msats_fee:
        msats_fee = invoice.conv.msats_fee

    amount_to_send_msats = Decimal(invoice.value_msat) - msats_fee
    amount_to_send_sats = Decimal(amount_to_send_msats / Decimal(1000)).quantize(
        Decimal("1."), rounding="ROUND_DOWN"
    )
    net_msats_fee = Decimal(invoice.value_msat) - amount_to_send_msats

    logger.info(
        f"Amount to forward (after fees): {amount_to_send_sats:,.0f} sats "
        f"fee: {net_msats_fee / 1000:.3f} sats {invoice.short_id}"
    )

    magi_to = AccName(invoice.cust_id).magi_prefix

    memo = received_lightning_message(invoice.memo, amount_to_send_sats)

    vsc_payload = VSCCallPayload(
        amount=str(amount_to_send_sats),
        to=magi_to,
        parent_id=invoice.group_id_p,
        msats_fee=str(net_msats_fee),
        memo=memo,
    )

    trx = await send_magi_transaction(vsc_payload=vsc_payload, nobroadcast=False)
    trx_id = trx.get("trx_id", "Failed") if trx else "Failed"

    invoice.add_reply(
        reply_id=trx_id,
        reply_type=ReplyType.MAGI_TRANSFER,
        reply_msat=0,
        reply_message=f"Sent {amount_to_send_sats:,.0f} sats to Magi with fee {net_msats_fee / 1000:.3f} sats. Transaction ID: {trx_id}",
        reply_error=None,
    )
    await invoice.save()

    logger.info(
        f"{ICON} Forwarded {amount_to_send_sats:,.0f} sats to Magi for invoice {invoice.short_id} with trx_id: {trx_id}",
        extra={"trx": trx, **vsc_payload.log_extra},
    )
    return


# MARK: OUTBOUND Magisats


async def magisats_outbound(
    magi_transfer: MagiBTCTransferEvent, vsc_call: VSCCall
) -> List[LedgerEntry]:
    """
    Record the accounting entries for a completed MagiSats transfer (forwarding) event.

    This function is called after a Lightning payment has already been received
    into the umbrel node and a corresponding VSC Liability has been created.

    Accounting flow (double-entry):

    1. Earlier (in the deposit handler):
       - Debit:  External Lightning Payments (umbrel)  + full received amount (e.g. 560 sats)
       - Credit: VSC Liability (devser.v4vapp)         + full received amount

    2. Server-to-Exchange leg (this function):
       - Debit:  VSC Liability                         - net amount sent to customer (e.g. 500 sats)
       - Credit: Exchange Holdings (MagiSwap)          - net amount sent to customer
         → This removes the forwarded funds from our assets while clearing the corresponding
           portion of the customer/app liability. The funds have now left the system via Magi.

    3. Fee retention leg (this function):
       - Debit:  VSC Liability                         - net fee (e.g. 60 sats)
       - Credit: Fee Income Magisats (MagiSwap)        + net fee
         → The remaining 60 sats stay in our Exchange Holdings asset and are recognized as revenue.

    Net economic effect of the entire flow:
    - Assets increase by exactly the fee amount (now correctly residing in Exchange Holdings)
    - VSC Liability returns to zero
    - Retained earnings / profit increases by the fee amount

    No "External Magi Payments" entry is needed here — the single server_to_exchange
    credit to Exchange Holdings is sufficient and clean. The previous WITHDRAW_LIGHTNING
    entry was creating phantom asset balances and reversed signs.

    All amounts are posted in MSATS using the conversion rates from the Magi transfer event.
    """
    # Now we transfer the amount_to_send_sats to the
    server_id = InternalConfig().server_id
    vsc_payload = VSCCallPayload.model_validate(vsc_call.payload)
    assert vsc_payload.amount, "Amount is missing in VSC payload"
    assert magi_transfer.amount == Decimal(vsc_payload.amount), (
        "Amount in VSC payload does not match Magi transfer event amount"
    )
    assert vsc_payload.msats_fee is not None, "MSATS fee is missing in VSC payload"

    net_fee_original_msats = Decimal(vsc_payload.msats_fee)
    amount_sent_msats = Decimal(magi_transfer.amount) * Decimal(1000)
    ledger_entries_list = []

    assert vsc_payload.parent_id, "Parent ID is missing in VSC payload for Magi transfer event"
    original_invoice = await load_tracked_object(vsc_payload.parent_id)
    if not isinstance(original_invoice, Invoice):
        logger.error(
            f"{ICON} Original invoice with group_id_p {vsc_payload.parent_id} not found or invalid for Magi transfer {magi_transfer.short_id}"
        )
        return []

    amount_received_msats = Decimal(original_invoice.value_msat)
    net_fee_msats = amount_received_msats - amount_sent_msats
    # now send the fee to the fee account
    quote = await MagiBTCTransferEvent.nearest_quote(timestamp=magi_transfer.timestamp)
    fee_conv = CryptoConversion(
        quote=quote,
        conv_from=Currency.MSATS,
        value=net_fee_msats,
    ).conversion

    assert net_fee_msats >= 0, (
        "Net fee cannot be negative. Check the amounts in the invoice and VSC payload."
    )
    assert net_fee_msats >= net_fee_original_msats, (
        "Net fee is less than the original fee. Check the amounts in the invoice and VSC payload."
    )

    try:
        default_exchange_adapter = get_exchange_adapter()
    except Exception as e:
        logger.error(f"Failed to initialize exchange adapter: {e}", extra={"error": str(e)})
        return []

    exchange_sub = default_exchange_adapter.exchange_name

    # ──────────────────────────────────────────────────────────────────────
    # 1. Server Lightning → Magi Exchange (forward the customer portion)
    # ──────────────────────────────────────────────────────────────────────
    ledger_type = LedgerType.MAGI_OUTBOUND
    server_to_exchange = LedgerEntry(
        cust_id=magi_transfer.cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Magi Transfer out {magi_transfer.amount:,.0f} sats for {magi_transfer.cust_id}",
        user_memo=vsc_payload.memo,
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=amount_sent_msats,  # using the rounded version
        debit_conv=magi_transfer.conv,
        credit=AssetAccount(name="Exchange Holdings", sub=exchange_sub),
        credit_unit=Currency.MSATS,
        credit_amount=amount_sent_msats,
        credit_conv=magi_transfer.conv,
        link=magi_transfer.link,
    )
    await server_to_exchange.save()
    ledger_entries_list.append(server_to_exchange)

    # ──────────────────────────────────────────────────────────────────────
    # 2. Fee income (the retained portion)
    # ──────────────────────────────────────────────────────────────────────
    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        cust_id=magi_transfer.cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Magisats {net_fee_msats / 1000:.3f} sats",
        user_memo=vsc_payload.memo,
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=net_fee_msats,
        debit_conv=fee_conv,
        credit=RevenueAccount(name="Fee Income Magisats", sub=exchange_sub),
        credit_unit=Currency.MSATS,
        credit_amount=net_fee_msats,
        credit_conv=fee_conv,
        link=magi_transfer.link,
    )
    await fee_ledger_entry.save()
    ledger_entries_list.append(fee_ledger_entry)

    # Notification to Hive (non-accounting)
    notification = KeepsatsTransfer(
        from_account=server_id,
        to_account=magi_transfer.cust_id,
        msats=0,  # this is a notification ONLY
        memo=vsc_payload.memo,
        invoice_message=vsc_payload.memo,
        notification=True,
        parent_id=vsc_payload.parent_id,
    )

    hive_client, _ = await get_verified_hive_client()
    trx = await send_custom_json(
        json_data=notification.model_dump(exclude_none=True, exclude_unset=True),
        send_account=server_id,
        active=True,
        id=InternalConfig().config.hive_config.custom_json_prefix + "_notification",
        hive_client=hive_client,
    )
    trx_id = trx.get("trx_id", "Failed") if trx else "Failed"
    logger.info(
        f"Notification {notification.log_str} (trx_id: {trx_id})",
        extra={"notification": False, **notification.log_extra, "trx": trx},
    )

    return ledger_entries_list


# MARK: INBOUND Magisats


async def magisats_inbound(
    magi_transfer: MagiBTCTransferEvent, vsc_call: VSCCall
) -> List[LedgerEntry]:
    """
    Record the accounting entries for a completed MagiSats transfer (forwarding) event.

    This function is called after a Lightning payment has already been received
    into the umbrel node and a corresponding VSC Liability has been created.

    Accounting flow (double-entry):

    1. Server-to-Exchange leg (this function):
       - Debit:  VSC Liability                         - net amount sent to customer (e.g. 500 sats)
       - Credit: Exchange Holdings (MagiSwap)          - net amount sent to customer
         → This removes the forwarded funds from our assets while clearing the corresponding
           portion of the customer/app liability. The funds have now left the system via Magi.

    3. Fee retention leg (this function):
       - Debit:  VSC Liability                         - net fee (e.g. 60 sats)
       - Credit: Fee Income Magisats (MagiSwap)        + net fee
         → The remaining 60 sats stay in our Exchange Holdings asset and are recognized as revenue.

    Net economic effect of the entire flow:
    - Assets increase by exactly the fee amount (now correctly residing in Exchange Holdings)
    - VSC Liability returns to zero
    - Retained earnings / profit increases by the fee amount

    No "External Magi Payments" entry is needed here — the single server_to_exchange
    credit to Exchange Holdings is sufficient and clean. The previous WITHDRAW_LIGHTNING
    entry was creating phantom asset balances and reversed signs.

    All amounts are posted in MSATS using the conversion rates from the Magi transfer event.
    """
    # Now we transfer the amount_to_send_sats to the
    vsc_payload = VSCCallPayload.model_validate(vsc_call.payload)
    assert vsc_payload.amount, "Amount is missing in VSC payload"
    assert magi_transfer.amount == Decimal(vsc_payload.amount), (
        "Amount in VSC payload does not match Magi transfer event amount"
    )
    assert magi_transfer.conv and not magi_transfer.conv.is_unset(), (
        "Conversion details are missing in Magi transfer event"
    )

    amount_sent_msats = Decimal(magi_transfer.amount) * Decimal(1000)
    ledger_entries_list = []
    net_fee_msats = magi_transfer.conv.msats_fee

    net_to_customer_msats = amount_sent_msats - net_fee_msats
    if net_to_customer_msats < 0:
        logger.error(
            f"Net amount to customer is negative for Magi transfer {magi_transfer.short_id}. Check the amounts in the invoice and VSC payload."
        )
        return []

    # now send the fee to the fee account
    quote = await MagiBTCTransferEvent.nearest_quote(timestamp=magi_transfer.timestamp)
    fee_conv = CryptoConversion(
        quote=quote,
        conv_from=Currency.MSATS,
        value=net_fee_msats,
    ).conversion

    assert net_fee_msats >= 0, (
        "Net fee cannot be negative. Check the amounts in the invoice and VSC payload."
    )

    try:
        default_exchange_adapter = get_exchange_adapter()
    except Exception as e:
        logger.error(f"{ICON} Failed to initialize exchange adapter: {e}", extra={"error": str(e)})
        return []

    exchange_sub = default_exchange_adapter.exchange_name
    processed_memo = ProcessedMemo(vsc_payload.memo)

    # Takes the cust_id (i.e. the destination) from either the sender of the transaction or the memo if there is one.
    cust_id = processed_memo.cust_id or magi_transfer.cust_id
    if not cust_id:
        logger.error(
            f"{ICON} Customer ID is missing in both processed memo and Magi transfer event for {magi_transfer.short_id}"
        )
        cust_id = "unknown_cust_id"

    # ──────────────────────────────────────────────────────────────────────
    # 1. Magi Exchange account → Customer (forward the customer portion)
    # ──────────────────────────────────────────────────────────────────────
    ledger_type = LedgerType.MAGI_INBOUND
    magi_inbound_ledger = LedgerEntry(
        cust_id=cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Receive Magisats to Keepsats {magi_transfer.amount:,.0f} sats for {cust_id}",
        user_memo=processed_memo.short_memo,
        debit=AssetAccount(name="Exchange Holdings", sub=exchange_sub),
        debit_unit=Currency.MSATS,
        debit_amount=amount_sent_msats,
        debit_conv=magi_transfer.conv,
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,
        ),
        credit_unit=Currency.MSATS,
        credit_amount=amount_sent_msats,  # using the rounded version
        credit_conv=magi_transfer.conv,
        link=magi_transfer.link,
    )
    await magi_inbound_ledger.save()
    ledger_entries_list.append(magi_inbound_ledger)

    # ──────────────────────────────────────────────────────────────────────
    # 2. Fee income (the retained portion)
    # ──────────────────────────────────────────────────────────────────────
    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Magisats receive {magi_transfer.amount:,.0f} sats {net_fee_msats / 1000:.3f} sats for {cust_id}",
        user_memo=processed_memo.short_memo,
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=net_fee_msats,
        debit_conv=fee_conv,
        credit=RevenueAccount(name="Fee Income Magisats", sub=exchange_sub),
        credit_unit=Currency.MSATS,
        credit_amount=net_fee_msats,
        credit_conv=fee_conv,
        link=magi_transfer.link,
    )
    await fee_ledger_entry.save()
    ledger_entries_list.append(fee_ledger_entry)

    if magi_transfer.pay_with_sats:
        await follow_on_transfer(tracked_op=magi_transfer, nobroadcast=False)
        logger.info(
            f"{ICON} {magi_transfer.short_id} Follow-on transfer initiated "
            f"for Magi inbound transfer {magi_transfer.short_id} to {cust_id}",
        )

    logger.info(
        f"{ICON} {magi_transfer.short_id} Processed Magi inbound transfer for {cust_id} "
        f"with total amount {magi_transfer.amount:,.0f} sats and fee {net_fee_msats / 1000:.3f} sats",
    )

    return ledger_entries_list
