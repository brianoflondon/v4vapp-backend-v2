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
from v4vapp_backend_v2.helpers.general_purpose_funcs import received_lightning_message
from v4vapp_backend_v2.hive.hive_extras import get_verified_hive_client, send_custom_json
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.magi.magi_classes import MagiBTCTransferEvent
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.process.hive_notification import send_magi_transfer_custom_json


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
                        logger.info(
                            f"Found outgoing VSC transfer call from {server_id} in custom JSON {magi_transfer.short_id}",
                            extra={**vsc_call.log_extra},
                        )
                        ledger_entries = await record_magisats_transfer_event(
                            magi_transfer=magi_transfer, vsc_call=vsc_call
                        )
                    if vsc_payload.to == AccName(server_id).magi_prefix:
                        logger.info(
                            f"Found incoming transfer to {server_id} in custom JSON {magi_transfer.short_id}",
                            extra={**vsc_call.log_extra},
                        )
    except AssertionError as e:
        logger.error(
            f"Assertion error while processing Magi BTC transfer event {magi_transfer.short_id}: {e}",
            extra={"error": str(e), **magi_transfer.log_extra},
        )

    except Exception as e:
        logger.error(
            f"Unexpected error while processing Magi BTC transfer event {magi_transfer.short_id}: {e}",
            extra={"error": str(e), **magi_transfer.log_extra},
        )

    return ledger_entries


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

    server_id = InternalConfig().server_id
    vsc_call = VSCCall(
        net_id="vsc-mainnet",
        contract_id="vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
        action="transfer",
        caller=f"hive:{server_id}",
        payload=vsc_payload,
        rc_limit=2000,
        intents=[],
    )

    trx = await send_magi_transfer_custom_json(
        vsc_call=vsc_call,
        nobroadcast=False,
    )
    trx_id = trx.get("trx_id", "Failed") if trx else "Failed"
    logger.info(
        f"Sent MAGI transfer custom JSON for invoice {invoice.short_id}, trx_id: {trx_id}",
        extra={"trx": trx, **vsc_call.log_extra},
    )
    invoice.add_reply(
        reply_id=trx_id,
        reply_type=ReplyType.MAGI_TRANSFER,
        reply_msat=0,
        reply_message=f"Sent {amount_to_send_sats:,.0f} sats to Magi with fee {net_msats_fee / 1000:.3f} sats. Transaction ID: {trx_id}",
        reply_error=None,
    )
    await invoice.save()

    return


async def record_magisats_transfer_event(
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
            f"Original invoice with group_id_p {vsc_payload.parent_id} not found or invalid for Magi transfer {magi_transfer.short_id}"
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
    ledger_type = LedgerType.SERVER_TO_EXCHANGE
    server_to_exchange = LedgerEntry(
        cust_id=magi_transfer.cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Server Lightning to Magi Exchange {magi_transfer.amount:,.0f} sats for {magi_transfer.cust_id}",
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
