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
    Route a Magi BTC transfer event to the correct accounting handler.

    Ensures conversion rates are populated, then iterates over the watched custom JSON
    operations attached to the event. For each VSC "transfer" action:

    - If the server is the **caller** (outbound), dispatches to `magisats_outbound`.
      Self-transfers (caller == recipient) are skipped with a warning.
    - If the server's Magi address is the **recipient** (inbound), dispatches to
      `magisats_inbound`.

    Args:
        magi_transfer: The on-chain Magi BTC transfer event to process.

    Returns:
        List of `LedgerEntry` objects created by the handler, or an empty list if
        no matching transfer was found or an error occurred.
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
    Forward sats to a customer's Magi address after a Lightning invoice is paid.

    Called when a Lightning invoice tagged `#magisats` is settled. Computes the net
    amount to forward after deducting the fee (from `fixed_quote` if present, otherwise
    from the invoice's conversion), constructs a `VSCCallPayload`, and broadcasts a
    Magi VSC transfer transaction on-chain via `send_magi_transaction`.

    The fee is embedded in the payload as `msats_fee` and carried forward to
    `magisats_outbound` (via `process_magi_btc_transfer_event`) for accounting once
    the on-chain VSC transfer is observed.

    Args:
        invoice: The settled Lightning invoice whose `cust_id` identifies the
                 destination Magi account.

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
    Record accounting entries when the server sends sats outbound via a Magi VSC transfer.

    Triggered when `process_magi_btc_transfer_event` detects an on-chain VSC "transfer"
    whose **caller** is the server (i.e. the server initiated the transfer, typically
    via `forward_magisats`). The original Lightning invoice is loaded via `parent_id`
    to derive the true fee (received msats − forwarded msats).

    Pre-condition (recorded earlier in the deposit handler):
      - Debit:  External Lightning Payments (umbrel)   full received amount (e.g. 560 sats)
      - Credit: VSC Liability (server_id)              full received amount

    Accounting entries created here (double-entry, all amounts in MSATS):

    1. MAGI_OUTBOUND — forward the customer portion:
       - Debit:  VSC Liability (server_id)             amount_sent_msats  (e.g. 500,000 msats)
       - Credit: Exchange Holdings (exchange_name)     amount_sent_msats
         → Clears the forwarded portion of the liability; funds have left the system via Magi.

    2. FEE_INCOME — retain the fee:
       - Debit:  VSC Liability (server_id)             net_fee_msats      (e.g. 60,000 msats)
       - Credit: Fee Income Magisats (exchange_name)   net_fee_msats
         → net_fee = amount_received − amount_sent (must be >= the fee quoted in the payload).

    Net effect:
      - VSC Liability returns to zero.
      - Exchange Holdings increases by `amount_sent_msats` (representing sats now in Magi).
      - Revenue increases by `net_fee_msats`.

    A non-accounting Hive `custom_json` notification is also broadcast to the customer.

    Args:
        magi_transfer: The on-chain Magi BTC transfer event (server as sender).
        vsc_call:      The parsed VSC call from the watched custom JSON.

    Returns:
        List of the two `LedgerEntry` objects saved, or an empty list on error.
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
    Record accounting entries when sats arrive at the server's Magi address from a customer.

    Triggered when `process_magi_btc_transfer_event` detects an on-chain VSC "transfer"
    whose **recipient** (`payload.to`) is the server's Magi-prefixed address. The fee is
    taken from the event's conversion details (`magi_transfer.conv.msats_fee`).
    The customer identity is resolved from the memo (if present) or falls back to
    `magi_transfer.cust_id`.

    Accounting entries created here (double-entry, all amounts in MSATS):

    1. MAGI_INBOUND — receive the full inbound amount:
       - Debit:  Exchange Holdings (exchange_name)     amount_sent_msats  (full received amount)
       - Credit: VSC Liability (cust_id)               amount_sent_msats
         → Records the sats arriving from the Magi exchange into our holdings, creating
           a corresponding liability to the customer.

    2. FEE_INCOME — retain the fee:
       - Debit:  VSC Liability (cust_id)               net_fee_msats
       - Credit: Fee Income Magisats (exchange_name)   net_fee_msats
         → Reduces the customer liability by the fee amount and recognises it as revenue.

    Net effect:
      - Exchange Holdings increases by `amount_sent_msats`.
      - VSC Liability to the customer is `net_to_customer_msats` (= received − fee).
      - Revenue increases by `net_fee_msats`.

    If `magi_transfer.pay_with_sats` is set, a follow-on Lightning transfer to the
    customer is also initiated via `follow_on_transfer`.

    Args:
        magi_transfer: The on-chain Magi BTC transfer event (server as recipient).
        vsc_call:      The parsed VSC call from the watched custom JSON.

    Returns:
        List of the two `LedgerEntry` objects saved, or an empty list on error.
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
