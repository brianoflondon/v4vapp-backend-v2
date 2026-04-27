from datetime import datetime, timezone
from decimal import Decimal
from pprint import pprint
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.account_name_type import AccName
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

    ledger_entries = []

    for custom_json in magi_transfer.custom_jsons or []:
        if custom_json.is_watched:
            json_data = custom_json.json_data
            if isinstance(json_data, VSCCall):
                ledger_entries = await record_magisats_transfer_event(
                    magi_transfer=magi_transfer, vsc_call=json_data
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

    vsc_payload = VSCCallPayload(
        amount=str(amount_to_send_sats),
        to=magi_to,
        parent_id=invoice.group_id_p,
        msats_fee=str(net_msats_fee),
        memo=f"Forwarding #magisats from invoice {invoice.short_id} with fee: {net_msats_fee / 1000:.3f} sats",
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

    pprint(vsc_call.model_dump())

    trx = await send_magi_transfer_custom_json(
        vsc_call=vsc_call,
        nobroadcast=False,
    )
    trx_id = trx.get("id") if trx else None
    logger.info(f"Sent MAGI transfer custom JSON for invoice {invoice.short_id}, trx_id: {trx_id}")

    return

    invoice.cust_id


async def record_magisats_transfer_event(
    magi_transfer: MagiBTCTransferEvent, vsc_call: VSCCall
) -> List[LedgerEntry]:

    # Now we transfer the amount_to_send_sats to the
    server_id = InternalConfig().server_id
    node_name = InternalConfig().node_name
    vsc_payload = vsc_call.payload
    assert vsc_payload.amount, "Amount is missing in VSC payload"
    assert magi_transfer.amount == Decimal(vsc_payload.amount), (
        "Amount in VSC payload does not match Magi transfer event amount"
    )
    assert vsc_payload.msats_fee is not None, "MSATS fee is missing in VSC payload"

    net_msats_fee = Decimal(vsc_payload.msats_fee)
    amount_to_send_msats = Decimal(magi_transfer.amount) * Decimal(1000)
    ledger_entries_list = []

    ledger_type = LedgerType.WITHDRAW_LIGHTNING
    incoming_ledger_entry = LedgerEntry(
        cust_id=magi_transfer.cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Send Lightning to Magi {magi_transfer.amount:,.0f} sats",
        user_memo=vsc_payload.memo,
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=amount_to_send_msats,  # using the rounded version
        debit_conv=magi_transfer.conv,
        credit=AssetAccount(name="External Magi Payments", sub=node_name, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=amount_to_send_msats,
        credit_conv=magi_transfer.conv,
    )
    await incoming_ledger_entry.save()
    ledger_entries_list.append(incoming_ledger_entry)

    # now send the fee to the fee account
    quote = await MagiBTCTransferEvent.nearest_quote(timestamp=magi_transfer.timestamp)
    fee_conv = CryptoConversion(
        quote=quote,
        conv_from=Currency.MSATS,
        value=net_msats_fee,
    ).conversion

    ledger_type = LedgerType.FEE_INCOME
    fee_ledger_entry = LedgerEntry(
        cust_id=magi_transfer.cust_id,
        short_id=magi_transfer.short_id,
        ledger_type=ledger_type,
        group_id=f"{magi_transfer.group_id}_{ledger_type.value}",
        op_type=magi_transfer.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee for Magisats {net_msats_fee / 1000:.3f} sats",
        user_memo=vsc_payload.memo,
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=net_msats_fee,
        debit_conv=fee_conv,
        credit=RevenueAccount(name="Fee Income Magisats", sub=node_name, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=net_msats_fee,
        credit_conv=fee_conv,
    )
    await fee_ledger_entry.save()
    ledger_entries_list.append(fee_ledger_entry)

    return ledger_entries_list
