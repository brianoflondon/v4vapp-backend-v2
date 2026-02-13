from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.keepsats_to_hive import conversion_keepsats_to_hive
from v4vapp_backend_v2.helpers.bad_actors_list import check_bad_hive_accounts
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import received_lightning_message
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.invoice_models import Invoice, InvoiceState
from v4vapp_backend_v2.process.hive_notification import reply_with_hive, send_transfer_custom_json


async def process_lightning_receipt(
    invoice: Invoice, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Process a Lightning invoice which is inbound.
    All inbound invoices will be first deposited as Keepsats.
    After they have been deposited, the funds will be moved around according to the original
    invoice.memo within the `process.process_hive.process_custom_json` function

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, Hive response won't be broadcast (testing mostly)

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the transfer operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    if invoice.state != InvoiceState.SETTLED:
        logger.warning(f"Invoice {invoice.short_id} is not settled.")
        return []

    server_id = InternalConfig().server_id
    node_name = InternalConfig().node_name

    fixed_quote = invoice.fixed_quote
    if fixed_quote:
        quote = fixed_quote.quote_response
    else:
        quote = await Invoice.nearest_quote(timestamp=invoice.timestamp)

    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv(quote=quote)

    ledger_entries_list = []
    # MARK: 1 Deposit Lightning
    ledger_type = LedgerType.DEPOSIT_LIGHTNING
    incoming_ledger_entry = LedgerEntry(
        cust_id=server_id,
        short_id=invoice.short_id,
        ledger_type=ledger_type,
        group_id=f"{invoice.group_id}_{ledger_type.value}",
        op_type=invoice.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Receive incoming Lightning {invoice.value_msat / 1000:,.0f} sats {invoice.memo}",
        credit=LiabilityAccount(
            name="VSC Liability",
            sub=server_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=invoice.value_msat,
        debit_conv=invoice.conv,
        debit=AssetAccount(name="External Lightning Payments", sub=node_name, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=invoice.value_msat,
        credit_conv=invoice.conv,
    )
    await incoming_ledger_entry.save()
    ledger_entries_list.append(incoming_ledger_entry)

    # Now we send it to the customer (if there is one) and the custom_json receiver needs to process.

    if invoice.cust_id and invoice.is_lndtohive:
        # check if cust_id is on the bad accounts list and if so, send to the v4vapp.sus account instead of the customer
        try:
            bad_account = await check_bad_hive_accounts([invoice.cust_id])
            if bad_account:
                raise ValueError(f"Customer ID {invoice.cust_id} is on the bad accounts list.")
        except Exception:
            incoming_ledger_entry.user_memo += (
                f"Suspicious account transaction: {invoice.cust_id} is on the bad accounts list"
            )
            incoming_ledger_entry.description += (
                f" | Suspicious account transaction: {invoice.cust_id} is on the bad accounts list"
            )
            await incoming_ledger_entry.save(upsert=True)
            logger.warning(
                f"Customer ID {invoice.cust_id} is on the bad accounts taking no further action {invoice.log_str}",
                extra={"notification": True, **invoice.log_extra},
            )
            invoice.cust_id = "v4vapp.sus"
            invoice.memo = f"v4vapp.sus account transaction: {invoice.cust_id} is on the bad accounts list #sats | {invoice.memo}"
            await invoice.save()

        # For now we will treat any inbound amount as Keepsats.
        # MARK: Sats in to Keepsats
        transfer = KeepsatsTransfer(
            from_account=server_id,
            to_account=invoice.cust_id,
            msats=invoice.value_msat,
            memo=invoice.memo,
            parent_id=invoice.group_id,  # This is the group_id of the original transfer
        )
        trx = await send_transfer_custom_json(transfer=transfer, nobroadcast=nobroadcast)
        logger.info(f"Sent custom_json: {trx['trx_id']}", extra={"trx": trx, **transfer.log_extra})

        return ledger_entries_list

    logger.warning(
        f"Invoice {invoice.short_id} received but no further actions. {invoice.log_str}",
        extra={"notification": False, **invoice.log_extra},
    )
    return ledger_entries_list


async def process_lightning_receipt_stage_2(invoice: Invoice, nobroadcast: bool = False) -> None:
    """
    Process the second part of a Lightning invoice which is inbound.
    This function is called after the initial deposit has been made by custom_json transfer
    from the server_id to the cust_id within the VSC Liability accounts.

    This is called from the `process_custom_json` function whenever the server is the sender.

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, Hive response won't be broadcast (testing mostly)

    Returns:
        List[LedgerEntry]: The ledger entries for the transfer operation.
    """

    # MARK: Sats to Hive or HBD
    server_id = InternalConfig().server_id
    if invoice.cust_id and invoice.is_lndtohive:
        if invoice.recv_currency in {Currency.HIVE, Currency.HBD}:
            # For now we will treat any inbound amount as Keepsats.
            # Not sure this is right... need to think about the custom_json use in the LND to Hive loop.
            logger.info(
                f"Processing Lightning to Hive conversion for customer ID: {invoice.cust_id}"
            )
            # This will send Hive or a custom_json at the end.
            # Check for fixed quote in the conversion to Hive/HBD
            await conversion_keepsats_to_hive(
                server_id=server_id,
                cust_id=invoice.cust_id,
                tracked_op=invoice,
                to_currency=invoice.recv_currency,
                nobroadcast=nobroadcast,
            )
            return
        elif invoice.recv_currency in {Currency.SATS, Currency.MSATS}:
            logger.info(
                f"Lightning to Keepsats deposit transfer for customer ID: {invoice.cust_id}",
                extra={"notification": False},
            )
            if invoice.cust_id == "v4vapp.sus":
                logger.info(
                    f"Received Lightning invoice from v4vapp.sus account, no further action will be taken. {invoice.log_str}",
                    extra={"notification": False, **invoice.log_extra},
                )
                return
            details = HiveReturnDetails(
                tracked_op=invoice,
                original_memo=invoice.memo,
                reason_str=received_lightning_message(invoice.memo, Decimal(invoice.value)),
                action=ReturnAction.CHANGE,
                pay_to_cust_id=invoice.cust_id,
                nobroadcast=nobroadcast,
            )
            await reply_with_hive(details=details, nobroadcast=nobroadcast)
            return

    # MARK: No further action (not a conversion)
    else:
        logger.error(
            f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}",
            extra={"notification": False, **invoice.log_extra},
        )
        raise NotImplementedError(
            f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}"
        )
