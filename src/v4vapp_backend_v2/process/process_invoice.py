from datetime import datetime, timezone
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.keepsats_to_hive import conversion_keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.models.invoice_models import Invoice, InvoiceState
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json


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
        group_id=f"{invoice.group_id}-{ledger_type.value}",
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

        #     logger.info(f"Processing Lightning to Keepsats for customer ID: {invoice.cust_id}")
        #     # This line here bypasses the Hive Return logic.

        #     ledger_entries, message, return_amount = await lightning_to_keepsats_deposit(
        #         invoice, nobroadcast
        #     )
        # # MARK: Sats to Hive or HBD
        # elif invoice.recv_currency in {Currency.HIVE, Currency.HBD}:
        #     logger.info(
        #         f"Processing Lightning to Hive conversion for customer ID: {invoice.cust_id}"
        #     )
        #     ledger_entries, message, return_amount = await process_lightning_to_hive(
        #         invoice, nobroadcast
        #     )
        # else:
        #     logger.error(
        #         f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}",
        #         extra={"notification": False, **invoice.log_extra},
        #     )
        #     raise NotImplementedError(
        #         f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}"
        #     )

        # if invoice.value_msat <= V4VConfig().data.minimum_invoice_payment_sats * 1_000:
        #     logger.info(f"Invoice {invoice.short_id} is below the minimum notification threshold.")
        #     return ledger_entries

        # if return_amount:
        #     return_details = HiveReturnDetails(
        #         tracked_op=invoice,
        #         original_memo=invoice.memo,
        #         reason_str=message,
        #         action=ReturnAction.REFUND,
        #         amount=return_amount,
        #         pay_to_cust_id=invoice.cust_id,
        #         nobroadcast=nobroadcast,
        #     )
        #     logger.info(f"Return amount to customer: {return_amount}")
        #     try:
        #         await depreciated_send_notification_hive_transfer(
        #             tracked_op=invoice,
        #             reason=message,
        #             nobroadcast=nobroadcast,
        #             amount=return_amount,
        #             pay_to_cust_id=invoice.cust_id,
        #         )
        #     except KeepsatsDepositNotificationError as e:
        #         logger.warning(
        #             f"Failed to send notification for invoice, skipped {invoice.short_id}: {e}",
        #             extra={"notification": False, **invoice.log_extra},
        #         )
        #     except Exception as e:
        #         logger.exception(
        #             f"Error returning Hive transfer: {e}",
        #             extra={
        #                 "notification": False,
        #                 "reason": message,
        #                 **invoice.log_extra,
        #             },
        #         )

        # return ledger_entries

    logger.warning(
        f"Invoice {invoice.short_id} received but no further actions. {invoice.log_str}",
        extra={"notification": False, **invoice.log_extra},
    )
    return ledger_entries_list


async def process_lightning_receipt_stage_2(invoice: Invoice, nobroadcast: bool = False) -> None:
    """
    Process the second part of a Lightning invoice which is inbound.
    This function is called after the initial deposit has been made.

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, Hive response won't be broadcast (testing mostly)

    Returns:
        List[LedgerEntry]: The ledger entries for the transfer operation.
    """

    # MARK: Sats to Hive or HBD
    server_id = InternalConfig().server_id
    if (
        invoice.cust_id
        and invoice.is_lndtohive
        and invoice.recv_currency in {Currency.HIVE, Currency.HBD}
    ):
        # For now we will treat any inbound amount as Keepsats.
        # Not sure this is right... need to think about the custom_json use in the LND to Hive loop.
        logger.info(f"Processing Lightning to Hive conversion for customer ID: {invoice.cust_id}")
        # This will send Hive or a custom_json at the end.
        await conversion_keepsats_to_hive(
            server_id=server_id,
            cust_id=invoice.cust_id,
            tracked_op=invoice,
            to_currency=invoice.recv_currency,
            nobroadcast=nobroadcast,
        )
        return

    else:
        logger.error(
            f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}",
            extra={"notification": False, **invoice.log_extra},
        )
        raise NotImplementedError(
            f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}"
        )
