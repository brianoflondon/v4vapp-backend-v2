from typing import List, Tuple

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import KeepsatsDepositNotificationError
from v4vapp_backend_v2.actions.cust_id_class import CustID
from v4vapp_backend_v2.actions.hive_notification import send_notification_hive_transfer
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.models.invoice_models import Invoice, InvoiceState


async def process_lightning_to_hive_or_keepsats(
    invoice: Invoice, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Process a Lightning invoice to Hive transfer.

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, the transfer will not be broadcasted.

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the transfer operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    if invoice.cust_id and invoice.is_lndtohive and invoice.state == InvoiceState.SETTLED:
        # For now we will treat any inbound amount as Keepsats.
        logger.info(f"Processing Lightning to Hive transfer for customer ID: {invoice.cust_id}")
        ledger_entries, message, return_amount = await lightning_to_keepsats_deposit(
            invoice, nobroadcast
        )
        if invoice.value_msat <= V4VConfig().data.minimum_invoice_payment_sats * 1_000:
            logger.info(f"Invoice {invoice.short_id} is below the minimum notification threshold.")
            return ledger_entries
        if return_amount:
            logger.info(f"Return amount to customer: {return_amount}")
            try:
                await send_notification_hive_transfer(
                    tracked_op=invoice,
                    reason=message,
                    nobroadcast=nobroadcast,
                )
            except KeepsatsDepositNotificationError as e:
                logger.warning(
                    f"Failed to send notification for invoice, skipped {invoice.short_id}: {e}",
                    extra={"notification": False, **invoice.log_extra},
                )
            except Exception as e:
                logger.exception(
                    f"Error returning Hive transfer: {e}",
                    extra={
                        "notification": False,
                        "reason": message,
                        **invoice.log_extra,
                    },
                )

        return ledger_entries

    raise NotImplementedError("Processing Lightning to Hive transfer is not implemented yet.")


async def lightning_to_keepsats_deposit(
    invoice: Invoice, nobroadcast: bool = False
) -> Tuple[list[LedgerEntry], str, Amount]:
    """
    Receive a Keepsats transfer from Lightning to Hive.

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, the transfer will not be broadcasted.

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the transfer operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    ledger_entries_list: list[LedgerEntry] = []
    return_amount = Amount("0.001 HIVE")  # Default return amount

    if not invoice.cust_id:
        logger.error(
            "Invoice does not have a customer ID.",
            extra={"notification": False, **invoice.log_extra},
        )
        cust_id = CustID("keepsats")
    else:
        cust_id = CustID(invoice.cust_id)

    msats_received = invoice.value_msat
    if invoice.conv is None or invoice.conv.is_unset():
        await invoice.update_conv()
    ledger_type = LedgerType.DEPOSIT_KEEPSATS
    deposit_ledger_entry = LedgerEntry(
        short_id=invoice.short_id,
        op_type=invoice.op_type,
        cust_id=cust_id,
        ledger_type=ledger_type,
        group_id=f"{invoice.group_id}-{ledger_type.value}",
        timestamp=invoice.timestamp,
        description=f"Deposit Keepsats {msats_received / 1000:,.0f} sats for {invoice.cust_id}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub="keepsats",
        ),
        debit_unit=Currency.MSATS,
        debit_amount=msats_received,
        debit_conv=invoice.conv,
        credit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,
        ),
        credit_unit=Currency.MSATS,
        credit_amount=msats_received,
        credit_conv=invoice.conv,
    )
    ledger_entries_list.append(deposit_ledger_entry)
    await deposit_ledger_entry.save()
    return (
        ledger_entries_list,
        f"Deposit of {msats_received / 1000:,.0f} sats successful",
        return_amount,
    )


# # TODO: this must be adapted for Lightning Keepsats replies
# async def send_notification_hive_transfer(
#     invoice: Invoice,
#     reason: str,
#     amount: Amount | None = None,
#     nobroadcast: bool = False,
# ) -> Dict[str, str]:
#     """
#     Send a notification and process a Hive transfer repayment for a given invoice.

#     This function handles the process of sending a Hive transfer (typically a repayment or change)
#     to the original sender of an invoice, logs relevant information, updates the invoice with
#     the transaction details, and manages error handling and notifications.

#     Args:
#         invoice (Invoice): The invoice object representing the original Hive to Lightning operation.
#         reason (str): The reason for the repayment or change transaction.
#         amount (Amount | None, optional): The amount to transfer. Defaults to 0.001 HIVE if not provided.
#         nobroadcast (bool, optional): If True, do not broadcast the transaction. Defaults to False.

#     Returns:
#         Dict[str, str]: The transaction details as returned by the Hive client.

#     Raises:
#         KeepsatsDepositNotificationError: If the repayment fails or an unexpected error occurs.
#     """

#     if not invoice.cust_id:
#         logger.error(
#             "Invoice does not have a customer ID.",
#             extra={"notification": False, **invoice.log_extra},
#         )
#         raise KeepsatsDepositNotificationError("Invoice does not have a customer ID.")

#     if not CustID(invoice.cust_id).is_hive:
#         logger.error(
#             "Invoice customer ID is not a valid Hive account.",
#             extra={"notification": False, **invoice.log_extra},
#         )
#         raise KeepsatsDepositNotificationError("Invoice customer ID is not a valid Hive account.")

#     logger.info(
#         f"Processing return/change for: {invoice.log_str}",
#         extra={"notification": False, **invoice.log_extra},
#     )
#     logger.info(
#         f"Reason: {reason} amount: {amount}",
#         extra={"reason": reason, "amount": amount, "nobroadcast": nobroadcast},
#     )
#     hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)

#     # We don't check the operation was already paid here because that is done in the processing function
#     amount = Amount("0.001 HIVE") if amount is None else amount
#     try:
#         memo = f"{reason} | § {invoice.short_id}{MEMO_FOOTER}"
#         trx = await send_transfer(
#             hive_client=hive_client,
#             from_account=server_account_name,
#             to_account=invoice.cust_id,  # Repay to the original sender
#             amount=amount,
#             memo=memo,
#         )
#         if trx:
#             # MARK: 5. Update hive_transfer
#             logger.info(
#                 f"Successfully paid reply to Hive to Lightning operation: {invoice.log_str}",
#                 extra={
#                     "notification": True,
#                     "trx": trx,
#                     **invoice.log_extra,
#                 },
#             )
#             try:
#                 return_amount = Amount(trx["operations"][0][1]["amount"])
#             except (KeyError, IndexError):
#                 return_amount = Amount("0.001 HIVE")
#             if not return_amount:
#                 return_amount = Amount("0.001 HIVE")
#             await TransferBase.update_quote()
#             invoice.change_conv = CryptoConversion(
#                 conv_from=return_amount.symbol,
#                 amount=return_amount,
#                 quote=TransferBase.last_quote,
#             ).conversion
#             return_amount_msat = invoice.change_conv.msats
#             # Now add the Hive reply to the original Hive transfer operation
#             # MARK: 5. Update hive_transfer
#             # TODO: Move this note of the reply id to the processing of the reply. complete_hive_to_lightning
#             reason = (
#                 f"Change transaction for operation {invoice.group_id}: {trx.get('trx_id', '')}"
#             )
#             invoice.add_reply(
#                 reply_id=trx.get("trx_id", ""),
#                 reply_type="transfer",
#                 reply_msat=return_amount_msat,
#                 reply_error=None,
#                 reply_message=reason,
#             )
#             await invoice.save()
#             logger.info(
#                 f"Updated Hive transfer with reply: {invoice.replies[-1]}",
#                 extra={"notification": False, **invoice.log_extra},
#             )
#             return trx
#         else:
#             raise HiveTransferError("No transaction created during Hive to Lightning repayment")
#     except HiveTransferError as e:
#         message = f"Failed to repay Hive to Lightning operation: {e}"
#         invoice.add_reply(
#             reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
#         )
#         await invoice.save()
#         logger.error(
#             message,
#             extra={"notification": False, **invoice.log_extra},
#         )
#         raise HiveToLightningError(message)

#     except Exception as e:
#         message = f"Unexpected error during Hive to Lightning repayment: {e}"
#         invoice.add_reply(
#             reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
#         )
#         await invoice.save()
#         logger.error(
#             message,
#             extra={"notification": False, **invoice.log_extra},
#         )
#         raise HiveToLightningError(message)
