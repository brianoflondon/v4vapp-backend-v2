from typing import List, Tuple

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.process.process_errors import KeepsatsDepositNotificationError
from v4vapp_backend_v2.actions.cust_id_class import CustID
from v4vapp_backend_v2.process.hive_notification import depreciated_send_notification_hive_transfer
from v4vapp_backend_v2.actions.lnd_to_hive import process_lightning_to_hive
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.invoice_models import Invoice, InvoiceState


async def process_lightning_to_hive_or_keepsats(
    invoice: Invoice, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Process a Lightning invoice to Hive transfer.
    This is where we decide which currency to receive, Keepsats, Hive or HBD

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
        # MARK: Sats in to Keepsats
        if invoice.recv_currency == Currency.SATS:
            logger.info(f"Processing Lightning to Keepsats for customer ID: {invoice.cust_id}")
            # This line here bypasses the Hive Return logic.
            ledger_entries, message, return_amount = await lightning_to_keepsats_deposit(
                invoice, nobroadcast
            )
        # MARK: Sats to Hive or HBD
        elif invoice.recv_currency in {Currency.HIVE, Currency.HBD}:
            logger.info(
                f"Processing Lightning to Hive conversion for customer ID: {invoice.cust_id}"
            )
            ledger_entries, message, return_amount = await process_lightning_to_hive(
                invoice, nobroadcast
            )
        else:
            logger.error(
                f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}",
                extra={"notification": False, **invoice.log_extra},
            )
            raise NotImplementedError(
                f"Unsupported currency for Lightning to Hive transfer: {invoice.recv_currency}"
            )

        if invoice.value_msat <= V4VConfig().data.minimum_invoice_payment_sats * 1_000:
            logger.info(f"Invoice {invoice.short_id} is below the minimum notification threshold.")
            return ledger_entries

        if return_amount:
            return_details = HiveReturnDetails(
                tracked_op=invoice,
                original_memo=invoice.memo,
                reason_str=message,
                action=ReturnAction.REFUND,
                amount=return_amount,
                pay_to_cust_id=invoice.cust_id,
                nobroadcast=nobroadcast,
            )
            logger.info(f"Return amount to customer: {return_amount}")
            try:
                await depreciated_send_notification_hive_transfer(
                    tracked_op=invoice,
                    reason=message,
                    nobroadcast=nobroadcast,
                    amount=return_amount,
                    pay_to_cust_id=invoice.cust_id,
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


# MARK: LND to Keepsats deposit
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
        user_memo=invoice.memo if invoice.memo else "",
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

    message = f"Deposit of {msats_received / 1000:,.0f} sats - {invoice.memo}"

    return (
        ledger_entries_list,
        message,
        return_amount,
    )


# MARK: LND to Hive deposit
async def lightning_to_hive_convert(
    invoice: Invoice, nobroadcast: bool = False
) -> Tuple[List[LedgerEntry], str, Amount]:
    """
    Convert a Lightning deposit to Hive.

    Args:
        invoice (Invoice): The Lightning invoice to be processed.
        nobroadcast (bool): If True, the transfer will not be broadcasted.

    Returns:
        Tuple[list[LedgerEntry], str, Amount]:
            - list[LedgerEntry]: The ledger entries for the transfer operation.
            - str: The message to be sent back to the customer as change.
            - Amount: The amount to be returned to the customer after fees (Hive or HBD).
    """
    try:
        return await process_lightning_to_hive(invoice, nobroadcast)
    except Exception as e:
        logger.error(f"Error processing Lightning to Hive conversion: {e}")
        raise NotImplementedError("Lightning to Hive conversion deposit is not implemented yet.")


# Last line of the file
