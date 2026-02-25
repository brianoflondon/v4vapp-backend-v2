import asyncio
from timeit import default_timer as timer
from typing import List
from uuid import uuid4

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
    LedgerEntryException,
)
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.sanity_checks import run_all_sanity_checks
from v4vapp_backend_v2.actions.tracked_any import TrackedAny, load_tracked_object
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import from_snake_case
from v4vapp_backend_v2.hive.hive_extras import HiveNotEnoughHiveInAccount
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_account_update2 import AccountUpdate2
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_cancelled import LimitOrderCancelled
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_producer_missed import ProducerMissed
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment
from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent
from v4vapp_backend_v2.process.hive_notification import reply_with_hive
from v4vapp_backend_v2.process.lock_str_class import CustIDLockException, LockStr
from v4vapp_backend_v2.process.process_errors import CustomJsonRetryError
from v4vapp_backend_v2.process.process_forward_events import process_forward
from v4vapp_backend_v2.process.process_hive import process_hive_op
from v4vapp_backend_v2.process.process_invoice import process_lightning_receipt
from v4vapp_backend_v2.process.process_payment import process_payment_success
from v4vapp_backend_v2.witness_monitor.witness_events import process_witness_event

SUCCESS_ICON = "✅"
FAILURE_ICON = "❌"
ICON = "📊"


async def process_tracked_event(tracked_op: TrackedAny, attempts: int = 0) -> List[LedgerEntry]:
    """
    Processes a tracked operation and creates a ledger entry if applicable.
    This method handles various types of tracked operations, including
    Hive operations (transfers, limit orders, fill orders) and Lightning
    operations (invoices, payments). It ensures that appropriate debit and
    credit accounts are assigned based on the operation type. If a ledger
    entry with the same group_id already exists, the operation is skipped.
    Args:
        tracked_op (TrackedAny): The tracked operation to process, which can be
            an OpAny, Invoice, or Payment.
    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    Raises:
        LedgerEntryCreationException: If the ledger entry cannot be created.
        LedgerEntryException: If there is an error processing the tracked operation.
    """
    finalize = True
    retry_task = None
    async with LockStr(f"pte_{tracked_op.group_id_p}").locked(
        timeout=None, blocking_timeout=None, request_details=tracked_op.log_str
    ):
        existing_entry = await LedgerEntry.load(group_id=tracked_op.group_id_p)
        if existing_entry:
            logger.warning(
                f"Ledger entry for {tracked_op.short_id} already exists.",
                extra={"notification": False},
            )
            return [existing_entry]

        ledger_entries: List[LedgerEntry] = []

        existing_op = await load_tracked_object(tracked_obj=tracked_op.group_id_p)
        if existing_op and existing_op.process_time:
            logger.warning(
                f"Process time already set for {tracked_op.short_id} already processed.",
                extra={"notification": False},
            )
            return ledger_entries

        if isinstance(tracked_op, AccountUpdate2):
            v4vconfig = V4VConfig()
            if v4vconfig.server_accname == tracked_op.account:
                v4vconfig.fetch()
            return ledger_entries

        if isinstance(tracked_op, AccountWitnessVote):
            # Do nothing with Account witness votes for now
            return ledger_entries

        if isinstance(tracked_op, ProducerReward) or isinstance(tracked_op, ProducerMissed):
            # No ledger entry necessary for producer rewards or missed blocks
            asyncio.create_task(process_witness_event(tracked_op=tracked_op))
            return ledger_entries

        if isinstance(tracked_op, BlockMarker):
            # This shouldn't be arrived at.
            logger.warning(
                "BlockMarker is not a valid operation.",
                extra={"notification": False, **tracked_op.log_extra},
            )
            return ledger_entries

        if isinstance(tracked_op, CustomJson) and "notification" in tracked_op.cj_id:
            # CustomJson notification is a special case.
            logger.debug(f"Notification CustomJson: {tracked_op.log_str}")
            return ledger_entries

        unknown_cust_id = f"unknown_cust_id_{uuid4()}"
        cust_id = getattr(tracked_op, "cust_id", str(unknown_cust_id))
        cust_id = str(unknown_cust_id) if not cust_id else cust_id
        logger.debug(f"{'=*=' * 20}")
        logger.debug(
            f"Customer ID {cust_id} {tracked_op.op_type} processing: {tracked_op.log_str}"
        )
        logger.debug(f"{'=*=' * 10} {cust_id} {'=*=' * 10}")
        start = timer()
        try:
            async with LockStr(f"pte_{cust_id}").locked(
                timeout=None, blocking_timeout=None, request_details=tracked_op.log_str
            ):
                if isinstance(
                    tracked_op,
                    (TransferBase, LimitOrderCreate, LimitOrderCancelled, FillOrder, CustomJson),
                ):
                    ledger_entries = await process_hive_op(op=tracked_op)
                elif isinstance(tracked_op, Invoice):
                    ledger_entries = await process_lightning_invoice(invoice=tracked_op)
                elif isinstance(tracked_op, Payment):
                    ledger_entries = await process_lightning_payment(payment=tracked_op)
                elif isinstance(tracked_op, TrackedForwardEvent):
                    # No ledger entry necessary for HTLC events
                    ledger_entries = await process_forward(tracked_forward_event=tracked_op)
                    logger.info(
                        tracked_op.log_str, extra={"notification": False, **tracked_op.log_extra}
                    )
                else:
                    raise ValueError("Invalid tracked object")

                return ledger_entries

        except CustomJsonRetryError as e:
            attempts += 1
            logger.warning(f"{ICON} CustomJson processing retry error: {e}")
            if attempts <= 3:
                retry_task = process_tracked_event(tracked_op=tracked_op, attempts=attempts)

            if retry_task:
                sleep_time = 10 * attempts
                logger.info(
                    f"{ICON} Retrying operation {tracked_op.short_id} after {sleep_time} seconds."
                )
                await LockStr(tracked_op.group_id_p).release_lock(tracked_op.group_id_p)
                await asyncio.sleep(sleep_time)
                asyncio.create_task(retry_task)
                finalize = False
                return ledger_entries
            else:
                logger.error(
                    f"{FAILURE_ICON} CustomJson processing failed after {e.attempts} attempts: {e}"
                )
                return ledger_entries

        except HiveNotEnoughHiveInAccount as e:
            logger.error(
                f"{FAILURE_ICON} Not enough funds for {cust_id}: {e}",
                extra={"notification": True, "error-code": f"not-enough-funds-{cust_id}"},
            )
            return ledger_entries

        except LedgerEntryDuplicateException as e:
            raise LedgerEntryDuplicateException(f"Ledger entry already exists: {e}") from e

        except LedgerEntryException as e:
            logger.exception(f"{FAILURE_ICON} Error processing tracked operation: {e}")
            raise LedgerEntryException(
                f"{FAILURE_ICON} Error processing tracked operation: {e}"
            ) from e

        except CustIDLockException as e:
            logger.error(f"{FAILURE_ICON} Error acquiring lock for {cust_id}: {e}")
            await asyncio.sleep(10)
            raise CustIDLockException(f"Error acquiring lock for {cust_id}: {e}") from e

        finally:
            if finalize:
                sanity_results = await run_all_sanity_checks()
                process_time = timer() - start
                tracked_op.process_time = process_time
                await tracked_op.save()
                logger.debug(f"{ICON} {'+++' * 10} {cust_id} {'+++' * 10}")
                logger.debug(f"{ICON} {tracked_op.log_str}")
                ledger_entries_log_extra = []
                for entry in ledger_entries:
                    ledger_log_extra = entry.log_extra.copy()
                    ledger_entries_log_extra.append(ledger_log_extra)
                    logger.info(
                        f"{ICON} {entry.log_str}",
                        extra={"notification": False, **ledger_log_extra},
                    )
                logger.info(
                    f"{SUCCESS_ICON} {process_time:>7,.2f} s {tracked_op.log_str}",
                    extra={
                        "notification": True,
                        "ledger_items": ledger_entries_log_extra,
                        **sanity_results.log_extra,
                    },
                )
                logger.debug(f"{ICON} {'+++' * 10} {cust_id} {'+++' * 10}")
                # DEBUG section


# MARK: Lightning Transactions

# MARK: Invoice (inbound Lightning)


async def process_lightning_invoice(
    invoice: Invoice, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Processes a Lightning Network invoice and updates the corresponding ledger entry.

    This function handles incoming Lightning invoices, updating the ledger entry with
    the appropriate credit and debit information based on the invoice details. If the
    invoice memo contains specific keywords (e.g., "Funding"), it assigns the correct
    asset and liability accounts. For other cases, it raises a NotImplementedError.

    Special: If the memo contains "Funding", it treats this as an incoming
    Owner's loan Funding to Treasury Lightning, updating the ledger entry accordingly.

    Args:
        invoice (Invoice): The Lightning invoice object containing payment details.
        ledger_entry (LedgerEntry): The ledger entry to be updated based on the invoice.

    Returns:
        LedgerEntry: The updated ledger entry reflecting the processed invoice.

    Raises:
        NotImplementedError: If the invoice memo does not match implemented cases.
    """
    # Invoice means we are receiving sats from external.
    # Invoice is locked by outer process.
    node_name = InternalConfig().node_name
    # MARK: Funding
    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv()
        await invoice.save()
    if "funding" in invoice.memo.lower():
        # Treat this as an incoming Owner's loan Funding to Treasury Lightning
        ledger_type = LedgerType.FUNDING
        ledger_entry = LedgerEntry(
            group_id=invoice.group_id,
            short_id=invoice.short_id,
            description=invoice.memo,
            timestamp=invoice.timestamp,
            op_type=invoice.op_type,
            ledger_type=ledger_type,
            cust_id=invoice.cust_id or node_name,
            debit=AssetAccount(name="Treasury Lightning", sub=node_name),
            debit_unit=Currency.MSATS,
            debit_amount=float(invoice.amt_paid_msat),
            credit=LiabilityAccount(name="Owner Loan Payable", sub=node_name),
            credit_amount=float(invoice.amt_paid_msat),
            credit_unit=Currency.MSATS,
            link=invoice.link,
        )
        await ledger_entry.save()
        return [ledger_entry]
    # MARK: Regular Invoice LND to Hive or Keepsats
    if invoice.is_lndtohive:
        ledger_entries = await process_lightning_receipt(invoice=invoice)
        return ledger_entries
    elif "Exchange" in invoice.memo:
        raise NotImplementedError("Exchange invoice processing is not implemented yet.")

    raise NotImplementedError("process_lightning_op is not implemented yet.")


# MARK: Payment (outbound Lightning)


async def process_lightning_payment(
    payment: Payment, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Processes a Lightning Network payment and updates the corresponding ledger entry.

    This function handles outgoing Lightning payments, updating the ledger entry with
    the appropriate credit and debit information based on the payment details. If the
    payment memo contains specific keywords (e.g., "Funding"), it assigns the correct
    asset and liability accounts. For other cases, it raises a NotImplementedError.

    Payment verification moved to he

    Args:
        payment (Payment): The Lightning payment object containing payment details.

    Returns:
        List[LedgerEntry]: The list of ledger entries reflecting the processed payment.

    Raises:
        NotImplementedError: If the payment memo does not match implemented cases.
    """
    if not payment.conv or payment.conv.is_unset():
        await payment.update_conv()
    v4vapp_group_id = ""
    if payment.succeeded and payment.custom_records:
        v4vapp_group_id = payment.custom_records.v4vapp_group_id or ""
        keysend_message = payment.custom_records.keysend_message or ""
        # existing_ledger_entry = await LedgerEntry.collection().find_one(
        #     filter={"group_id": v4vapp_group_id}
        # )
        initiating_op = await load_tracked_object(tracked_obj=v4vapp_group_id)
        # This is the case for a successful payment
        if initiating_op:
            ledger_entries_list = await process_payment_success(
                payment=payment,
                initiating_op=initiating_op,
                nobroadcast=nobroadcast,
            )
            return ledger_entries_list

    if payment.failed and payment.custom_records:
        v4vapp_group_id = payment.custom_records.v4vapp_group_id or ""
        keysend_message = payment.custom_records.keysend_message or ""
        existing_ledger_entry = await LedgerEntry.collection().find_one(
            filter={"group_id": v4vapp_group_id}
        )
        if existing_ledger_entry:
            old_ledger_entry = LedgerEntry.model_validate(existing_ledger_entry)
            hive_transfer = await load_tracked_object(tracked_obj=old_ledger_entry.group_id)
            # MARK: Hive to Lightning Payment Failed
            if isinstance(hive_transfer, TransferBase):
                # If a payment fails we need to update the hive_transfer if
                if not hive_transfer.replies:
                    # MARK: Record Failed payment and make a refund
                    # No Journal entry necessary because the Hive Refund will automatically create one
                    failure_reason = from_snake_case(payment.failure_reason.lower())
                    return_hive_message = f"Lightning payment failed {failure_reason}"
                    return_details = HiveReturnDetails(
                        tracked_op=hive_transfer,
                        original_memo=hive_transfer.memo,
                        reason_str=return_hive_message,
                        action=ReturnAction.REFUND,
                        pay_to_cust_id=hive_transfer.cust_id,
                        nobroadcast=nobroadcast,
                    )
                    trx = await reply_with_hive(details=return_details, nobroadcast=nobroadcast)
                    return []
                else:
                    logger.warning(
                        f"Hive transfer already has replies, skipping update. {hive_transfer.short_id}",
                        extra={"notification": False, **hive_transfer.log_extra},
                    )
                    return []

    if not v4vapp_group_id:
        raise LedgerEntryCreationException(
            "Payment does not have a valid v4vapp_group_id in custom records."
        )
    raise NotImplementedError(f"Not implemented yet for Payment: {payment.group_id}.")


# Last line of file
