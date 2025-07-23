import asyncio
from timeit import default_timer as timer
from typing import List, Union
from uuid import uuid4

from v4vapp_backend_v2.accounting.balance_sheet import check_balance_sheet_mongodb
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
    LedgerEntryException,
    LedgerType,
)
from v4vapp_backend_v2.actions.cust_id_class import CustID, CustIDLockException
from v4vapp_backend_v2.actions.hive_to_lnd import (
    process_hive_to_lightning,
    return_hive_transfer,
)
from v4vapp_backend_v2.actions.hold_release_keepsats import release_keepsats
from v4vapp_backend_v2.actions.lnd_to_keepsats_hive import process_lightning_to_hive_or_keepsats
from v4vapp_backend_v2.actions.payment_success import (
    hive_to_lightning_payment_success,
    keepsats_to_lightning_payment_success,
)
from v4vapp_backend_v2.actions.tracked_any import TrackedAny, TrackedTransfer, load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import from_snake_case, lightning_memo
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment


async def process_tracked_event(tracked_op: TrackedAny) -> List[LedgerEntry]:
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
    unknown_cust_id = uuid4()
    cust_id = getattr(tracked_op, "cust_id", str(unknown_cust_id))
    cust_id = str(unknown_cust_id) if not cust_id else cust_id
    logger.info(f"Customer ID {cust_id} processing tracked operation: {tracked_op.log_str}")
    start = timer()
    try:
        async with CustID(cust_id).locked(
            timeout=None,
            blocking_timeout=None,
        ):
            if isinstance(tracked_op, (TransferBase, LimitOrderCreate, FillOrder, CustomJson)):
                ledger_entry = await process_hive_op(op=tracked_op)
                ledger_entries = [ledger_entry]
            elif isinstance(tracked_op, Invoice) and tracked_op.settled:
                ledger_entries = await process_lightning_op(op=tracked_op)
            elif isinstance(tracked_op, Payment):
                ledger_entries = await process_lightning_op(op=tracked_op)
            else:
                raise ValueError("Invalid tracked object")

            if not ledger_entries:
                raise LedgerEntryCreationException("Ledger entry cannot be created.")
            for ledger_entry in ledger_entries:
                try:
                    # DEBUG section
                    is_balanced, _ = await check_balance_sheet_mongodb()
                    if not is_balanced:
                        logger.warning(
                            f"The balance sheet is not balanced for\n{ledger_entry.group_id}",
                            extra={"notification": False},
                        )
                except Exception as e:
                    logger.error(
                        f"Error saving ledger entry: {e}",
                        extra={**ledger_entry.log_extra, "notification": False},
                    )

            return ledger_entries
    except LedgerEntryDuplicateException as e:
        raise LedgerEntryDuplicateException(f"Ledger entry already exists: {e}") from e

    except LedgerEntryException as e:
        logger.exception(f"Error processing tracked operation: {e}")
        raise LedgerEntryException(f"Error processing tracked operation: {e}") from e

    except CustIDLockException as e:
        logger.error(f"Error acquiring lock for {cust_id}: {e}")
        await asyncio.sleep(10)
        raise CustIDLockException(f"Error acquiring lock for {cust_id}: {e}") from e

    finally:
        process_time = timer() - start
        tracked_op.process_time = process_time
        await tracked_op.save()
        logger.info(f"{'=' * 50}")
        logger.info(f"{process_time:>7,.2f} s {cust_id} processing tracked operation")
        logger.info(f"{tracked_op.log_str}")
        logger.info(f"{'=' * 50}")
        # if cust_id:
        #     # Ensure the lock is released even if an error occurs
        #     logger.info(f"Releasing lock for {cust_id} after processing tracked operation.")
        #     await CustID.release_lock(cust_id)


async def process_lightning_op(op: Invoice | Payment) -> List[LedgerEntry]:
    """
    Processes the Lightning operation and creates a ledger entry if applicable.

    This method handles various types of Lightning operations, including
    invoices and payments. It ensures that appropriate debit and credit
    accounts are assigned based on the operation type. If a ledger entry
    with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    ledger_entry = LedgerEntry(
        group_id=op.group_id,
        timestamp=op.timestamp,
    )
    if isinstance(op, Invoice):
        ledger_entry.op_type = "invoice"
        ledger_entries = await process_lightning_invoice(invoice=op, ledger_entry=ledger_entry)
    elif isinstance(op, Payment):
        ledger_entry.op_type = "payment"
        ledger_entries = await process_lightning_payment(payment=op)

    return ledger_entries


# MARK: Lightning Transactions

# MARK: Invoice (inbound Lightning)


async def process_lightning_invoice(
    invoice: Invoice, ledger_entry: LedgerEntry
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
    node_name = InternalConfig().config.lnd_config.default
    # MARK: Funding
    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv()
    if "funding" in invoice.memo.lower():
        # Treat this as an incoming Owner's loan Funding to Treasury Lightning
        ledger_entry.cust_id = invoice.cust_id if invoice.cust_id is not None else node_name
        ledger_entry.description = invoice.memo
        ledger_entry.credit_unit = ledger_entry.debit_unit = Currency.MSATS
        ledger_entry.credit_amount = ledger_entry.debit_amount = float(invoice.amt_paid_msat)
        ledger_entry.credit_conv = invoice.conv or CryptoConv()
        ledger_entry.debit_conv = invoice.conv or CryptoConv()
        ledger_entry.debit = AssetAccount(name="Treasury Lightning", sub=node_name)
        ledger_entry.credit = LiabilityAccount(name="Owner Loan Payable (funding)", sub=node_name)
        await ledger_entry.save()
        return [ledger_entry]
    # MARK: Regular Invoice LND to Hive or Keepsats
    if invoice.is_lndtohive:
        ledger_entries = await process_lightning_to_hive_or_keepsats(invoice=invoice)
        return ledger_entries
    elif "Exchange" in invoice.memo:
        print(invoice)

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
    # The Payment will already have been locked by the outer payment processing function.
    if not payment.conv or payment.conv.is_unset():
        await payment.update_conv()
    v4vapp_group_id = ""
    if payment.succeeded and payment.custom_records:
        v4vapp_group_id = payment.custom_records.v4vapp_group_id or ""
        keysend_message = payment.custom_records.keysend_message or ""
        existing_ledger_entry = await LedgerEntry.collection().find_one(
            filter={"group_id": v4vapp_group_id}
        )
        if existing_ledger_entry:
            old_ledger_entry = LedgerEntry.model_validate(existing_ledger_entry)
            initiating_op = await load_tracked_object(tracked_obj=old_ledger_entry.group_id)
            if isinstance(initiating_op, TransferBase):
                if getattr(initiating_op, "paywithsats", None):
                    ledger_entries_list = await keepsats_to_lightning_payment_success(
                        payment=payment,
                        old_ledger_entry=old_ledger_entry,
                        nobroadcast=nobroadcast,
                    )
                    if ledger_entries_list:
                        # We can now safely release the hold on the Keepsats
                        await release_keepsats(hive_transfer=initiating_op)
                else:
                    ledger_entries_list = await hive_to_lightning_payment_success(
                        payment=payment, old_ledger_entry=old_ledger_entry, nobroadcast=nobroadcast
                    )
                return ledger_entries_list
            elif isinstance(initiating_op, CustomJson):
                raise NotImplementedError(
                    f"CustomJson operation not implemented for v4vapp_group_id: {v4vapp_group_id}."
                )

        message = f"Not implemented yet {v4vapp_group_id} {keysend_message}"
        logger.error(message)
        raise NotImplementedError(message)

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
                    return_hive_message = (
                        f"Lightning payment failed {failure_reason} | § {hive_transfer.short_id} |"
                    )
                    try:
                        await return_hive_transfer(
                            hive_transfer=hive_transfer,
                            reason=return_hive_message,
                            nobroadcast=nobroadcast,
                        )
                    except Exception as e:
                        logger.exception(
                            f"Error processing return_hive_transfer: {e}",
                            extra={"notification": False, **hive_transfer.log_extra},
                        )
                        raise e
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


# MARK: Hive Transaction Processing


async def process_hive_op(op: TrackedAny) -> LedgerEntry:
    """
    Processes the transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    # Check if a ledger entry with the same group_id already exists
    existing_entry = await LedgerEntry.collection().find_one(filter={"group_id": op.group_id})
    if existing_entry:
        logger.info(f"Ledger entry for group_id {op.group_id} already exists. Skipping.")
        try:
            ledger_entry = LedgerEntry.model_validate(existing_entry)
        except Exception as e:
            message = f"Error validating existing ledger entry: {e}"
            logger.error(message)
            raise LedgerEntryCreationException(message) from e

        raise LedgerEntryDuplicateException(
            f"Ledger entry already exists: {ledger_entry.group_id}"
        )

    # Check if the transfer is between the server account and the treasury account
    # Check if the transfer is between specific accounts

    ledger_entry = LedgerEntry(
        group_id=op.group_id,
        short_id=op.short_id,
        timestamp=op.timestamp,
        op_type=op.op_type,
    )
    # MARK: Transfers or Recurrent Transfers
    if isinstance(op, BlockMarker):
        raise LedgerEntryCreationException("BlockMarker is not a valid operation.")

    try:
        if isinstance(op, TransferBase):
            ledger_entry = await process_transfer_op(hive_transfer=op, ledger_entry=ledger_entry)

        elif isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder):
            ledger_entry = await process_create_fill_order_op(
                limit_fill_order=op, ledger_entry=ledger_entry
            )

        elif isinstance(op, CustomJson):
            # CustomJson operations are not yet implemented
            raise NotImplementedError("CustomJson operations are not yet implemented.")
        return ledger_entry

    except LedgerEntryException as e:
        logger.error(f"Error processing transfer operation: {e}")
        raise LedgerEntryCreationException(f"Error processing transfer operation: {e}") from e


async def process_transfer_op(
    hive_transfer: TrackedTransfer, ledger_entry: LedgerEntry
) -> LedgerEntry:
    """
    Processes the transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    expense_accounts = ["privex"]
    processed_d_memo = lightning_memo(hive_transfer.d_memo)
    base_description = f"{hive_transfer.amount_str} from {hive_transfer.from_account} to {hive_transfer.to_account} {processed_d_memo}"
    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
    if not server_account or not treasury_account or not funding_account or not exchange_account:
        raise LedgerEntryCreationException(
            "Server account, treasury account, funding account, or exchange account not configured."
        )
    if not hive_transfer.conv:
        raise LedgerEntryCreationException("Conversion not set in operation.")

    follow_on_task = None
    ledger_entry.description = base_description
    ledger_entry.credit_unit = ledger_entry.debit_unit = hive_transfer.unit
    ledger_entry.credit_amount = ledger_entry.debit_amount = hive_transfer.amount_decimal
    ledger_entry.credit_conv = ledger_entry.debit_conv = hive_transfer.conv
    ledger_entry.cust_id = hive_transfer.cust_id

    # MARK: Server to Treasury
    if (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.description = f"Server to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.SERVER_TO_TREASURY
    # MARK: Treasury to Server
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == server_account
    ):
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.description = f"Treasury to Server transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_SERVER
    # MARK: Funding to Treasury
    elif (
        hive_transfer.from_account == funding_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=funding_account
        )
        ledger_entry.description = f"Funding to Treasury transfer: {base_description}"
    # MARK: Treasury to Funding
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == funding_account
    ):
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=treasury_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=funding_account)
        ledger_entry.description = f"Treasury to Funding transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_FUNDING
    # MARK: Treasury to Exchange
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == exchange_account
    ):
        ledger_entry.debit = AssetAccount(name="Exchange Deposits Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.description = f"Treasury to Exchange transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.TREASURY_TO_EXCHANGE
        # MARK: Exchange to Treasury
    elif (
        hive_transfer.from_account == exchange_account
        and hive_transfer.to_account == treasury_account
    ):
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Exchange Deposits Hive", sub=treasury_account)
        ledger_entry.description = f"Exchange to Treasury transfer: {base_description}"
        ledger_entry.ledger_type = LedgerType.EXCHANGE_TO_TREASURY
        # MARK: Payments to special expense accounts if
    elif (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account in expense_accounts
    ):
        # TODO: #110 Implement the system for expense accounts
        raise NotImplementedError("External expense accounts not implemented yet")
    # MARK: Server to customer account withdrawal
    elif hive_transfer.from_account == server_account:
        customer = hive_transfer.to_account
        server = hive_transfer.from_account
        ledger_entry.debit = LiabilityAccount("Customer Liability", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.description = f"Withdrawal: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_OUT
        # TODO: There is an argument to say that this hive_transfer should be noted as being connected to the prior event.

    # MARK: Customer account to server account deposit
    elif hive_transfer.to_account == server_account:
        customer = hive_transfer.from_account
        server = hive_transfer.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount("Customer Liability", sub=customer)
        ledger_entry.description = f"Deposit: {base_description}"
        ledger_entry.ledger_type = LedgerType.CUSTOMER_HIVE_IN
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        follow_on_task = process_hive_to_lightning(hive_transfer=hive_transfer)
    else:
        logger.info(
            f"Transfer between two different accounts: {hive_transfer.from_account} -> {hive_transfer.to_account}"
        )
        raise LedgerEntryCreationException("Transfer between untracked accounts.")
    await ledger_entry.save()

    if follow_on_task:
        # If there is a follow-on task, we need to run it in the background
        try:
            await follow_on_task
        except Exception as e:
            logger.exception(f"Follow-on task failed: {e}", extra={"notification": False})

    return ledger_entry


async def process_create_fill_order_op(
    limit_fill_order: Union[LimitOrderCreate, FillOrder], ledger_entry: LedgerEntry
) -> LedgerEntry:
    """
    Processes the create or fill order operation and creates a ledger entry if applicable.

    This method handles various types of orders, including limit orders and fill orders. It ensures that
    appropriate debit and credit accounts are assigned based on the order type. If a ledger entry with
    the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    if isinstance(limit_fill_order, LimitOrderCreate):
        logger.info(f"Limit order create: {limit_fill_order.orderid}")
        if not limit_fill_order.conv or limit_fill_order.conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.conv = CryptoConv(
                conv_from=limit_fill_order.amount_to_sell.unit,  # HIVE
                value=limit_fill_order.amount_to_sell.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.min_to_receive.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        ledger_entry.debit = AssetAccount(name="Escrow Hive", sub=limit_fill_order.owner)
        ledger_entry.credit = AssetAccount(
            name="Customer Deposits Hive", sub=limit_fill_order.owner
        )
        ledger_entry.description = limit_fill_order.ledger_str
        ledger_entry.ledger_type = LedgerType.LIMIT_ORDER_CREATE
        ledger_entry.debit_unit = ledger_entry.credit_unit = limit_fill_order.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = (
            limit_fill_order.amount_to_sell.amount_decimal
        )
        ledger_entry.debit_conv = ledger_entry.credit_conv = limit_fill_order.conv
    elif isinstance(limit_fill_order, FillOrder):
        logger.info(
            f"Fill order operation: {limit_fill_order.open_orderid} {limit_fill_order.current_owner}"
        )
        if not limit_fill_order.debit_conv or limit_fill_order.debit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.debit_conv = CryptoConv(
                conv_from=limit_fill_order.open_pays.unit,  # HIVE
                value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                converted_value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        if not limit_fill_order.credit_conv or limit_fill_order.credit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=limit_fill_order.timestamp)
            limit_fill_order.credit_conv = CryptoConv(
                conv_from=limit_fill_order.current_pays.unit,  # HBD
                value=limit_fill_order.current_pays.amount_decimal,  # 6.738 HBD
                converted_value=limit_fill_order.open_pays.amount_decimal,  # 25.052 HIVE
                quote=quote,
                timestamp=limit_fill_order.timestamp,
            )
        ledger_entry.debit = AssetAccount(
            name="Customer Deposits Hive", sub=limit_fill_order.current_owner
        )
        ledger_entry.credit = AssetAccount(name="Escrow Hive", sub=limit_fill_order.current_owner)
        ledger_entry.description = limit_fill_order.ledger_str
        ledger_entry.ledger_type = LedgerType.FILL_ORDER
        ledger_entry.debit_unit = limit_fill_order.open_pays.unit  # HIVE (received)
        ledger_entry.credit_unit = limit_fill_order.current_pays.unit  # HBD (given)
        ledger_entry.debit_amount = limit_fill_order.open_pays.amount_decimal  # 25.052 HIVE
        ledger_entry.credit_amount = limit_fill_order.current_pays.amount_decimal  # 6.738 HBD
        ledger_entry.debit_conv = limit_fill_order.debit_conv  # Conversion for HIVE
        ledger_entry.credit_conv = limit_fill_order.credit_conv  # Conversion for HBD
    else:
        logger.error(f"Unsupported operation type: {type(limit_fill_order)}")
        raise LedgerEntryCreationException("Unsupported operation type.")
    return ledger_entry
