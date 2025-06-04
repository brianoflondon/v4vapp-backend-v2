import asyncio
from typing import Any, List, Union

from pydantic import ValidationError

from v4vapp_backend_v2.accounting.account_type import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import (
    LedgerEntry,
    LedgerEntryCreationException,
    LedgerEntryDuplicateException,
    LedgerEntryException,
)
from v4vapp_backend_v2.actions.hive_to_lightning import (
    calculate_hive_return_change,
    lightning_payment_sent,
    process_hive_to_lightning,
)
from v4vapp_backend_v2.actions.lightning_to_hive import process_lightning_to_hive
from v4vapp_backend_v2.actions.tracked_any import DiscriminatedTracked, TrackedAny, TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_fill_recurrent_transfer import FillRecurrentTransfer
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_recurrent_transfer import RecurrentTransfer
from v4vapp_backend_v2.hive_models.op_transfer import Transfer, TransferBase
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment

# TrackedAny = Union[OpAny, Invoice, Payment]
# TODO: #111 implement discriminator in models to pick the right one


def tracked_any_filter(tracked: dict[str, Any]) -> TrackedAny:
    """
    Validates and filters a tracked object, ensuring it is of type OpAny, Invoice, or Payment.

    Removes the '_id' field from the input dictionary if present, then attempts to validate
    the object using the DiscriminatedTracked model. If validation is successful, returns
    the validated object as a TrackedAny type. Raises a ValueError if validation fails.

    Args:
        tracked (dict[str, Any]): The tracked object to validate and filter.

    Returns:
        TrackedAny: The validated tracked object of type OpAny, Invoice, or Payment.

    Raises:
        ValueError: If the object cannot be validated as one of the expected types.

    """
    if "_id" in tracked:
        del tracked["_id"]  # Remove _id field if present

    try:
        value = {"value": tracked}
        answer = DiscriminatedTracked.model_validate(value)
        return answer.value
    except ValidationError as e:
        raise ValueError(f"Failed to validate tracked object: {e}") from e
    except ValueError as e:
        logger.warning(
            f"Parsing as OpAny, Invoice, or Payment. {e}",
            extra={"notification": False, "tracked": tracked},
        )
        raise ValueError(
            f"Invalid tracked object type: Expected OpAny, Invoice, or Payment. {e}"
        ) from e


def tracked_transfer_filter(tracked: dict[str, Any]) -> TrackedTransfer:
    """
    Validates and filters a tracked object, ensuring it is of type TrackedTransfer.

    Removes the '_id' field from the input dictionary if present, then attempts to validate
    the object using the TrackedTransfer model. If validation is successful, returns
    the validated object as a TrackedTransfer type. Raises a ValueError if validation fails.

    Args:
        tracked (dict[str, Any]): The tracked object to validate and filter.

    Returns:
        TrackedTransfer: The validated tracked object of type TrackedTransfer.

    Raises:
        ValueError: If the object cannot be validated as a TrackedTransfer.
    """
    tracked_any = tracked_any_filter(tracked)
    if isinstance(tracked_any, (TransferBase, Transfer, RecurrentTransfer, FillRecurrentTransfer)):
        return tracked_any
    raise ValueError(
        f"Invalid tracked object type: Expected TrackedTransfer, got {type(tracked_any)}"
    )


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
    try:
        # This is the outer lock
        async with tracked_op:
            if isinstance(tracked_op, (TransferBase, LimitOrderCreate, FillOrder)):
                ledger_entry = await process_hive_op(op=tracked_op)
                ledger_entries = [ledger_entry]
            elif isinstance(tracked_op, Invoice):
                ledger_entries = await process_lightning_op(op=tracked_op)
            elif isinstance(tracked_op, Payment):
                ledger_entries = await process_lightning_op(op=tracked_op)
            else:
                raise ValueError("Invalid tracked object")

            if not ledger_entries:
                raise LedgerEntryCreationException("Ledger entry cannot be created.")
            for ledger_entry in ledger_entries:
                ans = await ledger_entry.save()
                logger.info(
                    f"Ledger entry saved: {ans}",
                    extra={**ledger_entry.log_extra, "notification": False},
                )
            return ledger_entries
    except LedgerEntryDuplicateException as e:
        raise LedgerEntryDuplicateException(f"Ledger entry already exists: {e}") from e

    except LedgerEntryException as e:
        logger.exception(f"Error processing tracked operation: {e}")
        raise LedgerEntryException(f"Error processing tracked operation: {e}") from e


async def process_lightning_op(op: Invoice | Payment) -> [LedgerEntry]:
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
        timestamp=op.creation_date,
        op=op,
    )
    if isinstance(op, Invoice):
        ledger_entries = await process_lightning_invoice(invoice=op, ledger_entry=ledger_entry)
    elif isinstance(op, Payment):
        ledger_entries = await process_lightning_payment(payment=op, ledger_entry=ledger_entry)

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
    if not invoice.conv or invoice.conv.is_unset():
        await invoice.update_conv()
    ledger_entry.description = invoice.memo
    ledger_entry.credit_unit = ledger_entry.debit_unit = Currency.MSATS
    ledger_entry.credit_amount = ledger_entry.debit_amount = float(invoice.amt_paid_msat)
    ledger_entry.credit_conv = invoice.conv or CryptoConv()
    ledger_entry.debit_conv = invoice.conv or CryptoConv()
    if "Funding" in invoice.memo:
        # Treat this as an incoming Owner's loan Funding to Treasury Lightning
        ledger_entry.debit = AssetAccount(name="Treasury Lightning", sub=node_name)
        ledger_entry.credit = LiabilityAccount(name="Owner Loan Payable (funding)", sub=node_name)
        return [ledger_entry]
    if invoice.is_lndtohive:
        await process_lightning_to_hive(invoice=invoice)
    elif "Exchange" in invoice.memo:
        print(invoice)

    raise NotImplementedError("process_lightning_op is not implemented yet.")


# MARK: Payment (outbound Lightning)


async def process_lightning_payment(
    payment: Payment, ledger_entry: LedgerEntry
) -> List[LedgerEntry]:
    """
    Processes a Lightning Network payment and updates the corresponding ledger entry.

    This function handles outgoing Lightning payments, updating the ledger entry with
    the appropriate credit and debit information based on the payment details. If the
    payment memo contains specific keywords (e.g., "Funding"), it assigns the correct
    asset and liability accounts. For other cases, it raises a NotImplementedError.

    Args:
        payment (Payment): The Lightning payment object containing payment details.
        ledger_entry (LedgerEntry): The ledger entry to be updated based on the payment.

    Returns:
        LedgerEntry: The updated ledger entry reflecting the processed payment.

    Raises:
        NotImplementedError: If the payment memo does not match implemented cases.
    """
    # The Payment will already have been locked by the outer payment processing function.
    assert TrackedBaseModel.db_client, "Database client must be set before processing payments."
    if not payment.conv or payment.conv.is_unset():
        await payment.update_conv()
    v4vapp_group_id = ""
    if payment.custom_records:
        v4vapp_group_id = payment.custom_records.v4vapp_group_id or ""
        keysend_message = payment.custom_records.keysend_message or ""
        existing_ledger_entry = await TrackedBaseModel.db_client.find_one(
            collection_name=LedgerEntry.collection(), query={"group_id": v4vapp_group_id}
        )
        if existing_ledger_entry:
            old_ledger_entry = LedgerEntry.model_validate(existing_ledger_entry)
            print(old_ledger_entry)
            print(old_ledger_entry.draw_t_diagram())
            hive_transfer = old_ledger_entry.op
            # MARK: Hive to Lightning Payment
            if isinstance(hive_transfer, TransferBase):
                # This means we have found an inbound Hive payment and this is the matching
                # payment
                # https://x.com/i/grok/share/YLSqQzDg6kmuxeaohwtff7s1m
                # Build the new ledger entry for the  Conversion of Hive to Sats in the hive_to_lightning file.

                # Mark: Record the Payment in the Hive Op
                cost_of_payment_msat = payment.value_msat + payment.fee_msat
                message = f"Lightning payment sent for operation {hive_transfer.group_id_p}: {payment.payment_hash} {payment.route_str} {cost_of_payment_msat / 1000:,.0f} sats"

                change_amount = await calculate_hive_return_change(
                    hive_transfer=hive_transfer,
                    payment=payment,
                )
                assert hive_transfer.change_amount is not None, (
                    "Change amount should not be None after calculating change."
                )

                # These two functions are carried out in the calculate_hive_return_change function
                # hive_transfer.change_amount = change_amount
                # await hive_transfer.update_conv()

                hive_transfer.add_reply(
                    reply_id=payment.group_id_p,
                    reply_type="payment",
                    reply_msat=cost_of_payment_msat,
                    reply_error=None,
                    reply_message=message,
                )
                quote = await TrackedBaseModel.nearest_quote(timestamp=payment.timestamp)
                hive_transfer.fee_conv = CryptoConversion(
                    conv_from=Currency.MSATS,
                    value=hive_transfer.conv.msats_fee,
                    quote=quote,
                ).conversion
                ans = await hive_transfer.save(
                    include={"replies", "fee_conv", "change_amount", "change_conv"}
                )
                # Also need to update the ledger_entry for the original hive_transfer
                old_ledger_entry.op = hive_transfer
                await old_ledger_entry.update_op()

                node_name = InternalConfig().config.lnd_config.default
                ledger_entries_list = []

                # MARK: Conversion of Hive to Sats
                conversion_credit_amount = hive_transfer.amount.beam - change_amount
                conversion_debit_amount = cost_of_payment_msat + hive_transfer.conv.msats_fee
                conversion_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_conversion",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Conversion of {conversion_credit_amount} to Lightning {conversion_debit_amount / 1000:,.0f} sats",
                    debit=AssetAccount(
                        name="Treasury Lightning",
                        sub=node_name,  # This is the SERVER Lightning
                    ),
                    debit_unit=Currency.MSATS,
                    debit_amount=conversion_debit_amount,
                    debit_conv=payment.conv,
                    credit=AssetAccount(
                        name="Customer Deposits Hive",
                        sub=hive_transfer.to_account,  # This is the CUSTOMER
                    ),
                    credit_unit=hive_transfer.unit,
                    credit_amount=conversion_credit_amount.amount,
                    credit_conv=hive_transfer.conv,
                )
                ledger_entries_list.append(conversion_ledger_entry)

                # MARK: Outgoing
                # Build the 3a ledger entry: Outgoing Lightning Payment
                fee_debit_amount = getattr(hive_transfer.fee_conv, hive_transfer.unit.lower())
                outgoing_debit_amount = (
                    hive_transfer.amount.beam - change_amount - fee_debit_amount
                )
                outgoing_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_outgoing",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Outgoing Lightning Payment {outgoing_debit_amount} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=outgoing_debit_amount.amount,
                    debit_conv=hive_transfer.conv,
                    credit=AssetAccount(
                        name="Treasury Lightning",
                        sub=node_name,  # This is the SERVER
                    ),
                    credit_unit=Currency.MSATS,
                    credit_amount=cost_of_payment_msat,
                    credit_conv=payment.conv,
                )
                ledger_entries_list.append(outgoing_ledger_entry)

                # MARK: Fee
                # Build the 3b ledger entry: the Fee expense
                fee_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_fee",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Fee for Lightning Payment {cost_of_payment_msat / 1000:,.0f} sats",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=fee_debit_amount,
                    debit_conv=hive_transfer.conv,
                    credit=RevenueAccount(
                        name="Fee Income Lightning",
                        sub=node_name,  # This is the SERVER
                    ),
                    credit_unit=Currency.MSATS,
                    credit_amount=hive_transfer.conv.msats_fee,
                    credit_conv=hive_transfer.conv,
                )
                ledger_entries_list.append(fee_ledger_entry)

                asyncio.create_task(
                    lightning_payment_sent(
                        payment=payment,
                        hive_transfer=hive_transfer,
                        nobroadcast=True,
                    )
                )
                return ledger_entries_list

        raise NotImplementedError(f"Not implemented yet {v4vapp_group_id} {keysend_message}")

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
    if TrackedBaseModel.db_client:
        existing_entry = await TrackedBaseModel.db_client.find_one(
            collection_name=LedgerEntry.collection(), query={"group_id": op.group_id}
        )
        if existing_entry:
            logger.warning(f"Ledger entry for group_id {op.group_id} already exists. Skipping.")
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
        timestamp=op.timestamp,
        op=op,
    )
    # MARK: Transfers or Recurrent Transfers
    if isinstance(op, BlockMarker):
        raise LedgerEntryCreationException("BlockMarker is not a valid operation.")

    try:
        if isinstance(op, TransferBase):
            ledger_entry = await process_transfer_op(hive_transfer=op, ledger_entry=ledger_entry)

        elif isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder):
            ledger_entry = await process_create_fill_order_op(op=op, ledger_entry=ledger_entry)
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
    description = hive_transfer.d_memo if hive_transfer.d_memo else ""
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
    ledger_entry.description = description
    ledger_entry.credit_unit = ledger_entry.debit_unit = hive_transfer.unit
    ledger_entry.credit_amount = ledger_entry.debit_amount = hive_transfer.amount_decimal
    ledger_entry.credit_conv = ledger_entry.debit_conv = hive_transfer.conv

    if (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account == treasury_account
    ):
        # MARK: Server to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == server_account
    ):
        # MARK: Treasury to Server
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
    elif (
        hive_transfer.from_account == funding_account
        and hive_transfer.to_account == treasury_account
    ):
        # MARK: Funding to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=funding_account
        )
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == funding_account
    ):
        # MARK: Treasury to Funding
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=treasury_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=funding_account)
    elif (
        hive_transfer.from_account == treasury_account
        and hive_transfer.to_account == exchange_account
    ):
        # MARK: Treasury to Exchange
        ledger_entry.debit = AssetAccount(name="Exchange Deposits Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
    elif (
        hive_transfer.from_account == exchange_account
        and hive_transfer.to_account == treasury_account
    ):
        # MARK: Exchange to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Exchange Deposits Hive", sub=treasury_account)
    elif (
        hive_transfer.from_account == server_account
        and hive_transfer.to_account in expense_accounts
    ):
        # MARK: Payments to special expense accounts if
        raise NotImplementedError("External expense accounts not implemented yet")
        # TODO: #110 Implement the system for expense accounts
    elif hive_transfer.from_account == server_account:
        # MARK: Server to customer account withdrawal
        customer = hive_transfer.to_account
        server = hive_transfer.from_account
        ledger_entry.debit = LiabilityAccount("Customer Liability Hive", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
    elif hive_transfer.to_account == server_account:
        # MARK: Customer account to server account
        customer = hive_transfer.from_account
        server = hive_transfer.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount("Customer Liability Hive", sub=customer)
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        follow_on_task = process_hive_to_lightning(hive_transfer=hive_transfer)
    else:
        logger.info(
            f"Transfer between two different accounts: {hive_transfer.from_account} -> {hive_transfer.to_account}"
        )
        raise LedgerEntryCreationException("Transfer between untracked accounts.")

    if follow_on_task:
        # If there is a follow-on task, we need to run it in the background
        asyncio.create_task(follow_on_task)

    return ledger_entry


async def process_create_fill_order_op(
    op: Union[LimitOrderCreate, FillOrder], ledger_entry: LedgerEntry
) -> LedgerEntry:
    """
    Processes the create or fill order operation and creates a ledger entry if applicable.

    This method handles various types of orders, including limit orders and fill orders. It ensures that
    appropriate debit and credit accounts are assigned based on the order type. If a ledger entry with
    the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    if isinstance(op, LimitOrderCreate):
        logger.info(f"Limit order create: {op.orderid}")
        if not op.conv or op.conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=op.timestamp)
            op.conv = CryptoConv(
                conv_from=op.amount_to_sell.unit,  # HIVE
                value=op.amount_to_sell.amount_decimal,  # 25.052 HIVE
                converted_value=op.min_to_receive.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=op.timestamp,
            )
        ledger_entry.op = op
        ledger_entry.debit = AssetAccount(name="Escrow Hive", sub=op.owner)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=op.owner)
        ledger_entry.description = op.ledger_str
        ledger_entry.debit_unit = ledger_entry.credit_unit = op.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = op.amount_to_sell.amount_decimal
        ledger_entry.debit_conv = ledger_entry.credit_conv = op.conv
    elif isinstance(op, FillOrder):
        logger.info(f"Fill order operation: {op.open_orderid} {op.current_owner}")
        if not op.debit_conv or op.debit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=op.timestamp)
            op.debit_conv = CryptoConv(
                conv_from=op.open_pays.unit,  # HIVE
                value=op.open_pays.amount_decimal,  # 25.052 HIVE
                converted_value=op.current_pays.amount_decimal,  # 6.738 HBD
                quote=quote,
                timestamp=op.timestamp,
            )
        if not op.credit_conv or op.credit_conv.is_unset():
            quote = await TrackedBaseModel.nearest_quote(timestamp=op.timestamp)
            op.credit_conv = CryptoConv(
                conv_from=op.current_pays.unit,  # HBD
                value=op.current_pays.amount_decimal,  # 6.738 HBD
                converted_value=op.open_pays.amount_decimal,  # 25.052 HIVE
                quote=quote,
                timestamp=op.timestamp,
            )
        ledger_entry.op = op
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=op.current_owner)
        ledger_entry.credit = AssetAccount(name="Escrow Hive", sub=op.current_owner)
        ledger_entry.description = op.ledger_str
        ledger_entry.debit_unit = op.open_pays.unit  # HIVE (received)
        ledger_entry.credit_unit = op.current_pays.unit  # HBD (given)
        ledger_entry.debit_amount = op.open_pays.amount_decimal  # 25.052 HIVE
        ledger_entry.credit_amount = op.current_pays.amount_decimal  # 6.738 HBD
        ledger_entry.debit_conv = op.debit_conv  # Conversion for HIVE
        ledger_entry.credit_conv = op.credit_conv  # Conversion for HBD
    else:
        logger.error(f"Unsupported operation type: {type(op)}")
        raise LedgerEntryCreationException("Unsupported operation type.")
    return ledger_entry
