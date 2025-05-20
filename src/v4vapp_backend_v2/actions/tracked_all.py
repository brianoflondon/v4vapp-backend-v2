import asyncio
from typing import Any, Union

from v4vapp_backend_v2.accounting.account_type import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.hive_to_lightning import process_hive_to_lightning
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment


class LedgerEntryException(Exception):
    """Custom exception for LedgerEntry errors."""

    pass


class LedgerEntryConfigurationException(LedgerEntryException):
    """Custom exception for LedgerEntry configuration errors."""

    pass


class LedgerEntryCreationException(LedgerEntryException):
    """Custom exception for LedgerEntry creation errors."""

    pass


class LedgerEntryDuplicateException(LedgerEntryException):
    """Custom exception for LedgerEntry duplicate errors."""

    pass


def tracked_any(tracked: dict[str, Any]) -> TrackedAny:
    """
    Check if the tracked object is of type OpAny, Invoice, or Payment.

    :param tracked: The object to check.
    :return: True if the object is of type OpAny, Invoice, or Payment, False otherwise.
    """
    if not tracked.get("locked", None):
        tracked["locked"] = False
    if isinstance(tracked, OpAny):
        return tracked
    elif isinstance(tracked, Invoice) or isinstance(tracked, Payment):
        return tracked

    if tracked.get("type", None):
        try:
            return op_any_or_base(tracked)
        except Exception as e:
            raise ValueError(f"Invalid tracked object: {e}")

    if tracked.get("r_hash", None):
        return Invoice.model_validate(tracked)
    if tracked.get("payment_hash", None):
        return Payment.model_validate(tracked)

    raise ValueError("Invalid tracked object")


async def process_tracked(tracked_op: TrackedAny) -> LedgerEntry:
    """
    Process the tracked object.

    :param tracked: The tracked object to process.
    :return: LedgerEntry
    """

    if isinstance(tracked_op, OpAny):
        ledger_entry = await process_hive_op(op=tracked_op)
        return ledger_entry
    elif isinstance(tracked_op, Invoice):
        return await process_lightning_op(op=tracked_op)
    elif isinstance(tracked_op, Payment):
        raise NotImplementedError("Payment processing is not implemented yet.")
    else:
        raise ValueError("Invalid tracked object")


async def ledger_entry_to_db(ledger_entry: LedgerEntry) -> None:
    """
    Save the ledger entry to the database.

    :param ledger_entry: The ledger entry to save.
    :return: The saved ledger entry.
    """
    if ledger_entry and ledger_entry.debit and ledger_entry.credit and TrackedBaseModel.db_client:
        try:
            ans = await TrackedBaseModel.db_client.insert_one(
                collection_name=LedgerEntry.collection(),
                document=ledger_entry.model_dump(by_alias=True),
                report_duplicates=True,
            )
            return ans
        except Exception as e:
            logger.error(f"Error saving ledger entry to database: {e}")
            raise LedgerEntryCreationException(f"Error saving ledger entry: {e}") from e
    raise LedgerEntryConfigurationException("Database client not configured.")


async def process_lightning_op(op: Invoice | Payment) -> LedgerEntry:
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
        ledger_entry = await process_lightning_invoice(invoice=op, ledger_entry=ledger_entry)
    elif isinstance(op, Payment):
        raise NotImplementedError("Payment processing is not implemented yet.")

    await ledger_entry_to_db(ledger_entry=ledger_entry)
    return ledger_entry


async def process_lightning_invoice(invoice: Invoice, ledger_entry: LedgerEntry) -> LedgerEntry:
    # Invoice means we are receiving sats from external.
    node_name = InternalConfig().config.lnd_config.default

    await invoice.lock_op()
    ledger_entry.description = invoice.memo
    ledger_entry.credit_unit = ledger_entry.debit_unit = Currency.MSATS
    ledger_entry.credit_amount = ledger_entry.debit_amount = float(invoice.amt_paid_msat)
    ledger_entry.credit_conv = invoice.conv or CryptoConv()
    ledger_entry.debit_conv = invoice.conv or CryptoConv()
    if "Funding" in invoice.memo:
        # Treat this as an incoming Owner's loan Funding to Treasury Lightning
        ledger_entry.debit = AssetAccount(name="Treasury Lightning", sub=node_name)
        ledger_entry.credit = LiabilityAccount(name="Owner Loan Payable (funding)", sub=node_name)
        return ledger_entry
    elif "Exchange" in invoice.memo:
        print(invoice)

    raise NotImplementedError("process_lightning_op is not implemented yet.")


# MARK: Hive Transaction Processing


async def process_hive_op(op: OpAny) -> LedgerEntry:
    """
    Processes the transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    if isinstance(op, BlockMarker):
        raise LedgerEntryCreationException("BlockMarker is not a valid operation.")
    if not isinstance(op, OpAny):
        raise LedgerEntryCreationException("Invalid operation type.")
    await op.lock_op()

    # Check if a ledger entry with the same group_id already exists
    if TrackedBaseModel.db_client:
        existing_entry = await TrackedBaseModel.db_client.find_one(
            collection_name=LedgerEntry.collection(), query={"group_id": op.group_id}
        )
        if existing_entry:
            logger.warning(f"Ledger entry for group_id {op.group_id} already exists. Skipping.")
            await op.unlock_op()
            try:
                ledger_entry = LedgerEntry.model_validate(existing_entry)
            except Exception as e:
                message = f"Error validating existing ledger entry: {e}"
                logger.error(message)
                raise LedgerEntryCreationException(message) from e

            raise LedgerEntryDuplicateException(f"Ledger entry already exists: {ledger_entry}")

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
            ledger_entry = await process_transfer_op(op=op, ledger_entry=ledger_entry)

        elif isinstance(op, LimitOrderCreate) or isinstance(op, FillOrder):
            ledger_entry = await process_create_fill_order_op(op=op, ledger_entry=ledger_entry)
    except LedgerEntryException as e:
        logger.error(f"Error processing transfer operation: {e}")
        await op.unlock_op()
        raise LedgerEntryCreationException(f"Error processing transfer operation: {e}") from e

    await ledger_entry_to_db(ledger_entry=ledger_entry)
    return ledger_entry


async def process_transfer_op(op: TransferBase, ledger_entry: LedgerEntry) -> LedgerEntry | None:
    """
    Processes the transfer operation and creates a ledger entry if applicable.

    This method handles various types of transfers, including those between the server account,
    treasury account, funding account, exchange account, and customer accounts. It ensures that
    appropriate debit and credit accounts are assigned based on the transfer type. If a ledger
    entry with the same group_id already exists, the operation is skipped.

    Returns:
        LedgerEntry: The created or existing ledger entry, or None if no entry is created.
    """
    description = op.d_memo if op.d_memo else ""
    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
    if not server_account or not treasury_account or not funding_account or not exchange_account:
        raise LedgerEntryCreationException(
            "Server account, treasury account, funding account, or exchange account not configured."
        )
    if not op.conv:
        raise LedgerEntryCreationException("Conversion not set in operation.")

    follow_on_task = None
    ledger_entry.description = description
    ledger_entry.credit_unit = ledger_entry.debit_unit = op.unit
    ledger_entry.credit_amount = ledger_entry.debit_amount = op.amount_decimal
    ledger_entry.credit_conv = ledger_entry.debit_conv = op.conv

    if op.from_account == server_account and op.to_account == treasury_account:
        # MARK: Server to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
    elif op.from_account == treasury_account and op.to_account == server_account:
        # MARK: Treasury to Server
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
    elif op.from_account == funding_account and op.to_account == treasury_account:
        # MARK: Funding to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=treasury_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=funding_account
        )
    elif op.from_account == treasury_account and op.to_account == funding_account:
        # MARK: Treasury to Funding
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=treasury_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=funding_account)
    elif op.from_account == treasury_account and op.to_account == exchange_account:
        # MARK: Treasury to Exchange
        ledger_entry.debit = AssetAccount(name="Exchange Deposits Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=treasury_account)
    elif op.from_account == exchange_account and op.to_account == treasury_account:
        # MARK: Exchange to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=exchange_account)
        ledger_entry.credit = AssetAccount(name="Exchange Deposits Hive", sub=treasury_account)
    elif op.from_account == server_account:
        # MARK: Server to customer account withdrawal
        customer = op.to_account
        server = op.from_account
        ledger_entry.debit = LiabilityAccount("Customer Liability Hive", sub=customer)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=server)
    elif op.to_account == server_account:
        # MARK: Customer account to server account
        customer = op.from_account
        server = op.to_account
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=server)
        ledger_entry.credit = LiabilityAccount("Customer Liability Hive", sub=customer)
        # Now we need to see if we can take action for this invoice
        # This will be handled in a separate task
        follow_on_task = process_hive_to_lightning(op=op)
    else:
        logger.info(
            f"Transfer between two different accounts: {op.from_account} -> {op.to_account}"
        )
        raise LedgerEntryCreationException("Transfer between untracked accounts.")

    if follow_on_task:
        # If we have a follow-on task, we need to run it in the background
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
        ledger_entry.debit = AssetAccount(name="Escrow Hive", sub=op.owner)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=op.owner)
        ledger_entry.description = op.ledger_str
        ledger_entry.debit_unit = ledger_entry.credit_unit = op.amount_to_sell.unit
        ledger_entry.debit_amount = ledger_entry.credit_amount = op.amount_to_sell.amount_decimal
        ledger_entry.debit_conv = ledger_entry.credit_conv = op.conv
    elif isinstance(op, FillOrder):
        logger.info(f"Fill order operation: {op.open_orderid} {op.current_owner}")
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
