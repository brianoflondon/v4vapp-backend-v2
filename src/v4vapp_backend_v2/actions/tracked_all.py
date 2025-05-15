import asyncio
from typing import Any, Union

from v4vapp_backend_v2.accounting.account_type import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.hive_to_lightning import process_hive_to_lightning
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.block_marker import BlockMarker
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any_or_base
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment

TrackedAny = Union[OpAny, Invoice, Payment]


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


def tracked_type(tracked: TrackedAny) -> str:
    """
    Generate a query for the tracked object.

    :param tracked: The tracked object.
    :return: A dictionary representing the query.
    """
    if isinstance(tracked, OpAny):
        return tracked.name
    elif isinstance(tracked, Invoice):
        return "invoice"
    elif isinstance(tracked, Payment):
        return "payment"
    else:
        raise ValueError("Invalid tracked object")


async def process_tracked(tracked: TrackedAny) -> LedgerEntry:
    """
    Process the tracked object.

    :param tracked: The tracked object to process.
    :return: LedgerEntry
    """
    if isinstance(tracked, OpAny):
        return await process_hive_op(op=tracked)
    elif isinstance(tracked, Invoice):
        return await process_lightning_op(op=tracked)
    elif isinstance(tracked, Payment):
        return await process_lightning_op(op=tracked)
    else:
        raise ValueError("Invalid tracked object")


async def process_lightning_op(op: Union[Invoice | Payment]) -> LedgerEntry:
    """
    Processes the Lightning operation. This method is a placeholder and should
    be overridden in subclasses to provide specific processing logic.

    Returns:
        None
    """
    pass  # Placeholder for Lightning operation processing logic


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
        return None
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
                logger.error(f"Error validating existing ledger entry: {e}")
                return None
            return ledger_entry  # Skip processing if duplicate

    # Check if the transfer is between the server account and the treasury account
    # Check if the transfer is between specific accounts

    ledger_entry = LedgerEntry(
        group_id=op.group_id,
        timestamp=op.timestamp,
        op=op,
    )
    # MARK: Transfers or Recurrent Transfers
    if isinstance(op, BlockMarker):
        return None

    if isinstance(op, TransferBase):
        ledger_entry = await process_transfer_op(op=op, ledger_entry=ledger_entry)

    elif isinstance(op, LimitOrderCreate):
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
    if ledger_entry and ledger_entry.debit and ledger_entry.credit and TrackedBaseModel.db_client:
        try:
            await TrackedBaseModel.db_client.insert_one(
                collection_name=LedgerEntry.collection(),
                document=ledger_entry.model_dump(
                    by_alias=True
                ),  # Ensure model_dump() is called correctly
                report_duplicates=True,
            )
            await op.unlock_op()
            return ledger_entry
        except Exception as e:
            logger.error(f"Error updating ledger: {e}")
    await op.unlock_op()
    return None


async def process_transfer_op(op: TransferBase, ledger_entry: LedgerEntry) -> LedgerEntry:
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
    server_account = InternalConfig().config.hive.server_account.name
    treasury_account = InternalConfig().config.hive.treasury_account.name
    funding_account = InternalConfig().config.hive.funding_account.name
    exchange_account = InternalConfig().config.hive.exchange_account.name

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
        asyncio.create_task(process_hive_to_lightning(op=op))
    else:
        logger.info(
            f"Transfer between two different accounts: {op.from_account} -> {op.to_account}"
        )
    return ledger_entry
