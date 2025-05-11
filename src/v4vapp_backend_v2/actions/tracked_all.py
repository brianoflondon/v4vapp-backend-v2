from typing import Any, Union

from v4vapp_backend_v2.accounting.account_type import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any
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
            return op_any(tracked)
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
    Processes the transfer operation. This method is a placeholder and should
    be overridden in subclasses to provide specific processing logic.

    Returns:
        LedgerEntry
    """
    await op.lock_op()
    ledger_entry = None
    server_account = InternalConfig().config.hive.server_account.name
    treasury_account = InternalConfig().config.hive.treasury_account.name
    funding_account = InternalConfig().config.hive.funding_account.name
    exchange_account = InternalConfig().config.hive.exchange_account.name
    # Check if the transfer is between the server account and the treasury account
    # Check if the transfer is between specific accounts
    if op.from_account == server_account and op.to_account == treasury_account:
        logger.info(
            f"Transfer from server account to treasury account: {op.from_account} -> {op.to_account}"
        )
    elif op.from_account == treasury_account and op.to_account == server_account:
        logger.info(
            f"Transfer from treasury account to server account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry = LedgerEntry(
            group_id=op.group_id,
            timestamp=op.timestamp,
            description=op.d_memo,
            unit=op.unit,
            amount=op.amount_decimal,
            conv=op.conv,
            debit=AssetAccount(name="Customer Deposits Hive", sub=op.to_account),
            credit=AssetAccount(name="Treasury Hive", sub=op.from_account),
            op=op,
        )

    elif op.from_account == funding_account and op.to_account == treasury_account:
        logger.info(
            f"Transfer from funding account to treasury account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry = LedgerEntry(
            group_id=op.group_id,
            timestamp=op.timestamp,
            description=op.d_memo,
            unit=op.unit,
            amount=op.amount_decimal,
            conv=op.conv,
            debit=AssetAccount(name="Treasury Hive", sub=op.to_account),
            credit=LiabilityAccount(name="Owner Loan Payable (funding)", sub=op.from_account),
            op=op,
        )

    elif op.from_account == treasury_account and op.to_account == funding_account:
        logger.info(
            f"Transfer from treasury account to funding account: {op.from_account} -> {op.to_account}"
        )
    elif op.from_account == treasury_account and op.to_account == exchange_account:
        logger.info(
            f"Transfer from treasury account to exchange account: {op.from_account} -> {op.to_account}"
        )
    elif op.from_account == exchange_account and op.to_account == treasury_account:
        logger.info(
            f"Transfer from exchange account to treasury account: {op.from_account} -> {op.to_account}"
        )
    elif op.from_account == server_account:
        logger.info(
            f"Transfer from server account to another account: {op.from_account} -> {op.to_account}"
        )
    elif op.to_account == server_account:
        logger.info(
            f"Transfer to server account from another account: {op.from_account} -> {op.to_account}"
        )
    else:
        logger.info(
            f"Transfer between two different accounts: {op.from_account} -> {op.to_account}"
        )

    if ledger_entry and ledger_entry.group_id and TrackedBaseModel.db_client:
        try:
            await TrackedBaseModel.db_client.insert_one(
                collection_name=ledger_entry.collection,
                document=ledger_entry.model_dump(
                    by_alias=True
                ),  # Ensure model_dump() is called correctly
            )
        except Exception as e:
            logger.error(f"Error updating ledger: {e}")

    await op.unlock_op()
    if ledger_entry:
        return ledger_entry
    else:
        return None
