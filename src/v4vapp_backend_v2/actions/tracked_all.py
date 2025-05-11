from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, Union

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


# Function to generate balance sheet
async def generate_balance_sheet(as_of_date: datetime = datetime.now(tz=timezone.utc)) -> Dict:
    """
    Generate a balance sheet from ledger entries as of a given date.
    Returns a dictionary with Assets, Liabilities, and Equity balances in USD.
    """
    # Initialize dictionaries to track balances
    ledger_entries = []

    async with TrackedBaseModel.db_client as db_client:
        cursor = await db_client.find(
            collection_name="ledger",
            query={"timestamp": {"$lte": as_of_date}},
            projection={
                "group_id": 1,
                "timestamp": 1,
                "description": 1,
                "amount": 1,
                "unit": 1,
                "conv": 1,
                "debit": 1,
                "credit": 1,
                "_id": 0,
                "op": 1,
            },
        )
        async for entry in cursor:
            ledger_entry = LedgerEntry.model_validate(entry)
            ledger_entries.append(ledger_entry)

    account_balances = defaultdict(float)

    # Process each ledger entry
    for entry in ledger_entries:
        if entry.timestamp > as_of_date:
            continue  # Skip entries after the as_of_date

        # Get USD amount from conv.usd
        usd_amount = entry.conv.usd

        # Update debit account (Assets increase, Liabilities/Equity decrease)
        if entry.debit.account_type == "Asset":
            account_balances[entry.debit.name] += usd_amount
        elif entry.debit.account_type in ["Liability", "Equity"]:
            account_balances[entry.debit.name] -= usd_amount

        # Update credit account (Assets decrease, Liabilities/Equity increase)
        if entry.credit.account_type == "Asset":
            account_balances[entry.credit.name] -= usd_amount
        elif entry.credit.account_type in ["Liability", "Equity"]:
            account_balances[entry.credit.name] += usd_amount

    # Organize balances by account type
    balance_sheet = {"Assets": {}, "Liabilities": {}, "Equity": {}}

    # Assign balances to appropriate categories
    for account_name, balance in account_balances.items():
        if account_name in [
            "Customer Deposits Hive",
            "Customer Deposits Lightning",
            "Treasury Hive",
            "Treasury Lightning",
        ]:
            balance_sheet["Assets"][account_name] = round(balance, 2)
        elif account_name in [
            "Customer Liability Hive",
            "Customer Liability Lightning",
            "Tax Liabilities",
            "Owner Loan Payable (funding)",
        ]:
            balance_sheet["Liabilities"][account_name] = round(balance, 2)
        elif account_name in [
            "Owner's Capital",
            "Retained Earnings",
            "Dividends/Distributions",
        ]:
            balance_sheet["Equity"][account_name] = round(balance, 2)

    # Calculate totals
    balance_sheet["Assets"]["Total Assets"] = round(sum(balance_sheet["Assets"].values()), 2)
    balance_sheet["Liabilities"]["Total Liabilities"] = round(
        sum(balance_sheet["Liabilities"].values()), 2
    )
    balance_sheet["Equity"]["Total Equity"] = round(sum(balance_sheet["Equity"].values()), 2)
    balance_sheet["Total Liabilities and Equity"] = round(
        balance_sheet["Liabilities"]["Total Liabilities"]
        + balance_sheet["Equity"]["Total Equity"],
        2,
    )

    return balance_sheet


def formatted_balance_sheet(balance_sheet: Dict) -> str:
    """
    Formats a balance sheet as a string, 70 characters wide with right-aligned numbers.

    Args:
        balance_sheet: Dictionary containing Assets, Liabilities, Equity, and totals.

    Returns:
        A formatted string representing the balance sheet.
    """
    # Accumulate lines in a list
    lines = []

    # Define column widths
    number_width = 12  # For "$XXXXXX.XX"
    name_width = 70 - number_width - 3  # 3 for ": " and space before number
    total_width = 70

    # Assets section
    lines.append("\nAssets")
    lines.append("-" * total_width)
    for account, balance in balance_sheet["Assets"].items():
        if account != "Total Assets":
            account_truncated = account[:name_width] if len(account) > name_width else account
            lines.append(f"{account_truncated:<{name_width}}: ${balance:>{number_width - 1}.2f}")
    lines.append(
        f"{'Total Assets':<{name_width}}: ${balance_sheet['Assets']['Total Assets']:>{number_width - 1}.2f}"
    )

    # Liabilities section
    lines.append("\nLiabilities")
    lines.append("-" * total_width)
    for account, balance in balance_sheet["Liabilities"].items():
        if account != "Total Liabilities":
            account_truncated = account[:name_width] if len(account) > name_width else account
            lines.append(f"{account_truncated:<{name_width}}: ${balance:>{number_width - 1}.2f}")
    lines.append(
        f"{'Total Liabilities':<{name_width}}: ${balance_sheet['Liabilities']['Total Liabilities']:>{number_width - 1}.2f}"
    )

    # Equity section
    lines.append("\nEquity")
    lines.append("-" * total_width)
    for account, balance in balance_sheet["Equity"].items():
        if account != "Total Equity":
            account_truncated = account[:name_width] if len(account) > name_width else account
            lines.append(f"{account_truncated:<{name_width}}: ${balance:>{number_width - 1}.2f}")
    lines.append(
        f"{'Total Equity':<{name_width}}: ${balance_sheet['Equity']['Total Equity']:>{number_width - 1}.2f}"
    )

    # Total Liabilities and Equity
    lines.append("\nTotal Liabilities and Equity")
    lines.append("-" * total_width)
    lines.append(
        f"{'':<{name_width}}  ${balance_sheet['Total Liabilities and Equity']:>{number_width - 1}.2f}"
    )

    # Join lines with newlines and return
    return "\n".join(lines)


