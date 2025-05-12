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
    ledger_entry = LedgerEntry(
        group_id=op.group_id,
        timestamp=op.timestamp,
        description=op.d_memo,
        unit=op.unit,
        amount=op.amount_decimal,
        conv=op.conv,
        op=op,
    )

    if op.from_account == server_account and op.to_account == treasury_account:
        # MARK: Server to Treasury
        logger.info(
            f"Transfer from server account to treasury account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=op.to_account)
        ledger_entry.credit = LiabilityAccount(name="Owner's Capital", sub=op.from_account)
    elif op.from_account == treasury_account and op.to_account == server_account:
        # MARK: Treasury to Server
        logger.info(
            f"Transfer from treasury account to server account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=op.to_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=op.from_account)
    elif op.from_account == funding_account and op.to_account == treasury_account:
        # MARK: Funding to Treasury
        logger.info(
            f"Transfer from funding account to treasury account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=op.to_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=op.from_account
        )
    elif op.from_account == treasury_account and op.to_account == funding_account:
        # MARK: Treasury to Funding
        logger.info(
            f"Transfer from treasury account to funding account: {op.from_account} -> {op.to_account}"
        )
        ledger_entry.debit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=op.from_account
        )
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=op.to_account)
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

    if ledger_entry and ledger_entry.debit and ledger_entry.credit and TrackedBaseModel.db_client:
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
    Generate a balance sheet with sub-accounts as of a given date.
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
        usd_amount = entry.conv.hive

        # Update debit account
        debit_key = (entry.debit.name, entry.debit.sub)
        if entry.debit.account_type == "Asset":
            account_balances[debit_key] += usd_amount
        elif entry.debit.account_type in ["Liability", "Equity"]:
            account_balances[debit_key] -= usd_amount

        # Update credit account
        credit_key = (entry.credit.name, entry.credit.sub)
        if entry.credit.account_type == "Asset":
            account_balances[credit_key] -= usd_amount
        elif entry.credit.account_type in ["Liability", "Equity"]:
            account_balances[credit_key] += usd_amount

    # Organize balances by account type and main account
    balance_sheet = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    # Assign balances to appropriate categories
    for (account_name, sub), balance in account_balances.items():
        if account_name in [
            "Customer Deposits Hive",
            "Customer Deposits Lightning",
            "Treasury Hive",
            "Treasury Lightning",
        ]:
            balance_sheet["Assets"][account_name][sub] = round(balance, 2)
        elif account_name in [
            "Customer Liability Hive",
            "Customer Liability Lightning",
            "Tax Liabilities",
            "Owner Loan Payable (funding)",
        ]:
            balance_sheet["Liabilities"][account_name][sub] = round(balance, 2)
        elif account_name in ["Owner's Capital", "Retained Earnings", "Dividends/Distributions"]:
            balance_sheet["Equity"][account_name][sub] = round(balance, 2)

    # Calculate main account totals and overall totals
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            total = sum(balance_sheet[category][account_name].values())
            balance_sheet[category][account_name]["Total"] = round(total, 2)
        balance_sheet[category]["Total"] = round(
            sum(
                account["Total"]
                for account in balance_sheet[category].values()
                if "Total" in account
            ),
            2,
        )

    balance_sheet["Total Liabilities and Equity"] = round(
        balance_sheet["Liabilities"]["Total"] + balance_sheet["Equity"]["Total"], 2
    )

    return balance_sheet


def truncate_text(text: str, max_length: int) -> str:
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


def formatted_balance_sheet(balance_sheet: Dict, as_of_date: datetime) -> str:
    """
    Format a balance sheet as a string within 60 characters.
    Returns a string with Assets, Liabilities, and Equity in USD.
    """
    # Initialize string buffer
    output = []

    # Header (truncate to fit 60 chars)
    date_str = as_of_date.strftime("%Y-%m-%d")
    header = f"Balance Sheet as of {date_str}"
    output.append(f"{truncate_text(header, 60):^60}")
    output.append("-" * 60)

    # Assets
    output.append(f"{'Assets':^60}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Assets"].items():
        if account_name != "Total":
            # Main account (truncate to 40 chars)
            account_display = truncate_text(account_name, 40)
            output.append(f"{account_display:<40}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    # Sub-account: 4-space indent, truncate to 30 chars
                    sub_display = truncate_text(sub, 30)
                    formatted_balance = f"${balance:,.2f}"
                    output.append(f"    {sub_display:<30} {formatted_balance:>15}")
            # Account total: 2-space indent
            total_display = f"Total {truncate_text(account_name, 33)}"
            formatted_total = f"${sub_accounts['Total']:,.2f}"
            output.append(f"  {total_display:<33} {formatted_total:>15}")
    # Total Assets
    formatted_total_assets = f"${balance_sheet['Assets']['Total']:,.2f}"
    output.append(f"{'Total Assets':<40} {formatted_total_assets:>15}")

    # Liabilities
    output.append(f"\n{'Liabilities':^60}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Liabilities"].items():
        if account_name != "Total":
            account_display = truncate_text(account_name, 40)
            output.append(f"{account_display:<40}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    sub_display = truncate_text(sub, 30)
                    formatted_balance = f"${balance:,.2f}"
                    output.append(f"    {sub_display:<30} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 33)}"
            formatted_total = f"${sub_accounts['Total']:,.2f}"
            output.append(f"  {total_display:<33} {formatted_total:>15}")
    formatted_total_liabilities = f"${balance_sheet['Liabilities']['Total']:,.2f}"
    output.append(f"{'Total Liabilities':<40} {formatted_total_liabilities:>15}")

    # Equity
    output.append(f"\n{'Equity':^60}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Equity"].items():
        if account_name != "Total":
            account_display = truncate_text(account_name, 40)
            output.append(f"{account_display:<40}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    sub_display = truncate_text(sub, 30)
                    formatted_balance = f"${balance:,.2f}"
                    output.append(f"    {sub_display:<30} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 33)}"
            formatted_total = f"${sub_accounts['Total']:,.2f}"
            output.append(f"  {total_display:<33} {formatted_total:>15}")
    formatted_total_equity = f"${balance_sheet['Equity']['Total']:,.2f}"
    output.append(f"{'Total Equity':<40} {formatted_total_equity:>15}")

    # Total Liabilities and Equity
    output.append(f"\n{'Total Liabilities and Equity':^60}")
    output.append("-" * 5)
    formatted_total = f"${balance_sheet['Total Liabilities and Equity']:,.2f}"
    output.append(f"{'':<40} {formatted_total:>15}")

    # Join lines with newlines
    return "\n".join(output)
