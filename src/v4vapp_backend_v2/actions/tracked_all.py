from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import pandas as pd

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
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=op.to_account)
        ledger_entry.credit = AssetAccount(name="Customer Deposits Hive", sub=op.from_account)
    elif op.from_account == treasury_account and op.to_account == server_account:
        # MARK: Treasury to Server
        ledger_entry.debit = AssetAccount(name="Customer Deposits Hive", sub=op.to_account)
        ledger_entry.credit = AssetAccount(name="Treasury Hive", sub=op.from_account)
    elif op.from_account == funding_account and op.to_account == treasury_account:
        # MARK: Funding to Treasury
        ledger_entry.debit = AssetAccount(name="Treasury Hive", sub=op.to_account)
        ledger_entry.credit = LiabilityAccount(
            name="Owner Loan Payable (funding)", sub=op.from_account
        )
    elif op.from_account == treasury_account and op.to_account == funding_account:
        # MARK: Treasury to Funding
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


async def get_ledger_entries(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "ledger",
) -> list[LedgerEntry]:
    """
    Get ledger entries from the database as of a given date.
    Returns a list of LedgerEntry objects.
    """
    ledger_entries = []

    async with TrackedBaseModel.db_client as db_client:
        cursor = await db_client.find(
            collection_name=collection_name,
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

    return ledger_entries


async def get_ledger_dataframe(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "ledger",
) -> pd.DataFrame:
    """
    Get ledger entries from the database as of a given date.
    Returns a DataFrame with LedgerEntry objects.
    """
    ledger_entries = await get_ledger_entries(
        as_of_date=as_of_date, collection_name=collection_name
    )
    data = []
    for entry in ledger_entries:
        data.append(
            {
                "timestamp": entry.timestamp,
                "group_id": entry.group_id,
                "description": entry.description,
                "amount": entry.amount,
                "unit": entry.unit,
                "conv_sats": entry.conv.sats,
                "conv_msats": entry.conv.msats,
                "conv_hive": entry.conv.hive,
                "conv_hbd": entry.conv.hbd,
                "conv_usd": entry.conv.usd,
                "debit_name": entry.debit.name,
                "debit_account_type": entry.debit.account_type,
                "debit_sub": entry.debit.sub,
                "credit_name": entry.credit.name,
                "credit_account_type": entry.credit.account_type,
                "credit_sub": entry.credit.sub,
            }
        )

    df = pd.DataFrame(data)
    return df


def generate_balance_sheet_pandas(df: pd.DataFrame) -> Dict:
    """
    Generate a balance sheet using Pandas, with balances in SATS, HIVE, HBD, USD.
    Returns a dictionary with Assets, Liabilities, and Equity.
    """
    # Process debits
    debit_df = df[
        [
            "debit_name",
            "debit_account_type",
            "debit_sub",
            "conv_sats",
            "conv_msats",
            "conv_hive",
            "conv_hbd",
            "conv_usd",
        ]
    ].copy()
    debit_df["sats"] = debit_df["conv_sats"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_sats"]
    )
    debit_df["msats"] = debit_df["conv_msats"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_msats"]
    )
    debit_df["hive"] = debit_df["conv_hive"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_hive"]
    )
    debit_df["hbd"] = debit_df["conv_hbd"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_hbd"]
    )
    debit_df["usd"] = debit_df["conv_usd"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_usd"]
    )
    debit_df = debit_df.rename(
        columns={"debit_name": "name", "debit_account_type": "account_type", "debit_sub": "sub"}
    )

    # Process credits
    credit_df = df[
        [
            "credit_name",
            "credit_account_type",
            "credit_sub",
            "conv_sats",
            "conv_msats",
            "conv_hive",
            "conv_hbd",
            "conv_usd",
        ]
    ].copy()
    credit_df["sats"] = -credit_df["conv_sats"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_sats"]
    )
    credit_df["msats"] = -credit_df["conv_msats"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_msats"]
    )
    credit_df["hive"] = -credit_df["conv_hive"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_hive"]
    )
    credit_df["hbd"] = -credit_df["conv_hbd"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_hbd"]
    )
    credit_df["usd"] = -credit_df["conv_usd"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_usd"]
    )
    credit_df = credit_df.rename(
        columns={"credit_name": "name", "credit_account_type": "account_type", "credit_sub": "sub"}
    )

    # Combine debits and credits
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)

    # Aggregate balances by name and sub
    balance_df = (
        combined_df.groupby(["name", "sub", "account_type"])[
            ["sats", "msats", "hive", "hbd", "usd"]
        ]
        .sum()
        .reset_index()
    )

    # Initialize balance sheet structure
    balance_sheet = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    # Assign balances to categories
    for _, row in balance_df.iterrows():
        name, sub, account_type = row["name"], row["sub"], row["account_type"]
        if account_type == "Asset":
            balance_sheet["Assets"][name][sub] = {
                "sats": round(row["sats"], 0),
                "msats": round(row["msats"], 0),
                "hive": round(row["hive"], 2),
                "hbd": round(row["hbd"], 2),
                "usd": round(row["usd"], 2),
            }
        elif account_type == "Liability":
            balance_sheet["Liabilities"][name][sub] = {
                "sats": round(row["sats"], 0),
                "msats": round(row["msats"], 0),
                "hive": round(row["hive"], 2),
                "hbd": round(row["hbd"], 2),
                "usd": round(row["usd"], 2),
            }
        elif account_type == "Equity":
            balance_sheet["Equity"][name][sub] = {
                "sats": round(row["sats"], 0),
                "msats": round(row["msats"], 0),
                "hive": round(row["hive"], 2),
                "hbd": round(row["hbd"], 2),
                "usd": round(row["usd"], 2),
            }

    # Calculate totals
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            total_sats = sum(
                sub_acc["sats"] for sub_acc in balance_sheet[category][account_name].values()
            )
            total_msats = sum(
                sub_acc["msats"] for sub_acc in balance_sheet[category][account_name].values()
            )
            total_hive = sum(
                sub_acc["hive"] for sub_acc in balance_sheet[category][account_name].values()
            )
            total_hbd = sum(
                sub_acc["hbd"] for sub_acc in balance_sheet[category][account_name].values()
            )
            total_usd = sum(
                sub_acc["usd"] for sub_acc in balance_sheet[category][account_name].values()
            )
            balance_sheet[category][account_name]["Total"] = {
                "sats": round(total_sats, 0),
                "msats": round(total_msats, 0),
                "hive": round(total_hive, 2),
                "hbd": round(total_hbd, 2),
                "usd": round(total_usd, 2),
            }
        total_sats = sum(
            acc["Total"]["sats"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_msats = sum(
            acc["Total"]["msats"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_hive = sum(
            acc["Total"]["hive"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_hbd = sum(
            acc["Total"]["hbd"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_usd = sum(
            acc["Total"]["usd"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        balance_sheet[category]["Total"] = {
            "sats": round(total_sats, 2),
            "msats": round(total_msats, 0),
            "hive": round(total_hive, 2),
            "hbd": round(total_hbd, 2),
            "usd": round(total_usd, 2),
        }

    balance_sheet["Total Liabilities and Equity"] = {
        "sats": round(
            balance_sheet["Liabilities"]["Total"]["sats"]
            + balance_sheet["Equity"]["Total"]["sats"],
            0,
        ),
        "msats": round(
            balance_sheet["Liabilities"]["Total"]["msats"]
            + balance_sheet["Equity"]["Total"]["msats"],
            0,
        ),
        "hive": round(
            balance_sheet["Liabilities"]["Total"]["hive"]
            + balance_sheet["Equity"]["Total"]["hive"],
            2,
        ),
        "hbd": round(
            balance_sheet["Liabilities"]["Total"]["hbd"] + balance_sheet["Equity"]["Total"]["hbd"],
            2,
        ),
        "usd": round(
            balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
            2,
        ),
    }

    return balance_sheet


# Format balance sheet as string (USD, 100-char width)
def format_balance_sheet(balance_sheet: Dict, as_of_date: datetime) -> str:
    """
    Format a balance sheet as a string within 100 characters (USD only).
    Returns a string with Assets, Liabilities, and Equity.
    """
    output = []
    date_str = as_of_date.strftime("%Y-%m-%d")
    header = f"Balance Sheet as of {date_str}"
    output.append(f"{truncate_text(header, 100):^100}")
    output.append("-" * 100)

    # Assets
    output.append(f"{'Assets':^100}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Assets"].items():
        if account_name != "Total":
            account_display = truncate_text(account_name, 80)
            output.append(f"{account_display:<80}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    sub_display = truncate_text(sub, 70)
                    formatted_balance = f"${balance['usd']:,.2f}"
                    output.append(f"    {sub_display:<70} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 73)}"
            formatted_total = f"${sub_accounts['Total']['usd']:,.2f}"
            output.append(f"  {total_display:<73} {formatted_total:>15}")
    formatted_total_assets = f"${balance_sheet['Assets']['Total']['usd']:,.2f}"
    output.append(f"{'Total Assets':<80} {formatted_total_assets:>15}")

    # Liabilities
    output.append(f"\n{'Liabilities':^100}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Liabilities"].items():
        if account_name != "Total":
            account_display = truncate_text(account_name, 80)
            output.append(f"{account_display:<80}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    sub_display = truncate_text(sub, 70)
                    formatted_balance = f"${balance['usd']:,.2f}"
                    output.append(f"    {sub_display:<70} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 73)}"
            formatted_total = f"${sub_accounts['Total']['usd']:,.2f}"
            output.append(f"  {total_display:<73} {formatted_total:>15}")
    formatted_total_liabilities = f"${balance_sheet['Liabilities']['Total']['usd']:,.2f}"
    output.append(f"{'Total Liabilities':<80} {formatted_total_liabilities:>15}")

    # Equity
    output.append(f"\n{'Equity':^100}")
    output.append("-" * 5)
    for account_name, sub_accounts in balance_sheet["Equity"].items():
        if account_name != "Total":
            account_display = truncate_text(account_name, 80)
            output.append(f"{account_display:<80}")
            for sub, balance in sub_accounts.items():
                if sub != "Total":
                    sub_display = truncate_text(sub, 70)
                    formatted_balance = f"${balance['usd']:,.2f}"
                    output.append(f"    {sub_display:<70} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 73)}"
            formatted_total = f"${sub_accounts['Total']['usd']:,.2f}"
            output.append(f"  {total_display:<73} {formatted_total:>15}")
    formatted_total_equity = f"${balance_sheet['Equity']['Total']['usd']:,.2f}"
    output.append(f"{'Total Equity':<80} {formatted_total_equity:>15}")

    # Total Liabilities and Equity
    output.append(f"\n{'Total Liabilities and Equity':^100}")
    output.append("-" * 5)
    formatted_total = f"${balance_sheet['Total Liabilities and Equity']['usd']:,.2f}"
    output.append(f"{'':<80} {formatted_total:>15}")

    return "\n".join(output)


# Format all currencies as a table
def format_all_currencies(balance_sheet: Dict) -> str:
    """
    Format a table with balances in SATS, HIVE, HBD, USD.
    Returns a string table for reference.
    """
    max_width = 106
    output = ["Balance Sheet in All Currencies"]
    output.append("-" * max_width)
    output.append(f"{'Account':<30} {'Sub':<20} {'SATS':>12} {'HIVE':>12} {'HBD':>12} {'USD':>12}")
    output.append("-" * max_width)

    for category in ["Assets", "Liabilities", "Equity"]:
        output.append(f"\n{category}")
        output.append("-" * 30)
        for account_name, sub_accounts in balance_sheet[category].items():
            if account_name != "Total":
                for sub, balance in sub_accounts.items():
                    if sub != "Total":
                        output.append(
                            f"{truncate_text(account_name, 30):<30} "
                            f"{truncate_text(sub, 20):<20} "
                            f"{balance['msats'] / 1_000:>12,.0f} "
                            f"{balance['hive']:>12,.2f} "
                            f"{balance['hbd']:>12,.2f} "
                            f"{balance['usd']:>12,.2f}"
                        )
                total = sub_accounts["Total"]
                output.append(
                    f"{'Total ' + truncate_text(account_name, 24):<30} "
                    f"{'':<20} "
                    f"{total['msats'] / 1_000:>12,.0f} "
                    f"{total['hive']:>12,.2f} "
                    f"{total['hbd']:>12,.2f} "
                    f"{total['usd']:>12,.2f}"
                )
        total = balance_sheet[category]["Total"]
        output.append("-" * max_width)
        output.append(
            f"{'Total ' + category:<30} "
            f"{'':<20} "
            f"{total['msats'] / 1_000:>12,.0f} "
            f"{total['hive']:>12,.2f} "
            f"{total['hbd']:>12,.2f} "
            f"{total['usd']:>12,.2f}"
        )
        output.append("-" * max_width)

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * max_width)
    output.append(
        f"{'Total Liab. & Equity':<30} "
        f"{'':<20} "
        f"{total['msats'] / 1_000:>12,.0f} "
        f"{total['hive']:>12,.2f} "
        f"{total['hbd']:>12,.2f} "
        f"{total['usd']:>12,.2f}"
    )
    output.append("=" * max_width)
    return "\n".join(output)


# # Function to generate balance sheet
# async def generate_balance_sheet(as_of_date: datetime = datetime.now(tz=timezone.utc)) -> Dict:
#     """
#     Generate a balance sheet with sub-accounts as of a given date.
#     Returns a dictionary with Assets, Liabilities, and Equity balances in USD.
#     """
#     # Initialize dictionaries to track balances
#     ledger_entries = await get_ledger_entries(as_of_date=as_of_date)

#     account_balances = defaultdict(float)

#     # Process each ledger entry
#     for entry in ledger_entries:
#         if entry.timestamp > as_of_date:
#             continue  # Skip entries after the as_of_date

#         # Get USD amount from conv.usd
#         usd_amount = entry.conv.hive

#         # Update debit account
#         debit_key = (entry.debit.name, entry.debit.sub)
#         if entry.debit.account_type == "Asset":
#             account_balances[debit_key] += usd_amount
#         elif entry.debit.account_type in ["Liability", "Equity"]:
#             account_balances[debit_key] -= usd_amount

#         # Update credit account
#         credit_key = (entry.credit.name, entry.credit.sub)
#         if entry.credit.account_type == "Asset":
#             account_balances[credit_key] -= usd_amount
#         elif entry.credit.account_type in ["Liability", "Equity"]:
#             account_balances[credit_key] += usd_amount

#     # Organize balances by account type and main account
#     balance_sheet = {
#         "Assets": defaultdict(dict),
#         "Liabilities": defaultdict(dict),
#         "Equity": defaultdict(dict),
#     }

#     # Assign balances to appropriate categories
#     for (account_name, sub), balance in account_balances.items():
#         if account_name in [
#             "Customer Deposits Hive",
#             "Customer Deposits Lightning",
#             "Treasury Hive",
#             "Treasury Lightning",
#         ]:
#             balance_sheet["Assets"][account_name][sub] = round(balance, 2)
#         elif account_name in [
#             "Customer Liability Hive",
#             "Customer Liability Lightning",
#             "Tax Liabilities",
#             "Owner Loan Payable (funding)",
#         ]:
#             balance_sheet["Liabilities"][account_name][sub] = round(balance, 2)
#         elif account_name in ["Owner's Capital", "Retained Earnings", "Dividends/Distributions"]:
#             balance_sheet["Equity"][account_name][sub] = round(balance, 2)

#     # Calculate main account totals and overall totals
#     for category in ["Assets", "Liabilities", "Equity"]:
#         for account_name in balance_sheet[category]:
#             total = sum(balance_sheet[category][account_name].values())
#             balance_sheet[category][account_name]["Total"] = round(total, 2)
#         balance_sheet[category]["Total"] = round(
#             sum(
#                 account["Total"]
#                 for account in balance_sheet[category].values()
#                 if "Total" in account
#             ),
#             2,
#         )

#     balance_sheet["Total Liabilities and Equity"] = round(
#         balance_sheet["Liabilities"]["Total"] + balance_sheet["Equity"]["Total"], 2
#     )

#     return balance_sheet


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


def get_account_balance(df: pd.DataFrame, account_name: str, sub_account: str = None) -> str:
    """
    Calculate the balance for a single account, separated by unit (HIVE, HBD).
    Returns a formatted string with total amount and converted values per unit (SATS, MSATS, HIVE, HBD, USD).
    """
    # Filter transactions for the account (debit or credit)
    max_width = 87
    debit_df = df[df["debit_name"] == account_name].copy()
    credit_df = df[df["credit_name"] == account_name].copy()

    # Apply sub-account filter if provided
    if sub_account:
        debit_df = debit_df[debit_df["debit_sub"] == sub_account]
        credit_df = credit_df[credit_df["credit_sub"] == sub_account]

    # For assets: debits increase balance, credits decrease balance
    debit_df["signed_amount"] = debit_df["amount"]
    credit_df["signed_amount"] = -credit_df["amount"]

    # Combine debits and credits
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)

    # Group by unit and sum amounts
    balance_df = (
        combined_df.groupby("unit")
        .agg(
            {
                "signed_amount": "sum",
                "conv_hive": "last",  # Use latest conversion rates
                "conv_hbd": "last",
                "conv_usd": "last",
                "conv_sats": "last",
                "conv_msats": "last",
            }
        )
        .reset_index()
    )

    # Rename columns for clarity
    balance_df = balance_df.rename(
        columns={
            "signed_amount": "total_amount",
            "conv_hive": "total_hive",
            "conv_hbd": "total_hbd",
            "conv_usd": "total_usd",
            "conv_sats": "total_sats",
            "conv_msats": "total_msats",
        }
    )

    # Adjust conversion values to reflect total_amount
    for index, row in balance_df.iterrows():
        if row["total_amount"] != 0:  # Avoid division by zero for zero balances
            if row["unit"] == "hive":
                factor = row["total_amount"] / (row["total_hive"] / row["total_hive"])  # Normalize
                balance_df.at[index, "total_hive"] = row["total_amount"]
                balance_df.at[index, "total_hbd"] = row["total_hbd"] * factor
                balance_df.at[index, "total_usd"] = row["total_usd"] * factor
                balance_df.at[index, "total_sats"] = row["total_sats"] * factor
                balance_df.at[index, "total_msats"] = row["total_msats"] * factor
            elif row["unit"] == "hbd":
                factor = row["total_amount"] / (row["total_hbd"] / row["total_hbd"])
                balance_df.at[index, "total_hive"] = row["total_hive"] * factor
                balance_df.at[index, "total_hbd"] = row["total_amount"]
                balance_df.at[index, "total_usd"] = row["total_usd"] * factor
                balance_df.at[index, "total_sats"] = row["total_sats"] * factor
                balance_df.at[index, "total_msats"] = row["total_msats"] * factor

    # Format output as string
    output = [f"Balance for {account_name} (sub: {sub_account if sub_account else 'All'})"]
    output.append("-" * max_width)
    output.append(
        f"{'Unit':<10} {'Amount':>10} {'HIVE':>12} {'HBD':>12} {'USD':>12} {'SATS':>12} {'MSATS':>12}"
    )
    output.append("-" * max_width)

    if balance_df.empty or balance_df["total_amount"].eq(0).all():
        output.append("No non-zero balances for this account.")
    else:
        for _, row in balance_df.iterrows():
            if row["total_amount"] != 0:  # Only show non-zero balances
                output.append(
                    f"{row['unit'].upper():<10} "
                    f"{row['total_amount']:>10,.2f} "
                    f"{row['total_hive']:>12,.2f} "
                    f"{row['total_hbd']:>12,.2f} "
                    f"{row['total_usd']:>12,.2f} "
                    f"{row['total_sats']:>12,.0f} "
                    f"{row['total_msats'] / 1_000:>12,.0f}"
                )
    output.append("=" * max_width)

    return "\n".join(output)


async def list_all_accounts() -> List[dict[str]]:
    pipeline = [
        {
            "$project": {
                "accounts": [
                    {
                        "account_type": "$debit.account_type",
                        "name": "$debit.name",
                        "sub": "$debit.sub",
                    },
                    {
                        "account_type": "$credit.account_type",
                        "name": "$credit.name",
                        "sub": "$credit.sub",
                    },
                ]
            }
        },
        {"$unwind": "$accounts"},
        {
            "$group": {
                "_id": {
                    "account_type": "$accounts.account_type",
                    "name": "$accounts.name",
                    "sub": "$accounts.sub",
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "account_type": "$_id.account_type",
                "name": "$_id.name",
                "sub": "$_id.sub",
            }
        },
        {"$sort": {"account_type": 1, "name": 1, "sub": 1}},
    ]

    collection = await TrackedBaseModel.db_client.get_collection("ledger")
    cursor = collection.aggregate(pipeline=pipeline)
    accounts = []
    async for account in cursor:
        accounts.append(account)
    return accounts
