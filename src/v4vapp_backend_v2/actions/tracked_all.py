import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

import pandas as pd

from v4vapp_backend_v2.accounting.account_type import AssetAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAny, op_any
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

    # Check if a ledger entry with the same group_id already exists
    if TrackedBaseModel.db_client:
        existing_entry = await TrackedBaseModel.db_client.find_one(
            collection_name=LedgerEntry.collection(), query={"group_id": op.group_id}
        )
        if existing_entry:
            logger.warning(f"Ledger entry for group_id {op.group_id} already exists. Skipping.")
            await op.unlock_op()
            return None  # Skip processing if duplicate

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
    # MARK: Transfers or Recurrent Transfers
    if isinstance(op, TransferBase):
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
        else:
            logger.info(
                f"Transfer between two different accounts: {op.from_account} -> {op.to_account}"
            )

    if ledger_entry and ledger_entry.debit and ledger_entry.credit and TrackedBaseModel.db_client:
        try:
            await TrackedBaseModel.db_client.insert_one(
                collection_name=LedgerEntry.collection(),
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
    Retrieves ledger entries from the database up to a specified date.
    Args:
        as_of_date (datetime, optional): The cutoff date for retrieving ledger entries.
            Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query.
            Defaults to "ledger".
    Returns:
        list[LedgerEntry]: A list of LedgerEntry objects representing the ledger entries
            retrieved from the database.
    Notes:
        - The function queries the database for entries with a timestamp less than or
          equal to the specified `as_of_date`.
        - The database query uses a projection to include specific fields in the result.
        - Each database entry is validated and converted into a `LedgerEntry` object
          before being added to the result list.
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
    Fetches ledger entries from the database as of a specified date and returns them as a pandas DataFrame.
    Args:
        as_of_date (datetime, optional): The cutoff date for fetching ledger entries. Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query. Defaults to "ledger".
    Returns:
        pd.DataFrame: A DataFrame containing ledger entry data with the following columns:
            - timestamp: The timestamp of the ledger entry.
            - group_id: The group ID associated with the ledger entry.
            - description: A description of the ledger entry.
            - amount: The amount of the ledger entry.
            - unit: The unit of the amount.
            - conv_sats: The conversion value in satoshis.
            - conv_msats: The conversion value in millisatoshis.
            - conv_hive: The conversion value in Hive cryptocurrency.
            - conv_hbd: The conversion value in Hive Backed Dollars (HBD).
            - conv_usd: The conversion value in US Dollars (USD).
            - debit_name: The name of the debit account.
            - debit_account_type: The type of the debit account.
            - debit_sub: The sub-account of the debit account.
            - credit_name: The name of the credit account.
            - credit_account_type: The type of the credit account.
            - credit_sub: The sub-account of the credit account.
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
    # Initialize DataFrame for native units
    combined_df = pd.DataFrame()

    # Process debits in native units
    debit_df = df[["debit_name", "debit_account_type", "debit_sub", "amount", "unit"]].copy()
    debit_df["amount_adj"] = debit_df.apply(
        lambda row: row["amount"] if row["debit_account_type"] == "Asset" else -row["amount"],
        axis=1,
    )
    debit_df = debit_df.rename(
        columns={"debit_name": "name", "debit_account_type": "account_type", "debit_sub": "sub"}
    )

    # Process credits in native units
    credit_df = df[["credit_name", "credit_account_type", "credit_sub", "amount", "unit"]].copy()
    credit_df["amount_adj"] = credit_df.apply(
        lambda row: -row["amount"] if row["credit_account_type"] == "Asset" else row["amount"],
        axis=1,
    )
    credit_df = credit_df.rename(
        columns={"credit_name": "name", "credit_account_type": "account_type", "credit_sub": "sub"}
    )

    # Combine debits and credits
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)

    # Aggregate by name, sub, and unit
    balance_df = (
        combined_df.groupby(["name", "sub", "account_type", "unit"])["amount_adj"]
        .sum()
        .reset_index()
    )

    # Initialize balance sheet
    balance_sheet = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    # Assign balances to categories in native units
    for _, row in balance_df.iterrows():
        name, sub, account_type, unit = row["name"], row["sub"], row["account_type"], row["unit"]
        amount = row["amount_adj"]
        category = (
            "Assets"
            if account_type == "Asset"
            else "Liabilities"
            if account_type == "Liability"
            else "Equity"
        )
        if sub not in balance_sheet[category][name]:
            balance_sheet[category][name][sub] = {"hive": 0.0, "hbd": 0.0}
        if unit == "hive":
            balance_sheet[category][name][sub]["hive"] += amount
        elif unit == "hbd":
            balance_sheet[category][name][sub]["hbd"] += amount

    # Apply conversions to totals using the latest conversion rates
    latest_conv = df.iloc[-1][["conv_sats", "conv_msats", "conv_hive", "conv_hbd", "conv_usd"]]
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            for sub in balance_sheet[category][account_name]:
                if sub == "Total":
                    continue
                hive = balance_sheet[category][account_name][sub]["hive"]
                hbd = balance_sheet[category][account_name][sub]["hbd"]
                # Convert to other currencies using the latest rates
                # (Simplified; use actual conversion logic based on conv fields)
                total_hive = hive + (hbd * (latest_conv["conv_hive"] / latest_conv["conv_hbd"]))
                balance_sheet[category][account_name][sub] = {
                    "sats": round(
                        total_hive * (latest_conv["conv_sats"] / latest_conv["conv_hive"]), 0
                    ),
                    "msats": round(
                        total_hive * (latest_conv["conv_msats"] / latest_conv["conv_hive"]), 0
                    ),
                    "hive": round(total_hive, 2),
                    "hbd": round(
                        total_hive * (latest_conv["conv_hbd"] / latest_conv["conv_hive"]), 2
                    ),
                    "usd": round(
                        total_hive * (latest_conv["conv_usd"] / latest_conv["conv_hive"]), 2
                    ),
                }

    # Calculate totals (same as before)
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            total_sats = sum(
                sub_acc["sats"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "sats" in sub_acc
            )
            total_msats = sum(
                sub_acc["msats"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "msats" in sub_acc
            )
            total_hive = sum(
                sub_acc["hive"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "hive" in sub_acc
            )
            total_hbd = sum(
                sub_acc["hbd"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "hbd" in sub_acc
            )
            total_usd = sum(
                sub_acc["usd"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "usd" in sub_acc
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
            "sats": round(total_sats, 0),
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
def balance_sheet_printout(balance_sheet: Dict, as_of_date: datetime) -> str:
    """
    This function takes a balance sheet dictionary and a date, and formats the balance sheet into a
    readable string representation. The output includes sections for Assets, Liabilities, and Equity,
    along with their respective totals. The total liabilities and equity are also displayed at the end.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data. It should have the following structure:
            {
                "Assets": {
                    "Account Name": {
                        "Sub-account Name": {"usd": float},
                        "Total": {"usd": float}
                    },
                    "Total": {"usd": float}
                },
                "Liabilities": {
                    "Account Name": {
                        "Sub-account Name": {"usd": float},
                        "Total": {"usd": float}
                    },
                    "Total": {"usd": float}
                },
                "Equity": {
                    "Account Name": {
                        "Sub-account Name": {"usd": float},
                        "Total": {"usd": float}
                    },
                    "Total": {"usd": float}
                },
                "Total Liabilities and Equity": {"usd": float}
            }
        as_of_date (datetime): The date for which the balance sheet is being formatted.

    Returns:
        str: A formatted string representation of the balance sheet, with each section and total aligned
        within a maximum width of 100 characters.
    """
    output = []
    max_width = 100
    date_str = as_of_date.strftime("%Y-%m-%d")
    header = f"Balance Sheet as of {date_str}"
    output.append(f"{truncate_text(header, max_width, centered=True)}")
    output.append("-" * max_width)

    # Assets
    heading = truncate_text("Assets", max_width, centered=True)
    output.append(f"{heading}")
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
    heading = truncate_text("Liabilities", max_width, centered=True)
    output.append(f"\n{heading}")
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
    heading = truncate_text("Equity", max_width, centered=True)
    output.append(f"\n{heading}")
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
    heading = truncate_text("Total Liabilities and Equity", max_width, centered=True)
    output.append(f"\n{heading}")
    output.append("-" * 5)
    formatted_total = f"${balance_sheet['Total Liabilities and Equity']['usd']:,.2f}"
    output.append(f"{'':<80} {formatted_total:>15}")

    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["usd"],
        balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
        rel_tol=0.01,
    )
    if is_balanced:
        output.append(f"\n{'The balance sheet is balanced.':^100}")
    else:
        output.append(f"\n{'******* The balance sheet is NOT balanced. ********':^100}")

    return "\n".join(output)


# Format all currencies as a table
def balance_sheet_all_currencies_printout(balance_sheet: Dict) -> str:
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

    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["usd"],
        balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
        rel_tol=0.01,
    )
    if is_balanced:
        output.append(f"\n{'The balance sheet is balanced.':^100}")
    else:
        output.append(f"\n{'******* The balance sheet is NOT balanced. ********':^100}")

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


def truncate_text(text: str, max_length: int, centered: bool = False) -> str:
    """
    Truncates a given text to a specified maximum length, optionally centering it.

    If the text exceeds the maximum length, it is truncated and appended with '...'.
    If the `centered` parameter is set to True, the truncated text is centered within
    the specified maximum length.

    Args:
        text (str): The input text to be truncated.
        max_length (int): The maximum allowed length of the text, including the ellipsis.
        centered (bool, optional): Whether to center the truncated text. Defaults to False.

    Returns:
        str: The truncated (and optionally centered) text.
    """
    if centered:
        text = text[: max_length - 3] + "..." if len(text) > max_length else text
        return text.center(max_length)
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
    max_width = 95
    # Filter transactions for the account (debit or credit)
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

    # Group by unit and aggregate
    balance_df = (
        combined_df.groupby("unit")
        .agg(
            {
                "signed_amount": "sum",
                "amount": "last",  # Original amount for scaling
                "conv_hive": "last",
                "conv_hbd": "last",
                "conv_usd": "last",
                "conv_sats": "last",
                "conv_msats": "last",
                "timestamp": "max",  # Ensure latest transaction
            }
        )
        .reset_index()
    )

    # Rename columns for clarity
    balance_df = balance_df.rename(
        columns={
            "signed_amount": "total_amount",
            "conv_hive": "base_hive",
            "conv_hbd": "base_hbd",
            "conv_usd": "base_usd",
            "conv_sats": "base_sats",
            "conv_msats": "base_msats",
        }
    )

    # Adjust conversion values to reflect total_amount
    balance_df["total_hive"] = 0.0
    balance_df["total_hbd"] = 0.0
    balance_df["total_usd"] = 0.0
    balance_df["total_sats"] = 0.0
    balance_df["total_msats"] = 0.0

    for index, row in balance_df.iterrows():
        if row["total_amount"] != 0:  # Avoid division by zero
            # Calculate scaling factor based on original transaction amount
            factor = row["total_amount"] / row["amount"]
            balance_df.at[index, "total_hive"] = row["base_hive"] * factor
            balance_df.at[index, "total_hbd"] = row["base_hbd"] * factor
            balance_df.at[index, "total_usd"] = row["base_usd"] * factor
            balance_df.at[index, "total_sats"] = row["base_sats"] * factor
            balance_df.at[index, "total_msats"] = row["base_msats"] * factor

    # Format output as string
    title_line = f"Balance for {account_name} (sub: {sub_account if sub_account else 'All'})"
    output = [title_line]
    output.append("-" * max_width)
    output.append(
        f"{'Unit':<10} {'Amount':>10} {'HIVE':>12} {'HBD':>12} {'USD':>12} {'SATS':>12} {'MSATS':>16}"
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
                    f"{row['total_msats']:>16,.0f}"
                )

    usd_total = balance_df["total_usd"].sum()
    sats_total = balance_df["total_sats"].sum()
    output.append(title_line)
    output.append(f"Total USD: {usd_total:>19,.2f}")
    output.append(f"Total SATS: {sats_total:>15,.0f}")
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
    return accounts
