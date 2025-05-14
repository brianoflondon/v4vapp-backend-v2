import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List

import pandas as pd

from v4vapp_backend_v2.accounting.account_type import Account
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text


async def get_ledger_entries(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "ledger",
    filter_by_account: Account | None = None,
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
    query = {"timestamp": {"$lte": as_of_date}}

    # Add account filter if provided
    if filter_by_account:
        # Match entries where either debit or credit side matches the account name and sub-account
        account_filter = {
            "$or": [
                {
                    "debit.name": filter_by_account.name,
                    "debit.sub": filter_by_account.sub if filter_by_account.sub else "",
                },
                {
                    "credit.name": filter_by_account.name,
                    "credit.sub": filter_by_account.sub if filter_by_account.sub else "",
                },
            ]
        }
        query.update(account_filter)
    ledger_entries = []
    async with TrackedBaseModel.db_client as db_client:
        cursor = await db_client.find(
            collection_name=collection_name,
            query=query,
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
            sort=[("timestamp", 1)],
        )
        async for entry in cursor:
            ledger_entry = LedgerEntry.model_validate(entry)
            ledger_entries.append(ledger_entry)
    return ledger_entries


async def get_ledger_dataframe(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "ledger",
    filter_by_account: Account | None = None,
) -> pd.DataFrame:
    """
    Fetches ledger entries from the database as of a specified date and returns them as a pandas DataFrame.
    Args:
        as_of_date (datetime, optional): The cutoff date for fetching ledger entries. Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query. Defaults to "ledger".
        filter_by_account (Account | None, optional): The account to filter by. Defaults to None.
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
        as_of_date=as_of_date, collection_name=collection_name, filter_by_account=filter_by_account
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


async def generate_balance_sheet_pandas(
    df: pd.DataFrame = pd.DataFrame(), reporting_date: datetime = None
) -> Dict:
    """
    Generates a GAAP-compliant balance sheet in USD, with supplemental columns for HIVE, HBD, SATS, and msats.
    Includes proper CTA calculation.

    Args:
        df (pd.DataFrame): Ledger entries DataFrame with columns: timestamp, amount, unit, conv_usd, etc.
        reporting_date (datetime, optional): The balance sheet date for exchange rate translations.
            If None, uses the timestamp of the most recent entry in the DataFrame.

    Returns:
        Dict: Balance sheet with primary values in USD and supplemental values in HIVE, HBD, SATS, and msats.
    """
    # Step 1: Determine the reporting date and spot rates
    if df.empty:
        df = await get_ledger_dataframe(
            as_of_date=reporting_date if reporting_date else datetime.now(tz=timezone.utc)
        )

    if df.empty:
        return {
            "Assets": defaultdict(dict),
            "Liabilities": defaultdict(dict),
            "Equity": defaultdict(dict),
            "Total Liabilities and Equity": {
                "usd": 0.0,
                "hive": 0.0,
                "hbd": 0.0,
                "sats": 0.0,
                "msats": 0.0,
            },
        }

    # Get the most recent entry (df is already sorted by timestamp, earliest to latest)
    latest_entry = df.iloc[-1]  # Last row is the most recent
    if reporting_date is None:
        reporting_date = latest_entry["timestamp"]

    # Derive spot rates from the most recent entry
    conv_usd = latest_entry["conv_usd"]
    conv_hbd = latest_entry["conv_hbd"]
    conv_hive = latest_entry["conv_hive"]
    conv_sats = latest_entry["conv_sats"]
    conv_msats = latest_entry["conv_msats"]

    spot_rates = {
        "hbd_to_usd": conv_usd / conv_hbd if conv_hbd != 0 else 1.0,
        "hive_to_usd": conv_usd / conv_hive if conv_hive != 0 else 0.0,
        "sats_to_usd": conv_usd / conv_sats if conv_sats != 0 else 0.0,
        "msats_to_usd": conv_usd / conv_msats if conv_msats != 0 else 0.0,
    }

    # Step 2: Sum amounts in native units and historical USD
    combined_df = pd.DataFrame()

    # Process debits
    debit_df = df[
        ["debit_name", "debit_account_type", "debit_sub", "amount", "unit", "conv_usd"]
    ].copy()
    debit_df["amount_adj"] = debit_df.apply(
        lambda row: row["amount"] if row["debit_account_type"] == "Asset" else -row["amount"],
        axis=1,
    )
    debit_df["usd_adj"] = debit_df.apply(
        lambda row: row["conv_usd"] if row["debit_account_type"] == "Asset" else -row["conv_usd"],
        axis=1,
    )
    debit_df = debit_df.rename(
        columns={"debit_name": "name", "debit_account_type": "account_type", "debit_sub": "sub"}
    )

    # Process credits
    credit_df = df[
        ["credit_name", "credit_account_type", "credit_sub", "amount", "unit", "conv_usd"]
    ].copy()
    credit_df["amount_adj"] = credit_df.apply(
        lambda row: -row["amount"] if row["credit_account_type"] == "Asset" else row["amount"],
        axis=1,
    )
    credit_df["usd_adj"] = credit_df.apply(
        lambda row: -row["conv_usd"] if row["credit_account_type"] == "Asset" else row["conv_usd"],
        axis=1,
    )
    credit_df = credit_df.rename(
        columns={"credit_name": "name", "credit_account_type": "account_type", "credit_sub": "sub"}
    )

    # Combine and aggregate by native unit and historical USD
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)
    balance_df = (
        combined_df.groupby(["name", "sub", "account_type", "unit"])
        .agg({"amount_adj": "sum", "usd_adj": "sum"})
        .reset_index()
    )

    # Step 3: Initialize balance sheet
    balance_sheet = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    # Step 4: Sum in native units and historical USD
    historical_usd = {
        "Assets": defaultdict(lambda: defaultdict(float)),
        "Liabilities": defaultdict(lambda: defaultdict(float)),
        "Equity": defaultdict(lambda: defaultdict(float)),
    }

    for _, row in balance_df.iterrows():
        name, sub, account_type, unit = row["name"], row["sub"], row["account_type"], row["unit"]
        amount = row["amount_adj"]
        usd_historical = row["usd_adj"]
        category = (
            "Assets"
            if account_type == "Asset"
            else "Liabilities"
            if account_type == "Liability"
            else "Equity"
        )
        if sub not in balance_sheet[category][name]:
            balance_sheet[category][name][sub] = {"hive": 0.0, "hbd": 0.0, "sats": 0.0}
        if unit == "hive":
            balance_sheet[category][name][sub]["hive"] += amount
        elif unit == "hbd":
            balance_sheet[category][name][sub]["hbd"] += amount
        elif unit == "sats":
            balance_sheet[category][name][sub]["sats"] += amount
        historical_usd[category][name][sub] += usd_historical

    # Step 5: Translate to USD and compute supplemental currencies
    translated_values = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            for sub in balance_sheet[category][account_name]:
                if sub == "Total":
                    continue
                hive = balance_sheet[category][account_name][sub]["hive"]
                hbd = balance_sheet[category][account_name][sub]["hbd"]
                sats = balance_sheet[category][account_name][sub]["sats"]

                # Translate to USD using spot rates at balance sheet date
                usd = (
                    hive * spot_rates["hive_to_usd"]
                    + hbd * spot_rates["hbd_to_usd"]
                    + sats * spot_rates["sats_to_usd"]
                )

                # Store translated values
                translated_values[category][account_name][sub] = {
                    "usd": round(usd, 2),
                    "hive": round(usd / spot_rates["hive_to_usd"], 2)
                    if spot_rates["hive_to_usd"] != 0
                    else 0.0,
                    "hbd": round(usd / spot_rates["hbd_to_usd"], 2)
                    if spot_rates["hbd_to_usd"] != 0
                    else 0.0,
                    "sats": round(usd / spot_rates["sats_to_usd"], 0)
                    if spot_rates["sats_to_usd"] != 0
                    else 0.0,
                    "msats": round(usd / spot_rates["msats_to_usd"], 0)
                    if spot_rates["msats_to_usd"] != 0
                    else 0.0,
                }

    # Step 6: Update balance sheet with translated values
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in translated_values[category]:
            for sub in translated_values[category][account_name]:
                balance_sheet[category][account_name][sub] = translated_values[category][
                    account_name
                ][sub]

    # Step 7: Calculate totals
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            total_usd = sum(
                sub_acc["usd"]
                for sub_acc in balance_sheet[category][account_name].values()
                if "usd" in sub_acc
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
            # Historical USD total for CTA calculation
            total_historical_usd = sum(
                historical_usd[category][account_name][sub]
                for sub in historical_usd[category][account_name]
            )
            balance_sheet[category][account_name]["Total"] = {
                "usd": round(total_usd, 2),
                "hive": round(total_hive, 2),
                "hbd": round(total_hbd, 2),
                "sats": round(total_sats, 0),
                "msats": round(total_msats, 0),
                "historical_usd": round(total_historical_usd, 2),
            }
        total_usd = sum(
            acc["Total"]["usd"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_hive = sum(
            acc["Total"]["hive"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_hbd = sum(
            acc["Total"]["hbd"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_sats = sum(
            acc["Total"]["sats"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_msats = sum(
            acc["Total"]["msats"] for acc in balance_sheet[category].values() if "Total" in acc
        )
        total_historical_usd = sum(
            acc["Total"]["historical_usd"]
            for acc in balance_sheet[category].values()
            if "Total" in acc
        )
        balance_sheet[category]["Total"] = {
            "usd": round(total_usd, 2),
            "hive": round(total_hive, 2),
            "hbd": round(total_hbd, 2),
            "sats": round(total_sats, 0),
            "msats": round(total_msats, 0),
            "historical_usd": round(total_historical_usd, 2),
        }

    # Step 8: Calculate CTA
    # CTA = (Translated USD at balance sheet date) - (Historical USD at transaction dates)
    total_assets_usd = balance_sheet["Assets"]["Total"]["usd"]
    total_liabilities_usd = balance_sheet["Liabilities"]["Total"]["usd"]
    total_equity_usd = balance_sheet["Equity"]["Total"]["usd"]

    total_assets_historical_usd = balance_sheet["Assets"]["Total"]["historical_usd"]
    total_liabilities_historical_usd = balance_sheet["Liabilities"]["Total"]["historical_usd"]
    total_equity_historical_usd = balance_sheet["Equity"]["Total"]["historical_usd"]

    # CTA for assets and liabilities
    cta = (
        (total_assets_usd - total_assets_historical_usd)
        - (total_liabilities_usd - total_liabilities_historical_usd)
        - (total_equity_usd - total_equity_historical_usd)
    )

    # Update CTA in Equity
    balance_sheet["Equity"]["CTA"]["default"] = {
        "usd": round(cta, 2),
        "hive": round(cta / spot_rates["hive_to_usd"], 2)
        if spot_rates["hive_to_usd"] != 0
        else 0.0,
        "hbd": round(cta / spot_rates["hbd_to_usd"], 2) if spot_rates["hbd_to_usd"] != 0 else 0.0,
        "sats": round(cta / spot_rates["sats_to_usd"], 0)
        if spot_rates["sats_to_usd"] != 0
        else 0.0,
        "msats": round(cta / spot_rates["msats_to_usd"], 0)
        if spot_rates["msats_to_usd"] != 0
        else 0.0,
    }

    # Recalculate Equity totals to include updated CTA
    total_usd = sum(
        sub_acc["usd"]
        for acc_name, acc in balance_sheet["Equity"].items()
        if acc_name != "Total"
        for sub, sub_acc in acc.items()
        if sub != "Total" and isinstance(sub_acc, dict) and "usd" in sub_acc
    )
    total_hive = sum(
        sub_acc["hive"]
        for acc_name, acc in balance_sheet["Equity"].items()
        if acc_name != "Total"
        for sub, sub_acc in acc.items()
        if sub != "Total" and isinstance(sub_acc, dict) and "hive" in sub_acc
    )
    total_hbd = sum(
        sub_acc["hbd"]
        for acc_name, acc in balance_sheet["Equity"].items()
        if acc_name != "Total"
        for sub, sub_acc in acc.items()
        if sub != "Total" and isinstance(sub_acc, dict) and "hbd" in sub_acc
    )
    total_sats = sum(
        sub_acc["sats"]
        for acc_name, acc in balance_sheet["Equity"].items()
        if acc_name != "Total"
        for sub, sub_acc in acc.items()
        if sub != "Total" and isinstance(sub_acc, dict) and "sats" in sub_acc
    )
    total_msats = sum(
        sub_acc["msats"]
        for acc_name, acc in balance_sheet["Equity"].items()
        if acc_name != "Total"
        for sub, sub_acc in acc.items()
        if sub != "Total" and isinstance(sub_acc, dict) and "msats" in sub_acc
    )
    balance_sheet["Equity"]["Total"] = {
        "usd": round(total_usd, 2),
        "hive": round(total_hive, 2),
        "hbd": round(total_hbd, 2),
        "sats": round(total_sats, 0),
        "msats": round(total_msats, 0),
    }

    balance_sheet["Total Liabilities and Equity"] = {
        "usd": round(
            balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
            2,
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
    }

    balance_sheet["is_balanced"] = check_balance_sheet(balance_sheet)
    if balance_sheet["is_balanced"]:
        logger.info(
            f"Balance Sheet is balanced. Assets {balance_sheet['Assets']['Total']['usd']} USD"
        )
    else:
        logger.warning("Balance Sheet is NOT balanced.")
        logger.warning(
            f"Assets: {balance_sheet['Assets']['Total']['usd']} != Liabilities + Equity: {balance_sheet['Liabilities']['Total']['usd']} + {balance_sheet['Equity']['Total']['usd']}",
            extra={"balance_sheet": balance_sheet},
        )
    return balance_sheet


def check_balance_sheet(
    balance_sheet: Dict,
) -> bool:
    """
    Checks if the balance sheet is balanced.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data with the following structure:
    """
    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["usd"],
        balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
        rel_tol=0.01,
    )
    return is_balanced


# Format balance sheet as string (USD, 100-char width)
def balance_sheet_printout(balance_sheet: Dict, as_of_date: datetime) -> str:
    """
    Formats the balance sheet into a readable string representation, displaying only USD values.
    Includes sections for Assets, Liabilities, and Equity, along with their respective totals.
    The total liabilities and equity are displayed at the end.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data with the following structure:
            {
                "Assets": {
                    "Account Name": {
                        "Sub-account Name": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float},
                        "Total": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float, "historical_usd": float}
                    },
                    "Total": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float, "historical_usd": float}
                },
                "Liabilities": {...},
                "Equity": {...},
                "Total Liabilities and Equity": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float}
            }
        as_of_date (datetime): The date for which the balance sheet is being formatted.

    Returns:
        str: A formatted string representation of the balance sheet, showing only USD values.
    """
    output = []
    max_width = 100
    date_str = as_of_date.strftime("%Y-%m-%d")
    header = f"Balance Sheet as of {date_str}"
    output.append(f"{truncate_text(header, max_width, centered=True)}")
    output.append("-" * max_width)

    for category in ["Assets", "Liabilities", "Equity"]:
        heading = truncate_text(category, max_width, centered=True)
        output.append(f"\n{heading}")
        output.append("-" * 5)
        for account_name, sub_accounts in balance_sheet[category].items():
            if account_name == "Total":
                continue
            # Check if all sub-account balances are zero (excluding "Total")
            all_zero = all(
                all(value == 0 for value in balance.values())
                for sub, balance in sub_accounts.items()
                if sub != "Total"
            )
            if all_zero:
                continue  # Skip accounts with all zero balances
            account_display = truncate_text(account_name, 80)
            output.append(f"{account_display:<80}")
            for sub, balance in sub_accounts.items():
                if sub == "Total":
                    continue
                sub_display = truncate_text(sub, 70)
                formatted_balance = f"${balance['usd']:,.2f}"
                output.append(f"    {sub_display:<70} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 73)}"
            formatted_total = f"${sub_accounts['Total']['usd']:,.2f}"
            output.append(f"  {total_display:<73} {formatted_total:>15}")
        formatted_total = f"${balance_sheet[category]['Total']['usd']:,.2f}"
        output.append(f"{'Total ' + category:<80} {formatted_total:>15}")

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


def balance_sheet_all_currencies_printout(balance_sheet: Dict) -> str:
    """
    Formats a table with balances in SATS, HIVE, HBD, and USD.
    Returns a string table for reference.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data with the following structure:
            {
                "Assets": {
                    "Account Name": {
                        "Sub-account Name": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float},
                        "Total": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float, "historical_usd": float}
                    },
                    "Total": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float, "historical_usd": float}
                },
                "Liabilities": {...},
                "Equity": {...},
                "Total Liabilities and Equity": {"usd": float, "hive": float, "hbd": float, "sats": float, "msats": float}
            }

    Returns:
        str: A formatted string table displaying balances in SATS, HIVE, HBD, and USD.
    """
    max_width = 110  # Adjusted width: 110 - 12 (msats column) = 98
    output = ["Balance Sheet in All Currencies"]
    output.append("-" * max_width)
    output.append(f"{'Account':<40} {'Sub':<17} {'SATS':>10} {'HIVE':>12} {'HBD':>12} {'USD':>12}")
    output.append("-" * max_width)

    for category in ["Assets", "Liabilities", "Equity"]:
        heading = truncate_text(category, max_width, centered=True)
        output.append(f"\n{heading}")
        output.append("-" * 30)
        for account_name, sub_accounts in balance_sheet[category].items():
            if account_name == "Total":
                continue
            # Check if all sub-account balances are zero (excluding "Total")
            all_zero = all(
                all(value == 0 for value in balance.values())
                for sub, balance in sub_accounts.items()
                if sub != "Total"
            )
            if all_zero:
                continue  # Skip accounts with all zero balances
            for sub, balance in sub_accounts.items():
                if sub == "Total":
                    continue
                output.append(
                    f"{truncate_text(account_name, 40):<40} "
                    f"{truncate_text(sub, 17):<17} "
                    f"{balance['sats']:>10,.0f} "
                    f"{balance['hive']:>12,.2f} "
                    f"{balance['hbd']:>12,.2f} "
                    f"{balance['usd']:>12,.2f}"
                )
            # Calculate total if "Total" key is missing
            if "Total" not in sub_accounts:
                total_usd = sum(
                    sub_acc["usd"]
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "usd" in sub_acc
                )
                total_hive = sum(
                    sub_acc["hive"]
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "hive" in sub_acc
                )
                total_hbd = sum(
                    sub_acc["hbd"]
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "hbd" in sub_acc
                )
                total_sats = sum(
                    sub_acc["sats"]
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "sats" in sub_acc
                )
                sub_accounts["Total"] = {
                    "usd": round(total_usd, 2),
                    "hive": round(total_hive, 2),
                    "hbd": round(total_hbd, 2),
                    "sats": round(total_sats, 0),
                    "msats": 0,  # Not used in display, but included for consistency
                }
            total = sub_accounts["Total"]
            output.append(
                f"{'Total ' + truncate_text(account_name, 35):<40} "
                f"{'':<17} "
                f"{total['sats']:>10,.0f} "
                f"{total['hive']:>12,.2f} "
                f"{total['hbd']:>12,.2f} "
                f"{total['usd']:>12,.2f}"
            )
        total = balance_sheet[category]["Total"]
        output.append("-" * max_width)
        output.append(
            f"{'Total ' + category:<40} "
            f"{'':<17} "
            f"{total['sats']:>10,.0f} "
            f"{total['hive']:>12,.2f} "
            f"{total['hbd']:>12,.2f} "
            f"{total['usd']:>12,.2f}"
        )
        output.append("-" * max_width)

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * max_width)
    output.append(
        f"{'Total Liab. & Equity':<40} "
        f"{'':<17} "
        f"{total['sats']:>10,.0f} "
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
        output.append(f"\n{'The balance sheet is balanced.':^94}")
    else:
        output.append(f"\n{'******* The balance sheet is NOT balanced. ********':^94}")

    output.append("=" * max_width)

    return "\n".join(output)


async def get_account_balance(
    account: Account,
    df: pd.DataFrame = pd.DataFrame(),
    full_history: bool = False,
    as_of_date: datetime = None,
) -> pd.DataFrame:
    """
    Calculate and display the balance for a specified account (and optional sub-account) from the DataFrame.
    Optionally lists all debit and credit transactions up to today, or shows only the closing balance.
    Properly accounts for assets and liabilities, and includes converted values to other units
    (SATS, HIVE, HBD, USD, msats).

    Args:
        df (pd.DataFrame): A DataFrame containing transaction data with the following columns:
            - timestamp: Timestamp of the transaction.
            - description: Description of the transaction.
            - debit_name: Name of the debit account.
            - credit_name: Name of the credit account.
            - debit_sub: Sub-account for the debit account.
            - credit_sub: Sub-account for the credit account.
            - debit_account_type: Type of the debit account (Asset, Liability, etc.).
            - credit_account_type: Type of the credit account (Asset, Liability, etc.).
            - amount: Transaction amount.
            - unit: Unit of the transaction (e.g., HIVE, HBD, SATS).
            - conv_hive: Conversion value to HIVE.
            - conv_hbd: Conversion value to HBD.
            - conv_usd: Conversion value to USD.
            - conv_sats: Conversion value to SATS.
            - conv_msats: Conversion value to MSATS.
        account (Account): An Account object specifying the account name, type, and optional sub-account.
        full_history (bool, optional): If True, shows the full transaction history with running balances.
                                       If False, shows only the closing balance. Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing either the full transaction history or the closing balance
                for the specified account and sub-account up as_of_date.
    """

    if df.empty:
        df = await get_ledger_dataframe(
            as_of_date=as_of_date,
            filter_by_account=account,
        )

    # Filter transactions for the account (debit or credit)
    debit_df = df[df["debit_name"] == account.name].copy()
    credit_df = df[df["credit_name"] == account.name].copy()

    # Apply sub-account filter if provided
    if account.sub:
        debit_df = debit_df[debit_df["debit_sub"] == account.sub]
        credit_df = credit_df[credit_df["credit_sub"] == account.sub]

    # Prepare debit and credit DataFrames
    debit_df = debit_df[
        [
            "timestamp",
            "description",
            "amount",
            "unit",
            "conv_hive",
            "conv_hbd",
            "conv_usd",
            "conv_sats",
            "conv_msats",
        ]
    ].copy()
    credit_df = credit_df[
        [
            "timestamp",
            "description",
            "amount",
            "unit",
            "conv_hive",
            "conv_hbd",
            "conv_usd",
            "conv_sats",
            "conv_msats",
        ]
    ].copy()

    # Add debit/credit columns and signed amounts
    debit_df["debit"] = debit_df["amount"]
    debit_df["credit"] = 0.0
    credit_df["debit"] = 0.0
    credit_df["credit"] = credit_df["amount"]

    # Determine signed amounts based on account type
    # For Assets: Debits increase (+), Credits decrease (-)
    # For Liabilities: Credits increase (+), Debits decrease (-)
    if account.account_type == "Asset":
        debit_df["signed_amount"] = debit_df["amount"]
        credit_df["signed_amount"] = -credit_df["amount"]
    else:  # Liability
        debit_df["signed_amount"] = -debit_df["amount"]
        credit_df["signed_amount"] = credit_df["amount"]

    # Combine debits and credits
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)

    # Sort by timestamp to ensure chronological order (should already be sorted, but ensures correctness)
    combined_df = combined_df.sort_values(by="timestamp").reset_index(drop=True)

    return combined_df


async def get_account_balance_printout(
    account: Account,
    df: pd.DataFrame = pd.DataFrame(),
    full_history: bool = False,
    as_of_date: datetime = None,
) -> str:
    """
    Calculate and display the balance for a specified account (and optional sub-account) from the DataFrame.
    Optionally lists all debit and credit transactions up to today, or shows only the closing balance.
    Properly accounts for assets and liabilities, and includes converted values to other units
    (SATS, HIVE, HBD, USD, msats).

    Args:
        df (pd.DataFrame): A DataFrame containing transaction data with the following columns:
            - timestamp: Timestamp of the transaction.
            - description: Description of the transaction.
            - debit_name: Name of the debit account.
            - credit_name: Name of the credit account.
            - debit_sub: Sub-account for the debit account.
            - credit_sub: Sub-account for the credit account.
            - debit_account_type: Type of the debit account (Asset, Liability, etc.).
            - credit_account_type: Type of the credit account (Asset, Liability, etc.).
            - amount: Transaction amount.
            - unit: Unit of the transaction (e.g., HIVE, HBD, SATS).
            - conv_hive: Conversion value to HIVE.
            - conv_hbd: Conversion value to HBD.
            - conv_usd: Conversion value to USD.
            - conv_sats: Conversion value to SATS.
            - conv_msats: Conversion value to MSATS.
        account (Account): An Account object specifying the account name, type, and optional sub-account.
        full_history (bool, optional): If True, shows the full transaction history with running balances.
                                       If False, shows only the closing balance. Defaults to False.

    Returns:
        str: A formatted string containing either the full transaction history or the closing balance
             for the specified account and sub-account up to today (May 14, 2025, 18:35 IDT).
             The output includes:
             - (If full_history=True) Timestamp, description, debit amount, credit amount, and running balance for each transaction.
             - Separate sections for each unit (e.g., HIVE, HBD, SATS).
             - Converted values to HIVE, HBD, USD, SATS, and msats for the final balance per unit.
             - Total USD and SATS values across all units.
    """
    max_width = 110

    combined_df = await get_account_balance(
        account=account,
        df=df,
        full_history=full_history,
        as_of_date=as_of_date,
    )

    # Group by unit to process each unit separately
    units = combined_df["unit"].unique()
    title_line = f"Balance for {account.name} (sub: {account.sub if account.sub else 'All'}) {account.account_type}"
    output = ["=" * max_width]
    output.append(title_line)
    output.append("-" * max_width)

    if combined_df.empty:
        output.append("No transactions found for this account up to today.")
        output.append("=" * max_width)
        return "\n".join(output)

    total_usd = 0.0
    total_sats = 0.0

    for unit in units:
        unit_df = combined_df[combined_df["unit"] == unit].copy()
        if unit_df.empty:
            continue

        # Calculate running balance
        unit_df["running_balance"] = unit_df["signed_amount"].cumsum()

        # Format output for this unit
        output.append(f"\nUnit: {unit.upper()}")
        output.append("-" * 10)
        if full_history:
            output.append(
                f"{'Timestamp':<20} {'Description':<45} {'Debit':>12} {'Credit':>12} {'Balance':>12}"
            )
            output.append("-" * 10)
            for _, row in unit_df.iterrows():
                timestamp = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                description = truncate_text(row["description"], 45)
                debit = row["debit"]
                credit = row["credit"]
                balance = row["running_balance"]
                output.append(
                    f"{timestamp:<20} "
                    f"{description:<45} "
                    f"{debit:>12,.2f} "
                    f"{credit:>12,.2f} "
                    f"{balance:>12,.2f}"
                )

        # Get the final balance for this unit and calculate converted values
        final_balance = unit_df["running_balance"].iloc[-1]
        if final_balance != 0:
            # Use the conversion rates from the latest transaction for this unit
            latest_row = unit_df.iloc[-1]
            factor = final_balance / latest_row["amount"]
            total_hive = latest_row["conv_hive"] * factor
            total_hbd = latest_row["conv_hbd"] * factor
            total_usd_for_unit = latest_row["conv_usd"] * factor
            total_sats_for_unit = latest_row["conv_sats"] * factor
            total_msats = latest_row["conv_msats"] * factor

            output.append("-" * max_width)
            output.append(f"{'Final Balance':<20} {final_balance:>42,.2f} {unit.upper():>12}")
            output.append(
                f"{'Converted Values':<10} "
                f"{total_hive:>15,.2f} HIVE "
                f"{total_hbd:>12,.2f} HBD "
                f"{total_usd_for_unit:>12,.2f} USD "
                f"{total_sats_for_unit:>12,.0f} SATS "
                f"{total_msats:>16,.0f} msats"
            )

            total_usd += total_usd_for_unit
            total_sats += total_sats_for_unit

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>19,.2f}")
    output.append(f"Total SATS: {total_sats:>15,.0f}")
    output.append(title_line)
    output.append("=" * max_width + "\n")

    return "\n".join(output)


async def list_all_accounts() -> List[Account]:
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
    async for doc in cursor:
        account = Account.model_validate(doc)
        accounts.append(account)
    return accounts
