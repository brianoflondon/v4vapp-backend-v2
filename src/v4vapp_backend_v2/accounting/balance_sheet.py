import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pprint import pprint
from typing import Any, Dict, List, Tuple

import pandas as pd

from v4vapp_backend_v2.accounting.account_type import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import (
    filter_by_account_as_of_date_query,
    list_all_accounts_pipeline,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text


async def get_ledger_entries(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "",
    filter_by_account: LedgerAccount | None = None,
) -> list[LedgerEntry]:
    """
    Retrieves ledger entries from the database up to a specified date, optionally filtered by account.

    Args:
        as_of_date (datetime, optional): The cutoff date for retrieving ledger entries.
            Defaults to the current UTC datetime.
        collection_name (str, optional): The name of the database collection to query.
            Defaults to "ledger".
        filter_by_account (Account | None, optional): An Account object to filter entries by.
            If provided, only entries where the account matches either the debit or credit side
            (considering both name and sub-account) are returned. Defaults to None.

    Returns:
        list[LedgerEntry]: A list of LedgerEntry objects sorted by timestamp (ascending).

    Notes:
        - Queries the database for entries with a timestamp less than or equal to `as_of_date`.
        - Sorts results by timestamp in ascending order (earliest to latest).
        - If filter_by_account is provided, matches entries where either the debit or credit side
          corresponds to the specified account name and sub-account.
    """
    collection_name = LedgerEntry.collection() if not collection_name else collection_name
    query = filter_by_account_as_of_date_query(account=filter_by_account, as_of_date=as_of_date)
    ledger_entries = []
    async with TrackedBaseModel.db_client as db_client:
        cursor = await db_client.find(
            collection_name=collection_name,
            query=query,
            # projection={
            #     "group_id": 1,
            #     "timestamp": 1,
            #     "description": 1,
            #     "debit_amount": 1,
            #     "debit_unit": 1,
            #     "debit_conv": 1,
            #     "credit_amount": 1,
            #     "credit_unit": 1,
            #     "credit_conv": 1,
            #     "debit": 1,
            #     "credit": 1,
            #     "_id": 0,
            #     "op": 1,
            # },
            sort=[("timestamp", 1)],
        )
        async for entry in cursor:
            try:
                ledger_entry = LedgerEntry.model_validate(entry)
                ledger_entries.append(ledger_entry)
            except Exception as e:
                logger.error(
                    f"Error validating ledger entry: {entry}. Error: {e}",
                    extra={"notification": False, "entry": entry, "error": str(e)},
                )
                continue
    return ledger_entries


async def get_ledger_dataframe(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    collection_name: str = "",
    filter_by_account: LedgerAccount | None = None,
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
            - short_id: A short identifier for the ledger entry.
            - description: A description of the ledger entry.
            - debit_amount: The amount of the debit transaction.
            - debit_unit: The unit of the debit amount.
            - credit_amount: The amount of the credit transaction.
            - credit_unit: The unit of the credit amount.
            - debit_conv_sats, debit_conv_msats, debit_conv_hive, debit_conv_hbd, debit_conv_usd: Converted values for debit.
            - credit_conv_sats, credit_conv_msats, credit_conv_hive, credit_conv_hbd, credit_conv_usd: Converted values for credit.
            - debit_name: The name of the debit account.
            - debit_account_type: The type of the debit account.
            - debit_sub: The sub-account of the debit account.
            - credit_name: The name of the credit account.
            - credit_account_type: The type of the credit account.
            - credit_sub: The sub-account of the credit account.
    """
    collection_name = LedgerEntry.collection() if not collection_name else collection_name
    ledger_entries = await get_ledger_entries(
        as_of_date=as_of_date, collection_name=collection_name, filter_by_account=filter_by_account
    )
    data = []
    for entry in ledger_entries:
        if entry.debit and entry.credit:
            debit_modifier = -1 if entry.debit.contra else 1
            credit_modifier = -1 if entry.credit.contra else 1
            debit_modifier = 1
            credit_modifier = 1

            debit_amount = debit_modifier * entry.debit_amount
            debit_unit = entry.debit_unit.value if entry.debit_unit else None
            debit_conv = debit_modifier * entry.debit_conv
            credit_amount = credit_modifier * entry.credit_amount
            credit_unit = entry.credit_unit.value if entry.credit_unit else None
            credit_conv = credit_modifier * entry.credit_conv

            data.append(
                {
                    "timestamp": entry.timestamp,
                    "group_id": entry.group_id,
                    "short_id": entry.short_id,
                    "description": entry.description,
                    "debit_amount": debit_amount,
                    "debit_unit": debit_unit,
                    "debit_conv_sats": debit_conv.sats,
                    "debit_conv_msats": debit_conv.msats,
                    "debit_conv_hive": debit_conv.hive,
                    "debit_conv_hbd": debit_conv.hbd,
                    "debit_conv_usd": debit_conv.usd,
                    "credit_amount": credit_amount,
                    "credit_unit": credit_unit,
                    "credit_conv_sats": credit_conv.sats,
                    "credit_conv_msats": credit_conv.msats,
                    "credit_conv_hive": credit_conv.hive,
                    "credit_conv_hbd": credit_conv.hbd,
                    "credit_conv_usd": credit_conv.usd,
                    "debit_name": entry.debit.name,
                    "debit_account_type": entry.debit.account_type,
                    "debit_sub": entry.debit.sub,
                    "debit_contra": entry.debit.contra,
                    "credit_name": entry.credit.name,
                    "credit_account_type": entry.credit.account_type,
                    "credit_sub": entry.credit.sub,
                    "credit_contra": entry.credit.contra,
                }
            )

    df = pd.DataFrame(data)
    return df


# MARK: Balance Sheet Generation
async def generate_balance_sheet_pandas(
    df: pd.DataFrame = pd.DataFrame(), reporting_date: datetime = None
) -> Dict:
    """
    Generates a GAAP-compliant balance sheet in USD, with supplemental columns for HIVE, HBD, SATS, and msats.
    Includes proper CTA calculation.
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
    latest_entry = df.iloc[-1]
    if reporting_date is None:
        reporting_date = latest_entry["timestamp"]

    # Derive spot rates from the most recent entry (using credit side for consistency)
    conv_usd = latest_entry["credit_conv_usd"]
    conv_hbd = latest_entry["credit_conv_hbd"]
    conv_hive = latest_entry["credit_conv_hive"]
    conv_sats = latest_entry["credit_conv_sats"]
    conv_msats = latest_entry["credit_conv_msats"]

    spot_rates = {
        "hbd_to_usd": conv_usd / conv_hbd if conv_hbd != 0 else 1.0,
        "hive_to_usd": conv_usd / conv_hive if conv_hive != 0 else 0.0,
        "sats_to_usd": conv_usd / conv_sats if conv_sats != 0 else 0.0,
        "msats_to_usd": conv_usd / conv_msats if conv_msats != 0 else 0.0,
    }

    # Step 2: Sum amounts in native units and historical USD
    # Process debits
    debit_df = df[
        [
            "debit_name",
            "debit_account_type",
            "debit_sub",
            "debit_amount",
            "debit_unit",
            "debit_conv_usd",
            "debit_contra",
        ]
    ].copy()
    debit_df = debit_df.rename(
        columns={
            "debit_name": "name",
            "debit_account_type": "account_type",
            "debit_sub": "sub",
            "debit_amount": "amount",
            "debit_unit": "unit",
            "debit_conv_usd": "conv_usd",
            "debit_contra": "contra",
        }
    )

    # Process credits
    credit_df = df[
        [
            "credit_name",
            "credit_account_type",
            "credit_sub",
            "credit_amount",
            "credit_unit",
            "credit_conv_usd",
            "credit_contra",
        ]
    ].copy()
    credit_df = credit_df.rename(
        columns={
            "credit_name": "name",
            "credit_account_type": "account_type",
            "credit_sub": "sub",
            "credit_amount": "amount",
            "credit_unit": "unit",
            "credit_conv_usd": "conv_usd",
            "credit_contra": "contra",
        }
    )

    # Combine debits and credits with signed amounts
    # print("Processing debit and credit entries...")

    debit_df["amount_adj"] = debit_df.apply(
        lambda row: row["amount"] if row["account_type"] == "Asset" else -row["amount"],
        axis=1,
    )
    debit_df["usd_adj"] = debit_df.apply(
        lambda row: row["conv_usd"] if row["account_type"] == "Asset" else -row["conv_usd"],
        axis=1,
    )

    credit_df["amount_adj"] = credit_df.apply(
        lambda row: -row["amount"] if row["account_type"] == "Asset" else row["amount"],
        axis=1,
    )
    credit_df["usd_adj"] = credit_df.apply(
        lambda row: -row["conv_usd"] if row["account_type"] == "Asset" else row["conv_usd"],
        axis=1,
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
        if unit and unit.lower() == "hive":
            balance_sheet[category][name][sub]["hive"] += amount
        elif unit and unit.lower() == "hbd":
            balance_sheet[category][name][sub]["hbd"] += amount
        elif unit and unit.lower() == "sats":
            balance_sheet[category][name][sub]["sats"] += amount
        historical_usd[category][name][sub] += usd_historical

    # Step 7: Translate to USD and supplemental currencies
    translated_values = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
    }

    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            # Ensure translated_values has an entry for every account, even if empty
            if not translated_values[category][account_name]:
                for sub in balance_sheet[category][account_name]:
                    if sub != "Total":
                        translated_values[category][account_name][sub] = {
                            "usd": 0.0,
                            "hive": 0.0,
                            "hbd": 0.0,
                            "sats": 0.0,
                            "msats": 0.0,
                        }
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

                # Update translated values
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
        for account_name in balance_sheet[category]:
            for sub in translated_values[category][account_name]:
                balance_sheet[category][account_name][sub] = translated_values[category][
                    account_name
                ][sub]

    # Step 7: Calculate totals for each account and category
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            sub_accounts = {
                sub: balance
                for sub, balance in balance_sheet[category][account_name].items()
                if sub != "Total"
            }
            total_usd = (
                sum(sub_acc["usd"] for sub_acc in sub_accounts.values() if "usd" in sub_acc)
                if sub_accounts
                else 0.0
            )
            total_hive = (
                sum(sub_acc["hive"] for sub_acc in sub_accounts.values() if "hive" in sub_acc)
                if sub_accounts
                else 0.0
            )
            total_hbd = (
                sum(sub_acc["hbd"] for sub_acc in sub_accounts.values() if "hbd" in sub_acc)
                if sub_accounts
                else 0.0
            )
            total_sats = (
                sum(sub_acc["sats"] for sub_acc in sub_accounts.values() if "sats" in sub_acc)
                if sub_accounts
                else 0.0
            )
            total_msats = (
                sum(sub_acc["msats"] for sub_acc in sub_accounts.values() if "msats" in sub_acc)
                if sub_accounts
                else 0.0
            )
            total_historical_usd = (
                sum(
                    historical_usd[category][account_name][sub]
                    for sub in historical_usd[category][account_name]
                )
                if historical_usd[category][account_name]
                else 0.0
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
    total_assets_usd = balance_sheet["Assets"]["Total"]["usd"]
    total_liabilities_usd = balance_sheet["Liabilities"]["Total"]["usd"]
    total_equity_usd = balance_sheet["Equity"]["Total"]["usd"]

    total_assets_historical_usd = balance_sheet["Assets"]["Total"]["historical_usd"]
    total_liabilities_historical_usd = balance_sheet["Liabilities"]["Total"]["historical_usd"]
    total_equity_historical_usd = balance_sheet["Equity"]["Total"]["historical_usd"]

    cta = (
        (total_assets_usd - total_assets_historical_usd)
        - (total_liabilities_usd - total_liabilities_historical_usd)
        - (total_equity_usd - total_equity_historical_usd)
    )

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


def check_balance_sheet(balance_sheet: Dict) -> bool:
    """
    Checks if the balance sheet is balanced.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.
    """
    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["usd"],
        balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
        rel_tol=0.01,
    )
    return is_balanced


def balance_sheet_printout(
    balance_sheet: Dict, as_of_date: datetime = datetime.now(tz=timezone.utc)
) -> str:
    """
    Formats the balance sheet into a readable string representation, displaying only USD values.
    Includes sections for Assets, Liabilities, and Equity, along with their respective totals.
    The total liabilities and equity are displayed at the end.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.
        as_of_date (datetime): The date for which the balance sheet is being formatted.

    Returns:
        str: A formatted string representation of the balance sheet, showing only USD values.
    """
    output = []
    max_width = 94
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
            account_display = truncate_text(account_name, 74)
            output.append(f"{account_display:<74}")
            for sub, balance in sub_accounts.items():
                if sub == "Total":
                    continue
                sub_display = truncate_text(sub, 64)
                formatted_balance = f"${balance['usd']:,.2f}"
                output.append(f"    {sub_display:<64} {formatted_balance:>15}")
            total_display = f"Total {truncate_text(account_name, 67)}"
            formatted_total = f"${sub_accounts['Total']['usd']:,.2f}"
            output.append(f"  {total_display:<67} {formatted_total:>15}")
        formatted_total = f"${balance_sheet[category]['Total']['usd']:,.2f}"
        output.append(f"{'Total ' + category:<74} {formatted_total:>15}")

    # Total Liabilities and Equity
    heading = truncate_text("Total Liabilities and Equity", max_width, centered=True)
    output.append(f"\n{heading}")
    output.append("-" * 5)
    formatted_total = f"${balance_sheet['Total Liabilities and Equity']['usd']:,.2f}"
    output.append(f"{'':<74} {formatted_total:>15}")

    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["usd"],
        balance_sheet["Liabilities"]["Total"]["usd"] + balance_sheet["Equity"]["Total"]["usd"],
        rel_tol=0.01,
    )
    if is_balanced:
        output.append(f"\n{'The balance sheet is balanced.':^94}")
    else:
        output.append(f"\n{'******* The balance sheet is NOT balanced. ********':^94}")

    return "\n".join(output)


def balance_sheet_all_currencies_printout(balance_sheet: Dict) -> str:
    """
    Formats a table with balances in SATS, HIVE, HBD, and USD.
    Returns a string table for reference.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.

    Returns:
        str: A formatted string table displaying balances in SATS, HIVE, HBD, and USD.
    """
    max_width = 115
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
                    f"{balance.get('sats', 0):>10,.0f} "
                    f"{balance.get('hive', 0):>12,.2f} "
                    f"{balance.get('hbd', 0):>12,.2f} "
                    f"{balance.get('usd', 0):>12,.2f}"
                )
            if "Total" not in sub_accounts:
                total_usd = sum(
                    sub_acc.get("usd", 0)
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "usd" in sub_acc
                )
                total_hive = sum(
                    sub_acc.get("hive", 0)
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "hive" in sub_acc
                )
                total_hbd = sum(
                    sub_acc.get("hbd", 0)
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "hbd" in sub_acc
                )
                total_sats = sum(
                    sub_acc.get("sats", 0)
                    for sub, sub_acc in sub_accounts.items()
                    if sub != "Total" and "sats" in sub_acc
                )
                sub_accounts["Total"] = {
                    "usd": round(total_usd, 2),
                    "hive": round(total_hive, 2),
                    "hbd": round(total_hbd, 2),
                    "sats": round(total_sats, 0),
                    "msats": 0,
                }
            total = sub_accounts["Total"]
            output.append(
                f"{'Total ' + truncate_text(account_name, 35):<40} "
                f"{'':<17} "
                f"{total.get('sats', 0):>10,.0f} "
                f"{total.get('hive', 0):>12,.2f} "
                f"{total.get('hbd', 0):>12,.2f} "
                f"{total.get('usd', 0):>12,.2f}"
            )
        total = balance_sheet[category]["Total"]
        output.append("-" * max_width)
        output.append(
            f"{'Total ' + category:<40} "
            f"{'':<17} "
            f"{total.get('sats', 0):>10,.0f} "
            f"{total.get('hive', 0):>12,.2f} "
            f"{total.get('hbd', 0):>12,.2f} "
            f"{total.get('usd', 0):>12,.2f}"
        )
        output.append("-" * max_width)

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * max_width)
    output.append(
        f"{'Total Liab. & Equity':<40} "
        f"{'':<17} "
        f"{total.get('sats', 0):>10,.0f} "
        f"{total.get('hive', 0):>12,.2f} "
        f"{total.get('hbd', 0):>12,.2f} "
        f"{total.get('usd', 0):>12,.2f}"
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
    account: LedgerAccount,
    df: pd.DataFrame = pd.DataFrame(),
    full_history: bool = False,
    as_of_date: datetime | None = None,
) -> pd.DataFrame:
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    if df.empty:
        df = await get_ledger_dataframe(
            as_of_date=as_of_date,
            filter_by_account=account,
        )

    if df.empty:
        logger.warning(f"No transactions found for account {account.name} up to {as_of_date}.")
        return pd.DataFrame()

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
            "short_id",
            "debit_amount",
            "debit_unit",
            "debit_conv_hive",
            "debit_conv_hbd",
            "debit_conv_usd",
            "debit_conv_sats",
            "debit_conv_msats",
            "debit_contra",
        ]
    ].copy()
    credit_df = credit_df[
        [
            "timestamp",
            "description",
            "short_id",
            "credit_amount",
            "credit_unit",
            "credit_conv_hive",
            "credit_conv_hbd",
            "credit_conv_usd",
            "credit_conv_sats",
            "credit_conv_msats",
            "credit_contra",
        ]
    ].copy()

    # Add debit/credit columns and signed amounts
    debit_df["debit_amount"] = debit_df["debit_amount"]
    debit_df["credit_amount"] = 0.0
    debit_df["debit_unit"] = debit_df["debit_unit"]
    debit_df["credit_unit"] = None
    credit_df["debit_amount"] = 0.0
    credit_df["credit_amount"] = credit_df["credit_amount"]
    credit_df["debit_unit"] = None
    credit_df["credit_unit"] = credit_df["credit_unit"]

    # Determine signed amounts based on account type
    if account.account_type == "Asset":
        debit_df["signed_amount"] = debit_df["debit_amount"]
        credit_df["signed_amount"] = -credit_df["credit_amount"]
    else:  # Liability, Equity, Revenue, Expense
        debit_df["signed_amount"] = -debit_df["debit_amount"]
        credit_df["signed_amount"] = credit_df["credit_amount"]

    # Combine debits and credits
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)
    combined_df = combined_df.sort_values(by="timestamp").reset_index(drop=True)

    return combined_df


@dataclass
class ConvertedSummary:
    hive: float
    hbd: float
    usd: float
    sats: float
    msats: float


@dataclass
class UnitSummary:
    final_balance: float
    converted: ConvertedSummary


@dataclass
class AccountBalanceSummary:
    unit_summaries: Dict[str, UnitSummary] = field(default_factory=dict)
    total_usd: float = 0.0
    total_sats: float = 0.0
    line_items: List[str] = field(default_factory=list)
    output_text: str = ""


async def get_account_balance_printout(
    account: LedgerAccount,
    df: pd.DataFrame = pd.DataFrame(),
    full_history: bool = False,
    as_of_date: datetime | None = None,
) -> Tuple[str, AccountBalanceSummary]:
    """
    Calculate and display the balance for a specified account (and optional sub-account) from the DataFrame.
    Optionally lists all debit and credit transactions up to today, or shows only the closing balance.
    Properly accounts for assets and liabilities, and includes converted values to other units
    (SATS, HIVE, HBD, USD, msats).

    Args:
        account (Account): An Account object specifying the account name, type, and optional sub-account.
        df (pd.DataFrame): A DataFrame containing transaction data with columns: timestamp, debit_amount, debit_unit, etc.
        full_history (bool, optional): If True, shows the full transaction history with running balances.
                                       If False, shows only the closing balance. Defaults to False.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to None (current date).

    Returns:
        str: A formatted string containing either the full transaction history or the closing balance
             for the specified account and sub-account up to the specified date.
    """
    max_width = 125
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    combined_df = await get_account_balance(
        account=account,
        df=df,
        full_history=full_history,
        as_of_date=as_of_date,
    )
    if combined_df.empty:
        logger.warning(f"No transactions found for account {account.name} up to {as_of_date}.")
        return "No transactions found for this account up to today.", AccountBalanceSummary()

    # Group by unit (process debit_unit and credit_unit separately)
    units = set(combined_df["debit_unit"].dropna().unique()).union(
        set(combined_df["credit_unit"].dropna().unique())
    )
    title_line = f"Balance for {account}"
    output = ["=" * max_width]
    output.append(title_line)
    output.append("-" * max_width)

    if combined_df.empty:
        output.append("No transactions found for this account up to today.")
        output.append("=" * max_width)
        return "\n".join(output), AccountBalanceSummary()

    total_usd = 0.0
    total_sats = 0.0
    unit_balances = {unit: 0.0 for unit in units}

    summary = AccountBalanceSummary()
    summary.total_usd = 0.0
    summary.total_sats = 0.0
    summary.line_items = []
    summary.unit_summaries = {}

    for unit in units:
        unit_df = combined_df[
            (combined_df["debit_unit"] == unit) | (combined_df["credit_unit"] == unit)
        ].copy()
        if unit_df.empty:
            continue

        # Calculate running balance for this unit
        unit_df["running_balance"] = unit_df["signed_amount"].cumsum()

        # Determine display unit: if MSATS, display as SATS
        display_unit = "SATS" if unit.upper() == "MSATS" else unit.upper()
        conversion_factor = 1000 if unit.upper() == "MSATS" else 1  # Convert MSATS to SATS

        # Format output for this unit
        output.append(f"\nUnit: {display_unit}")
        output.append("-" * 10)
        if full_history:
            for _, row in unit_df.iterrows():
                contra_str = (
                    "(-)"
                    if (pd.notna(row["credit_contra"]) and row["credit_contra"])
                    or (pd.notna(row["debit_contra"]) and row["debit_contra"])
                    else "   "
                )
                timestamp = row["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                description = truncate_text(row["description"], 45)
                debit = row["debit_amount"] if row["debit_unit"] == unit else 0.0
                credit = row["credit_amount"] if row["credit_unit"] == unit else 0.0
                balance = row["running_balance"]
                short_id = row.get("short_id", "")
                if unit.upper() == "MSATS":
                    debit = debit / conversion_factor
                    credit = credit / conversion_factor
                    balance = balance / conversion_factor
                debit_str = f"{debit:,.0f}" if unit.upper() == "MSATS" else f"{debit:>12,.3f}"
                credit_str = f"{credit:,.0f}" if unit.upper() == "MSATS" else f"{credit:>12,.3f}"
                balance_str = (
                    f"{balance:,.0f}" if unit.upper() == "MSATS" else f"{balance:>12,.3f}"
                )
                line = (
                    f"{timestamp:<20} "
                    f"{description:<45} "
                    f"{contra_str} "
                    f"{debit_str:>12} "
                    f"{credit_str:>12} "
                    f"{balance_str:>12} "
                    f"{short_id:>15}"
                )
                summary.line_items.append(line)
                output.append(line)

        # Get the final balance for this unit and calculate converted values
        final_balance = unit_df["running_balance"].iloc[-1]
        unit_balances[unit] = final_balance
        if final_balance != 0:
            # Use the conversion rates from the latest transaction for this unit
            latest_row = unit_df.iloc[-1]
            if latest_row["debit_unit"] == unit:
                conv_hive = latest_row["debit_conv_hive"]
                conv_hbd = latest_row["debit_conv_hbd"]
                conv_usd = latest_row["debit_conv_usd"]
                conv_sats = latest_row["debit_conv_sats"]
                conv_msats = latest_row["debit_conv_msats"]
                amount = latest_row["debit_amount"]
            else:
                conv_hive = latest_row["credit_conv_hive"]
                conv_hbd = latest_row["credit_conv_hbd"]
                conv_usd = latest_row["credit_conv_usd"]
                conv_sats = latest_row["credit_conv_sats"]
                conv_msats = latest_row["credit_conv_msats"]
                amount = latest_row["credit_amount"]

            factor = final_balance / amount if amount != 0 else 0.0
            total_hive = conv_hive * factor
            total_hbd = conv_hbd * factor
            total_usd_for_unit = conv_usd * factor
            total_sats_for_unit = conv_sats * factor
            total_msats = conv_msats * factor

            summary.unit_summaries[unit] = UnitSummary(
                final_balance=final_balance,
                converted=ConvertedSummary(
                    hive=total_hive,
                    hbd=total_hbd,
                    usd=total_usd_for_unit,
                    sats=total_sats_for_unit,
                    msats=total_msats,
                ),
            )
            summary.total_usd += total_usd_for_unit
            summary.total_sats += total_sats_for_unit

            output.append("-" * max_width)
            output.append(
                f"{'Converted    ':<10} "
                f"{total_hive:>15,.3f} HIVE "
                f"{total_hbd:>12,.3f} HBD "
                f"{total_usd_for_unit:>12,.3f} USD "
                f"{total_sats_for_unit:>12,.0f} SATS "
                f"{total_msats:>16,.0f} msats"
            )
            output.append("-" * max_width)
            # Display final balance in SATS if unit is MSATS
            display_balance = (
                final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
            )
            balance_str = (
                f"{display_balance:,.0f}"
                if unit.upper() == "MSATS"
                else f"{display_balance:>10,.3f}"
            )
            output.append(f"{'Final Balance':<18} {balance_str:>10} {display_unit:<5}")

            # if unit.upper() != "MSATS" and adjustment.get(unit, 0) != 0:
            #     adjusted_balance = final_balance + adjustment.get(unit, 0)
            #     adjusted_balance_str = f"{adjusted_balance:>10,.2f}"
            #     output.append(f"{'Adj Balance':<18} {adjusted_balance_str:>10} {display_unit:<5}")

            total_usd += total_usd_for_unit
            total_sats += total_sats_for_unit

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>19,.3f}")
    output.append(f"Total SATS: {total_sats:>18,.0f}")
    output.append(title_line)

    output.append("=" * max_width + "\n")

    summary.output_text = "\n".join(output)

    return summary.output_text, summary


async def list_all_accounts() -> List[LedgerAccount]:
    """
    Lists all unique accounts in the ledger by aggregating debit and credit accounts.

    Returns:
        List[Account]: A list of unique Account objects sorted by account type, name, and sub-account.
    """
    pipeline = list_all_accounts_pipeline()

    collection = await TrackedBaseModel.db_client.get_collection("ledger")
    cursor = collection.aggregate(pipeline=pipeline)
    accounts = []
    async for doc in cursor:
        account = LedgerAccount.model_validate(doc)
        accounts.append(account)
    return accounts


# async def get_conversion_adjustment(
#     conversion_account: Account, as_of_date: datetime | None = None
# ) -> Dict[str, float]:
#     """
#     Queries the ledger for conversion entries credited to Customer Deposits Hive,
#     sums the credit_amounts for HIVE and HBD, and returns them as an adjustment.

#     Returns:
#         Dict[str, float]: A dictionary with keys "hive" and "hbd" representing the adjustments.
#     """
#     if not as_of_date:
#         as_of_date = datetime.now(tz=timezone.utc)

#     collection = await TrackedBaseModel.db_client.get_collection("ledger")
#     query = {
#         "group_id": {"$regex": ".*conversion"},
#         "credit.name": {"$regex": f"^{conversion_account.name}"},
#         "credit.sub": {"$regex": f"^{conversion_account.sub}$"} if conversion_account.sub else {},
#         "timestamp": {"$lte": as_of_date},
#     }
#     cursor = collection.find(query)
#     hive_total = 0.0
#     hbd_total = 0.0
#     async for doc in cursor:
#         credit_unit = doc.get("credit_unit", "").lower()
#         credit_amount = float(doc.get("credit_amount", 0))
#         if credit_unit == "hive":
#             hive_total += credit_amount
#         elif credit_unit == "hbd":
#             hbd_total += credit_amount
#     return {
#         "hive": hive_total,
#         "hbd": hbd_total,
#     }
