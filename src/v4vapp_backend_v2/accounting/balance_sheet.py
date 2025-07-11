import math
from asyncio import TaskGroup
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict

import pandas as pd

from v4vapp_backend_v2.accounting.account_balances import get_all_accounts
from v4vapp_backend_v2.accounting.profit_and_loss import generate_profit_and_loss_report
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text


@dataclass
class BalanceSheetDict:
    Assets: "defaultdict[str, dict]"
    Liabilities: "defaultdict[str, dict]"
    Equity: "defaultdict[str, dict]"
    is_balanced: bool
    as_of_date: datetime


# MARK: Balance Sheet Generation
async def generate_balance_sheet_pandas_from_accounts(
    df: pd.DataFrame = pd.DataFrame(),
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
) -> Dict:
    async with TaskGroup() as tg:
        all_accounts_task = tg.create_task(get_all_accounts(as_of_date=as_of_date))
        profit_and_loss_task = tg.create_task(
            generate_profit_and_loss_report(df=df, as_of_date=as_of_date)
        )
    all_accounts = await all_accounts_task
    profit_and_loss = await profit_and_loss_task

    net_income = profit_and_loss["Net Income"]

    balance_sheet = {
        "Assets": defaultdict(dict),
        "Liabilities": defaultdict(dict),
        "Equity": defaultdict(dict),
        "is_balanced": False,
        "as_of_date": as_of_date,
    }

    # Add in Net Income which we found earlier
    for sub, values in net_income.items():
        if sub == "Total":
            continue
        if "Retained Earnings" not in balance_sheet["Equity"]:
            balance_sheet["Equity"]["Retained Earnings"] = {}
        balance_sheet["Equity"]["Retained Earnings"][sub] = {
            "usd": values["usd"],
            "hive": values["hive"],
            "hbd": values["hbd"],
            "sats": values["sats"],
            "msats": values["msats"],
        }

    for account, summary in all_accounts.items():
        if account.account_type not in ["Asset", "Liability", "Equity"]:
            continue
        if account.account_type == "Asset":
            balance_sheet["Assets"][account.name][account.sub] = {
                "usd": summary.total_usd,
                "hive": summary.total_hive,
                "hbd": summary.total_hbd,
                "sats": summary.total_sats,
                "msats": summary.total_msats,
            }
        elif account.account_type == "Liability":
            balance_sheet["Liabilities"][account.name][account.sub] = {
                "usd": summary.total_usd,
                "hive": summary.total_hive,
                "hbd": summary.total_hbd,
                "sats": summary.total_sats,
                "msats": summary.total_msats,
            }
        elif account.account_type == "Equity":
            balance_sheet["Equity"][account.name][account.sub] = {
                "usd": summary.total_usd,
                "hive": summary.total_hive,
                "hbd": summary.total_hbd,
                "sats": summary.total_sats,
                "msats": summary.total_msats,
            }

    # Calculate totals for each account name
    for category in ["Assets", "Liabilities", "Equity"]:
        for account_name in balance_sheet[category]:
            total = {
                "usd": 0.0,
                "hive": 0.0,
                "hbd": 0.0,
                "sats": 0.0,
                "msats": 0.0,
            }
            for sub in balance_sheet[category][account_name]:
                if sub == "Total":  # Skip if Total already exists (unlikely)
                    continue
                for key in total:
                    total[key] += balance_sheet[category][account_name][sub][key]
            balance_sheet[category][account_name]["Total"] = total

    # Calculate totals for each category
    for category in ["Assets", "Liabilities", "Equity"]:
        total = {
            "usd": 0.0,
            "hive": 0.0,
            "hbd": 0.0,
            "sats": 0.0,
            "msats": 0.0,
        }
        for account_name in balance_sheet[category]:
            if "Total" in balance_sheet[category][account_name]:
                for key in total:
                    total[key] += balance_sheet[category][account_name]["Total"][key]
        balance_sheet[category]["Total"] = total

    # Calculate grand total (Assets vs Liabilities + Equity)
    assets_total = balance_sheet["Assets"]["Total"]
    liabilities_total = balance_sheet["Liabilities"]["Total"]
    equity_total = balance_sheet["Equity"]["Total"]

    balance_sheet["Total Liabilities and Equity"] = {
        "usd": liabilities_total["usd"] + equity_total["usd"],
        "hive": liabilities_total["hive"] + equity_total["hive"],
        "hbd": liabilities_total["hbd"] + equity_total["hbd"],
        "sats": liabilities_total["sats"] + equity_total["sats"],
        "msats": liabilities_total["msats"] + equity_total["msats"],
    }

    # Check if the balance sheet is balanced (Assets = Liabilities + Equity)
    balance_sheet["is_balanced"] = bool(check_balance_sheet(balance_sheet=balance_sheet))

    return balance_sheet


def check_balance_sheet(balance_sheet: Dict) -> bool:
    """
    Checks if the balance sheet is balanced.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.
    """
    tolerance_msats = 10_000  # tolerance of 10 sats.

    is_balanced = math.isclose(
        balance_sheet["Assets"]["Total"]["msats"],
        balance_sheet["Liabilities"]["Total"]["msats"] + balance_sheet["Equity"]["Total"]["msats"],
        rel_tol=0.01,
        abs_tol=tolerance_msats,
    )
    return is_balanced


def balance_sheet_printout(
    balance_sheet: Dict, as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1)
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

    if balance_sheet["is_balanced"]:
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

    if balance_sheet["is_balanced"]:
        output.append(f"\n{'The balance sheet is balanced.':^94}")
    else:
        output.append(f"\n{'******* The balance sheet is NOT balanced. ********':^94}")

    output.append("=" * max_width)

    return "\n".join(output)
