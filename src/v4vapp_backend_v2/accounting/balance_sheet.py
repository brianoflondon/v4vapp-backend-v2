import math
from asyncio import TaskGroup
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple

from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
from v4vapp_backend_v2.accounting.pipelines.balance_sheet_pipelines import (
    balance_sheet_check_pipeline,
    balance_sheet_pipeline,
    profit_loss_pipeline,
)
from v4vapp_backend_v2.config.setup import async_time_stats_decorator
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text


# @async_time_stats_decorator()
async def generate_balance_sheet_mongodb(
    as_of_date: datetime = datetime.now(tz=timezone.utc), age: timedelta = timedelta(seconds=0)
) -> Dict:
    """
    Generates a balance sheet from MongoDB data.

    Args:
        as_of_date (datetime): The date for which the balance sheet is generated.
        age (timedelta | None): The age of the data to include in the balance sheet.

    Returns:
        Sequence[Mapping[str, Any]]: The generated balance sheet.
    """
    bs_pipeline = balance_sheet_pipeline(as_of_date=as_of_date, age=age)
    pl_pipeline = profit_loss_pipeline(as_of_date=as_of_date, age=age)

    bs_cursor = await LedgerEntry.collection().aggregate(pipeline=bs_pipeline)
    pl_cursor = await LedgerEntry.collection().aggregate(pipeline=pl_pipeline)

    async with TaskGroup() as tg:
        balance_sheet_task = tg.create_task(bs_cursor.to_list())
        profit_loss_task = tg.create_task(pl_cursor.to_list())
        balance_sheet_check_task = tg.create_task(
            check_balance_sheet_mongodb(as_of_date=as_of_date, age=age)
        )
    balance_sheet_list = await balance_sheet_task
    profit_loss_list = await profit_loss_task
    is_balanced, tolerance_msats = await balance_sheet_check_task

    balance_sheet = balance_sheet_list[0] if balance_sheet_list else {}
    profit_loss = profit_loss_list[0] if profit_loss_list else {}

    if "Equity" not in balance_sheet:
        balance_sheet["Equity"] = {}

    net_income = profit_loss["Net Income"] if profit_loss else {}
    for sub, values in net_income.items():
        if "Retained Earnings" not in balance_sheet["Equity"]:
            balance_sheet["Equity"]["Retained Earnings"] = {}
        balance_sheet["Equity"]["Retained Earnings"][sub] = {
            "usd": values["usd"],
            "hive": values["hive"],
            "hbd": values["hbd"],
            "sats": values["sats"],
            "msats": values["msats"],
        }

    # Compute section totals
    currencies = ["hbd", "hive", "msats", "sats", "usd"]
    for section in ["Assets", "Liabilities", "Equity"]:
        if section in balance_sheet:
            section_total = {cur: 0.0 for cur in currencies}
            for account in balance_sheet[section]:
                if account != "Total" and "Total" in balance_sheet[section][account]:
                    for cur in currencies:
                        section_total[cur] += balance_sheet[section][account]["Total"].get(
                            cur, 0.0
                        )
            balance_sheet[section]["Total"] = section_total

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

    tolerance_msats_check = assets_total["msats"] - (
        liabilities_total["msats"] + equity_total["msats"]
    )
    assert tolerance_msats_check == tolerance_msats, (
        f"Balance sheet tolerance mismatch: {tolerance_msats_check} != {tolerance_msats}"
    )

    balance_sheet["is_balanced"] = is_balanced
    balance_sheet["tolerance"] = tolerance_msats
    balance_sheet["as_of_date"] = as_of_date

    return balance_sheet


async def check_balance_sheet_mongodb(
    as_of_date: datetime = datetime.now(tz=timezone.utc), age: timedelta | None = None
) -> Tuple[bool, float]:
    """
    Checks if the balance sheet is balanced using MongoDB data.

    Args:
        as_of_date (datetime): The date for which the balance sheet is checked.
        age (timedelta | None): The age of the data to include in the balance sheet.

    Returns:
        bool: True if the balance sheet is balanced, False otherwise.
    """
    bs_check_pipeline = balance_sheet_check_pipeline(as_of_date=as_of_date, age=age)
    bs_check_cursor = await LedgerEntry.collection().aggregate(pipeline=bs_check_pipeline)
    bs_check = await bs_check_cursor.to_list()

    # Database is empty or no data found
    if not bs_check:
        return True, 0.0

    tolerance_msats = 10_000  # tolerance of 10 sats.
    is_balanced = math.isclose(
        bs_check[0]["assets_msats"],
        bs_check[0]["liabilities_msats"] + bs_check[0]["equity_msats"],
        rel_tol=0.01,
        abs_tol=tolerance_msats,
    )
    return is_balanced, bs_check[0]["total_msats"]


def balance_sheet_printout(balance_sheet: Dict) -> str:
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
    date_str = balance_sheet["as_of_date"].strftime("%Y-%m-%d")
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

    output.append("=" * max_width)

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
                    f"{balance.get('hive', 0):>12,.3f} "
                    f"{balance.get('hbd', 0):>12,.3f} "
                    f"{balance.get('usd', 0):>12,.3f}"
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
                f"{'   Total ' + truncate_text(account_name, 35):<40} "
                f"{'':<17} "
                f"{total.get('sats', 0):>10,.0f} "
                f"{total.get('hive', 0):>12,.3f} "
                f"{total.get('hbd', 0):>12,.3f} "
                f"{total.get('usd', 0):>12,.3f}"
            )
            output.append(f"{'_' * max_width}")
        total = balance_sheet[category]["Total"]
        output.append("-" * max_width)
        output.append(
            f"{'   Total ' + category:<40} "
            f"{'':<17} "
            f"{total.get('sats', 0):>10,.0f} "
            f"{total.get('hive', 0):>12,.3f} "
            f"{total.get('hbd', 0):>12,.3f} "
            f"{total.get('usd', 0):>12,.3f}"
        )
        output.append("-" * max_width)

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * max_width)
    output.append(
        f"{'Total Liab. & Equity':<40} "
        f"{'':<17} "
        f"{total.get('sats', 0):>10,.0f} "
        f"{total.get('hive', 0):>12,.3f} "
        f"{total.get('hbd', 0):>12,.3f} "
        f"{total.get('usd', 0):>12,.3f}"
    )

    if balance_sheet["is_balanced"]:
        balance_line_text = (
            f"The balance sheet is balanced ({balance_sheet['tolerance']:.1f} msats tolerance)."
        )
    else:
        balance_line_text = (
            f"******* The balance sheet is NOT balanced. "
            f"Tolerance: {balance_sheet['tolerance']:.1f} msats. ********"
        )
    output.append(f"\n{balance_line_text:^94}")

    output.append("=" * max_width)

    return "\n".join(output)
