import math
from asyncio import TaskGroup
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Tuple

from bson import Decimal128

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.pipelines.balance_sheet_pipelines import (
    balance_sheet_check_pipeline,
    balance_sheet_pipeline,
    profit_loss_pipeline,
)
from v4vapp_backend_v2.helpers.general_purpose_funcs import truncate_text


def _convert_decimal128_to_decimal(value: Any) -> Any:
    """Convert Decimal128 values to Decimal for arithmetic operations."""
    if isinstance(value, Decimal128):
        return Decimal(str(value))
    elif isinstance(value, dict):
        return {k: _convert_decimal128_to_decimal(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [_convert_decimal128_to_decimal(item) for item in value]
    else:
        return value


# @async_time_stats_decorator()
async def generate_balance_sheet_mongodb(
    as_of_date: datetime | None = None, age: timedelta = timedelta(seconds=0)
) -> Dict[str, Any]:
    """
    Generates a balance sheet from MongoDB data.

    Args:
        as_of_date (datetime): The date for which the balance sheet is generated.
        age (timedelta | None): The age of the data to include in the balance sheet.

    Returns:
        Sequence[Mapping[str, Any]]: The generated balance sheet.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

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

    # Convert Decimal128 values to Decimal for arithmetic operations
    balance_sheet = _convert_decimal128_to_decimal(balance_sheet)
    profit_loss = _convert_decimal128_to_decimal(profit_loss)

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
                section_total = {cur: Decimal(0) for cur in currencies}
                for account in balance_sheet[section]:
                    if account != "Total" and "Total" in balance_sheet[section][account]:
                        for cur in currencies:
                            section_total[cur] += Decimal(balance_sheet[section][account]["Total"].get(
                                cur, Decimal(0)
                            ))
                balance_sheet[section]["Total"] = section_total
            else:
                balance_sheet[section] = {
                    "Total": {cur: Decimal(0) for cur in currencies}
                }  # Calculate grand total (Assets vs Liabilities + Equity)
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
    # Use math.isclose to avoid brittle exact equality on computed sums
    if not math.isclose(tolerance_msats_check, tolerance_msats, rel_tol=0.0, abs_tol=1_000):
        raise AssertionError(
            f"Balance sheet tolerance mismatch: computed={tolerance_msats_check} msats, "
            f"check pipeline returned={tolerance_msats} msats"
        )

    balance_sheet["is_balanced"] = is_balanced
    balance_sheet["tolerance"] = tolerance_msats
    balance_sheet["as_of_date"] = as_of_date

    return balance_sheet


async def check_balance_sheet_mongodb(
    as_of_date: datetime | None = None, age: timedelta | None = None
) -> Tuple[bool, float]:
    """
    Checks if the balance sheet is balanced using MongoDB data.

    Args:
        as_of_date (datetime | None): The date for which the balance sheet is checked.
        age (timedelta | None): The age of the data to include in the balance sheet.

    Returns:
        bool: True if the balance sheet is balanced, False otherwise.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

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


def balance_sheet_printout(balance_sheet: Dict, vsc_details: bool = False) -> str:
    """
    Formats the balance sheet into a readable string representation, displaying only USD values.
    Includes sections for Assets, Liabilities, and Equity, along with their respective totals.
    The total liabilities and equity are displayed at the end.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.
        vsc_details (bool): If False, hides individual sub-account lines for VSC Liability account.

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
            if not (account_name == "VSC Liability" and not vsc_details):
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


def balance_sheet_all_currencies_printout(balance_sheet: Dict, vsc_details: bool = False) -> str:
    """
    Formats a table with balances in SATS, HIVE, HBD, and USD.
    Returns a string table for reference.

    Args:
        balance_sheet (Dict): A dictionary containing the balance sheet data.
        vsc_details (bool): If False, hides individual sub-account lines for VSC Liability account.

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

            # For Retained Earnings, use dynamic labels based on sign
            is_retained_earnings = category == "Equity" and account_name == "Retained Earnings"

            if not (account_name == "VSC Liability" and not vsc_details):
                for sub, balance in sub_accounts.items():
                    if sub == "Total":
                        continue

                    if is_retained_earnings:
                        # Determine label based on sign (using sats as the base unit for checking)
                        if balance.get("sats", 0) >= 0:
                            dynamic_label = "Retained Earnings"
                        else:
                            dynamic_label = "Retained Loss"
                    else:
                        dynamic_label = account_name

                    output.append(
                        f"{truncate_text(dynamic_label, 40):<40} "
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

            # Dynamic total label for Retained Earnings
            if is_retained_earnings:
                if total.get("sats", 0) >= 0:
                    total_label = "   Total Retained Earnings"
                else:
                    total_label = "   Total Retained Loss"
            else:
                total_label = "   Total " + truncate_text(account_name, 35)

            output.append(
                f"{total_label:<40} "
                f"{'':<17} "
                f"{total.get('sats', 0):>10,.0f} "
                f"{total.get('hive', 0):>12,.3f} "
                f"{total.get('hbd', 0):>12,.3f} "
                f"{total.get('usd', 0):>12,.3f}"
            )
            output.append(f"{'-' * max_width}")
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
        output.append("=" * max_width)

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * max_width)
    output.append(
        f"{'Total Liabilities & Equity':<40} "
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
