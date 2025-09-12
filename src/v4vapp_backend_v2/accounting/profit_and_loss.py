from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from bson import Decimal128

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.pipelines.balance_sheet_pipelines import profit_loss_pipeline
from v4vapp_backend_v2.database.db_tools import _convert_decimal128_to_decimal



async def generate_profit_and_loss_report(
    as_of_date: datetime | None = None,
    age: timedelta = timedelta(days=0),
) -> dict:
    """
    Generates a Profit and Loss report summarizing Revenue and Expense accounts, using msats as the base unit.

    Args:
        df (pd.DataFrame, optional): DataFrame of ledger entries. If empty, fetches from database.
        as_of_date (datetime, optional): End date for the report period.
        collection_name (str, optional): Database collection name. Defaults to "ledger".

    Returns:
        dict: A dictionary with:
            - Revenue: {account_name: {sub: {sats, msats, hive, hbd, usd}}}
            - Expenses: {account_name: {sub: {sats, msats, hive, hbd, usd}}}
            - Net Income: {sub: {sats, msats, hive, hbd, usd}, "Total": {...}}
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    # Fetch ledger entries if DataFrame is empty
    pl_pipeline = profit_loss_pipeline(as_of_date=as_of_date, age=age)
    pl_cursor = await LedgerEntry.collection().aggregate(pipeline=pl_pipeline)
    profit_loss_list = await pl_cursor.to_list()
    profit_loss = profit_loss_list[0] if profit_loss_list else {}

    # Convert Decimal128 values to Decimal for formatting operations
    profit_loss = _convert_decimal128_to_decimal(profit_loss)

    return profit_loss


async def profit_and_loss_printout(
    pl_report: dict[str, Any] | None = None,
    as_of_date: datetime | None = None,
    age: timedelta = timedelta(days=0),
) -> str:
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    if pl_report is None:
        pl_report = await generate_profit_and_loss_report(as_of_date=as_of_date, age=age)

    max_width = 126
    output = []

    # Header
    date_str = f"{as_of_date:%Y-%m-%d %H:%M:%S} UTC"
    output.append(f"Profit and Loss Report for {date_str}")
    output.append("-" * max_width)
    output.append(
        f"{'Account':<40} {'Sub':<17} {'SATS':>10} {'msats':>12} {'HIVE':>12} {'HBD':>12} {'USD':>12}"
    )
    output.append("-" * max_width)

    # Revenue
    output.append("\nRevenue")
    output.append("-" * 30)
    for account_name, sub_accounts in pl_report["Revenue"].items():
        for sub, balance in sub_accounts.items():
            if sub == "Total":
                continue
            output.append(
                f"{account_name:<40} {sub:<17} {balance['sats']:>10,.0f} {balance['msats']:>12,.0f} {balance['hive']:>12,.3f} {balance['hbd']:>12,.3f} {balance['usd']:>12,.2f}"
            )
        total = sub_accounts.get("Total", {})
        output.append(
            f"{'   Total ' + account_name:<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
        )
    output.append("-" * max_width)

    # Expenses
    output.append("\nExpenses")
    output.append("-" * 30)
    for account_name, sub_accounts in pl_report["Expenses"].items():
        for sub, balance in sub_accounts.items():
            if sub == "Total":
                continue
            output.append(
                f"{account_name:<40} {sub:<17} {balance['sats']:>10,.0f} {balance['msats']:>12,.0f} {balance['hive']:>12,.3f} {balance['hbd']:>12,.3f} {balance['usd']:>12,.2f}"
            )
        total = sub_accounts.get("Total", {})
        output.append(
            f"{'   Total ' + account_name:<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
        )
    output.append("-" * max_width)

    # Net Income
    output.append("\nNet Income")
    output.append("-" * 30)
    for sub, balance in pl_report["Net Income"].items():
        if sub == "Total":
            continue
        # Determine label based on sign (using msats as the base unit for checking)
        if balance["msats"] >= 0:
            label = "Net Income"
        else:
            label = "Net Loss"
        output.append(
            f"{label:<40} {sub:<17} {balance['sats']:>10,.0f} {balance['msats']:>12,.0f} {balance['hive']:>12,.3f} {balance['hbd']:>12,.3f} {balance['usd']:>12,.2f}"
        )
    # Handle the total separately
    total = pl_report["Net Income"].get("Total", {})
    total_label = "   Total Net Income" if total.get("msats", 0) >= 0 else "   Total Net Loss"
    output.append(
        f"{total_label:<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
    )
    output.append("=" * max_width)

    return "\n".join(output)
    return "\n".join(output)
