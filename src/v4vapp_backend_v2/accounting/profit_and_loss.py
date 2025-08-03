from datetime import datetime, timedelta, timezone

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.pipelines.balance_sheet_pipelines import profit_loss_pipeline


async def generate_profit_and_loss_report(
    as_of_date: datetime = datetime.now(tz=timezone.utc),
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
    # Fetch ledger entries if DataFrame is empty
    pl_pipeline = profit_loss_pipeline(as_of_date=as_of_date, age=age)
    pl_cursor = await LedgerEntry.collection().aggregate(pipeline=pl_pipeline)
    profit_loss_list = await pl_cursor.to_list()
    profit_loss = profit_loss_list[0] if profit_loss_list else {}

    return profit_loss


async def profit_and_loss_printout(
    pl_report: dict = {},
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta = timedelta(days=0),
) -> str:
    """
    Formats a Profit and Loss report, displaying SATS, msats, HIVE, HBD, and USD, with Net Income by sub-account and total.

    Args:
        pl_report (dict): The P&L report dictionary from generate_profit_and_loss_report.
        as_of_date (datetime, optional): End date for the report period.

    Returns:
        str: A formatted string representation of the P&L report.
    """
    if not pl_report:
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
        output.append(
            f"{'Net Income':<40} {sub:<17} {balance['sats']:>10,.0f} {balance['msats']:>12,.0f} {balance['hive']:>12,.3f} {balance['hbd']:>12,.3f} {balance['usd']:>12,.2f}"
        )
    total = pl_report["Net Income"].get("Total", {})
    output.append(
        f"{'   Total Net Income':<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
    )
    output.append("=" * max_width)

    return "\n".join(output)
