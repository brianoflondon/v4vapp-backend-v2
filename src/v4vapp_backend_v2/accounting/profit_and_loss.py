from collections import defaultdict
from datetime import datetime, timedelta, timezone

import pandas as pd

from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_dataframe


async def generate_profit_and_loss_report(
    df: pd.DataFrame = pd.DataFrame(),
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
    collection_name: str = "",
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
    if df.empty:
        df = await get_ledger_dataframe(
            as_of_date=as_of_date,
            collection_name=collection_name,
        )

    if df.empty:
        return {
            "Revenue": defaultdict(dict),
            "Expenses": defaultdict(dict),
            "Net Income": {
                "Total": {"sats": 0.0, "msats": 0.0, "hive": 0.0, "hbd": 0.0, "usd": 0.0}
            },
        }

    # Get spot rates from the latest entry
    latest_entry = df.iloc[-1]
    conv_usd = latest_entry["credit_conv_usd"]
    conv_hbd = latest_entry["credit_conv_hbd"]
    conv_hive = latest_entry["credit_conv_hive"]
    conv_sats = latest_entry["credit_conv_sats"]
    conv_msats = latest_entry["credit_conv_msats"]

    spot_rates = {
        "sats_to_usd": conv_usd / conv_sats if conv_sats != 0 else 0.0,
        "msats_to_usd": conv_usd / conv_msats if conv_msats != 0 else 0.0,
        "hive_to_usd": conv_usd / conv_hive if conv_hive != 0 else 0.0,
        "hbd_to_usd": conv_usd / conv_hbd if conv_hbd != 0 else 1.0,
    }

    # Process debits and credits
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

    # Apply sign changes
    def sign_change(row):
        ans = 1 if row["account_type"] in ["Asset", "Expense"] else -1
        if row["contra"]:
            ans *= -1
        return ans

    debit_df["amount_adj"] = debit_df.apply(lambda row: row["amount"] * sign_change(row), axis=1)
    debit_df["usd_adj"] = debit_df.apply(lambda row: row["conv_usd"] * sign_change(row), axis=1)

    credit_df["amount_adj"] = credit_df.apply(
        lambda row: -row["amount"] * sign_change(row), axis=1
    )
    credit_df["usd_adj"] = credit_df.apply(lambda row: -row["conv_usd"] * sign_change(row), axis=1)

    # Combine and filter for Revenue and Expense accounts
    combined_df = pd.concat([debit_df, credit_df], ignore_index=True)
    pl_df = combined_df[combined_df["account_type"].isin(["Revenue", "Expense"])].copy()

    # Aggregate by account name, sub-account, and unit
    pl_df = (
        pl_df.groupby(["name", "sub", "account_type", "unit"])
        .agg({"amount_adj": "sum", "usd_adj": "sum"})
        .reset_index()
    )

    # Initialize P&L report
    pl_report = {
        "Revenue": defaultdict(dict),
        "Expenses": defaultdict(dict),
        "Net Income": defaultdict(dict),
    }

    # Process Revenue and Expenses in msats
    for _, row in pl_df.iterrows():
        name, sub, account_type, unit = row["name"], row["sub"], row["account_type"], row["unit"]
        amount = row["amount_adj"]
        category = "Revenue" if account_type == "Revenue" else "Expenses"

        if sub not in pl_report[category][name]:
            pl_report[category][name][sub] = {
                "sats": 0.0,
                "msats": 0.0,
                "hive": 0.0,
                "hbd": 0.0,
                "usd": 0.0,
            }

        msats = amount if unit.lower() == "msats" else amount * 1000
        usd = msats * spot_rates["msats_to_usd"] if spot_rates["msats_to_usd"] != 0 else 0.0

        pl_report[category][name][sub]["msats"] += msats
        pl_report[category][name][sub]["sats"] += msats / 1000
        pl_report[category][name][sub]["usd"] += usd
        pl_report[category][name][sub]["hive"] += (
            usd / spot_rates["hive_to_usd"] if spot_rates["hive_to_usd"] != 0 else 0.0
        )
        pl_report[category][name][sub]["hbd"] += (
            usd / spot_rates["hbd_to_usd"] if spot_rates["hbd_to_usd"] != 0 else 0.0
        )

    # Calculate totals for Revenue and Expenses
    for category in ["Revenue", "Expenses"]:
        for account_name in pl_report[category]:
            sub_accounts = pl_report[category][account_name]
            total_msats = sum(sub_acc["msats"] for sub_acc in sub_accounts.values())
            total_usd = (
                total_msats * spot_rates["msats_to_usd"]
                if spot_rates["msats_to_usd"] != 0
                else 0.0
            )
            pl_report[category][account_name]["Total"] = {
                "sats": round(total_msats / 1000, 5),
                "msats": round(total_msats, 5),
                "hive": round(total_usd / spot_rates["hive_to_usd"], 5)
                if spot_rates["hive_to_usd"] != 0
                else 0.0,
                "hbd": round(total_usd / spot_rates["hbd_to_usd"], 5)
                if spot_rates["hbd_to_usd"] != 0
                else 0.0,
                "usd": round(total_usd, 5),
            }

    # Calculate Net Income by sub-account and total
    sub_net_income = defaultdict(
        lambda: {"sats": 0.0, "msats": 0.0, "hive": 0.0, "hbd": 0.0, "usd": 0.0}
    )
    for sub in set(
        sub for acc in pl_report["Revenue"].values() for sub in acc.keys() if sub != "Total"
    ).union(sub for acc in pl_report["Expenses"].values() for sub in acc.keys() if sub != "Total"):
        revenue_msats = sum(
            pl_report["Revenue"][acc][sub]["msats"]
            for acc in pl_report["Revenue"]
            if sub in pl_report["Revenue"][acc] and sub != "Total"
        )
        expense_msats = sum(
            pl_report["Expenses"][acc][sub]["msats"]
            for acc in pl_report["Expenses"]
            if sub in pl_report["Expenses"][acc] and sub != "Total"
        )
        net_msats = revenue_msats - expense_msats
        net_usd = (
            net_msats * spot_rates["msats_to_usd"] if spot_rates["msats_to_usd"] != 0 else 0.0
        )
        sub_net_income[sub] = {
            "sats": round(net_msats / 1000, 5),
            "msats": round(net_msats, 5),
            "hive": round(net_usd / spot_rates["hive_to_usd"], 5)
            if spot_rates["hive_to_usd"] != 0
            else 0.0,
            "hbd": round(net_usd / spot_rates["hbd_to_usd"], 5)
            if spot_rates["hbd_to_usd"] != 0
            else 0.0,
            "usd": round(net_usd, 5),
        }

    # Total Net Income
    total_revenue_msats = sum(
        acc["Total"]["msats"] for acc in pl_report["Revenue"].values() if "Total" in acc
    )
    total_expenses_msats = sum(
        acc["Total"]["msats"] for acc in pl_report["Expenses"].values() if "Total" in acc
    )
    net_msats = total_revenue_msats - total_expenses_msats
    net_usd = net_msats * spot_rates["msats_to_usd"] if spot_rates["msats_to_usd"] != 0 else 0.0
    pl_report["Net Income"] = {sub: values for sub, values in sub_net_income.items()}
    pl_report["Net Income"]["Total"] = {
        "sats": round(net_msats / 1000, 5),
        "msats": round(net_msats, 5),
        "hive": round(net_usd / spot_rates["hive_to_usd"], 5)
        if spot_rates["hive_to_usd"] != 0
        else 0.0,
        "hbd": round(net_usd / spot_rates["hbd_to_usd"], 5)
        if spot_rates["hbd_to_usd"] != 0
        else 0.0,
        "usd": round(net_usd, 5),
    }

    return pl_report


def profit_and_loss_printout(
    pl_report: dict, as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1)
) -> str:
    """
    Formats a Profit and Loss report, displaying SATS, msats, HIVE, HBD, and USD, with Net Income by sub-account and total.

    Args:
        pl_report (dict): The P&L report dictionary from generate_profit_and_loss_report.
        as_of_date (datetime, optional): End date for the report period.

    Returns:
        str: A formatted string representation of the P&L report.
    """
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
            f"{'Total ' + account_name:<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
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
            f"{'Total ' + account_name:<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
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
        f"{'Total Net Income':<40} {'':<17} {total.get('sats', 0):>10,.0f} {total.get('msats', 0):>12,.0f} {total.get('hive', 0):>12,.3f} {total.get('hbd', 0):>12,.3f} {total.get('usd', 0):>12,.2f}"
    )
    output.append("=" * max_width)

    return "\n".join(output)
