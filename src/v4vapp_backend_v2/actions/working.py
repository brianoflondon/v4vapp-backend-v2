import pandas as pd
from datetime import datetime
from typing import Dict, List
from pydantic import BaseModel, Field


# Ledger entry model (simplified for brevity)
class Conv(BaseModel):
    hive: float
    hbd: float
    usd: float
    sats: float
    msats: float
    msats_fee: float
    btc: float
    sats_hive: float
    sats_hbd: float
    conv_from: str
    value: float
    source: str
    fetch_date: datetime
    in_limits: bool


class Account(BaseModel):
    name: str
    account_type: str
    sub: str


class Op(BaseModel):
    locked: bool
    realm: str
    trx_id: str
    op_in_trx: int
    type: str
    block_num: int
    trx_num: int
    timestamp: datetime
    extensions: List
    from_: str = Field(..., alias="from")
    to: str
    amount: Dict
    memo: str
    conv: Conv
    d_memo: str
    link: str


class LedgerEntry(BaseModel):
    _id: Dict
    group_id: str
    timestamp: datetime
    description: str
    amount: float
    unit: str
    conv: Conv
    debit: Account
    credit: Account
    op: Op


# Provided ledger entries
ledger_entries_data = [
    {
        "_id": {"$oid": "6820e2df890d432b2e700fe9"},
        "group_id": "95793083_8fc6b78d32c8c8050aae9117915b3c17757f65b6_1_real",
        "timestamp": {"$date": "2025-05-11T09:58:00.000Z"},
        "description": "Opening Balance for testing v2 backend",
        "amount": 100,
        "unit": "hive",
        "conv": {
            "hive": 100,
            "hbd": 26.390002,
            "usd": 26.390002,
            "sats": 25391,
            "msats": 25391180,
            "msats_fee": 481650,
            "btc": 0.0002539118,
            "sats_hive": 253.9118,
            "sats_hbd": 962.1515,
            "conv_from": "hive",
            "value": 100,
            "source": "Binance",
            "fetch_date": {"$date": "2025-05-11T17:47:51.986Z"},
            "in_limits": True,
        },
        "debit": {"name": "Treasury Hive", "account_type": "Asset", "sub": "devtre.v4vapp"},
        "credit": {
            "name": "Owner Loan Payable (funding)",
            "account_type": "Liability",
            "sub": "v4vapp.dhf",
        },
        "op": {
            "locked": True,
            "realm": "real",
            "trx_id": "8fc6b78d32c8c8050aae9117915b3c17757f65b6",
            "op_in_trx": 1,
            "type": "transfer",
            "block_num": 95793083,
            "trx_num": 13,
            "timestamp": {"$date": "2025-05-11T09:58:00.000Z"},
            "extensions": [],
            "from": "v4vapp.dhf",
            "to": "devtre.v4vapp",
            "amount": {"amount": "100000", "nai": "@@000000021", "precision": 3},
            "memo": "Opening Balance for testing v2 backend",
            "conv": {
                "hive": 100,
                "hbd": 26.390002,
                "usd": 26.390002,
                "sats": 25391,
                "msats": 25391180,
                "msats_fee": 481650,
                "btc": 0.0002539118,
                "sats_hive": 253.9118,
                "sats_hbd": 962.1515,
                "conv_from": "hive",
                "value": 100,
                "source": "Binance",
                "fetch_date": {"$date": "2025-05-11T17:47:51.986Z"},
                "in_limits": True,
            },
            "d_memo": "Opening Balance for testing v2 backend",
            "link": "https://hivehub.dev/tx/8fc6b78d32c8c8050aae9117915b3c17757f65b6",
        },
    },
    {
        "_id": {"$oid": "6820e3ce890d432b2e700fea"},
        "group_id": "95801581_3284a7bf2900835e8b0d6d25fd235f38192f2dee_1_real",
        "timestamp": {"$date": "2025-05-11T17:03:15.000Z"},
        "description": "Opening Balance for testing v2 backend HBD",
        "amount": 50,
        "unit": "hbd",
        "conv": {
            "hive": 189.46569,
            "hbd": 50,
            "usd": 50,
            "sats": 48108,
            "msats": 48107575,
            "msats_fee": 867828,
            "btc": 0.00048107575,
            "sats_hive": 253.9118,
            "sats_hbd": 962.1515,
            "conv_from": "hbd",
            "value": 50,
            "source": "Binance",
            "fetch_date": {"$date": "2025-05-11T17:47:51.986Z"},
            "in_limits": True,
        },
        "debit": {"name": "Treasury Hive", "account_type": "Asset", "sub": "devtre.v4vapp"},
        "credit": {
            "name": "Owner Loan Payable (funding)",
            "account_type": "Liability",
            "sub": "v4vapp.dhf",
        },
        "op": {
            "locked": True,
            "realm": "real",
            "trx_id": "3284a7bf2900835e8b0d6d25fd235f38192f2dee",
            "op_in_trx": 1,
            "type": "transfer",
            "block_num": 95801581,
            "trx_num": 19,
            "timestamp": {"$date": "2025-05-11T17:03:15.000Z"},
            "extensions": [],
            "from": "v4vapp.dhf",
            "to": "devtre.v4vapp",
            "amount": {"amount": "50000", "nai": "@@000000013", "precision": 3},
            "memo": "Opening Balance for testing v2 backend HBD",
            "conv": {
                "hive": 189.46569,
                "hbd": 50,
                "usd": 50,
                "sats": 48108,
                "msats": 48107575,
                "msats_fee": 867828,
                "btc": 0.00048107575,
                "sats_hive": 253.9118,
                "sats_hbd": 962.1515,
                "conv_from": "hbd",
                "value": 50,
                "source": "Binance",
                "fetch_date": {"$date": "2025-05-11T17:47:51.986Z"},
                "in_limits": True,
            },
            "d_memo": "Opening Balance for testing v2 backend HBD",
            "link": "https://hivehub.dev/tx/3284a7bf2900835e8b0d6d25fd235f38192f2dee",
        },
    },
    {
        "_id": {"$oid": "6820e5f7890d432b2e700feb"},
        "group_id": "95802587_6472af41e3236308d08318594e977e18e4de572b_1_real",
        "timestamp": {"$date": "2025-05-11T17:53:45.000Z"},
        "description": "Send opening balance to Live Server",
        "amount": 50,
        "unit": "hive",
        "conv": {
            "hive": 50,
            "hbd": 13.200001,
            "usd": 13.200001,
            "sats": 12687,
            "msats": 12687145,
            "msats_fee": 265681,
            "btc": 0.00012687145,
            "sats_hive": 253.7429,
            "sats_hbd": 961.1473,
            "conv_from": "hive",
            "value": 50,
            "source": "Binance",
            "fetch_date": {"$date": "2025-05-11T18:01:27.108Z"},
            "in_limits": True,
        },
        "debit": {
            "name": "Customer Deposits Hive",
            "account_type": "Asset",
            "sub": "devser.v4vapp",
        },
        "credit": {"name": "Treasury Hive", "account_type": "Asset", "sub": "devtre.v4vapp"},
        "op": {
            "locked": True,
            "realm": "real",
            "trx_id": "6472af41e3236308d08318594e977e18e4de572b",
            "op_in_trx": 1,
            "type": "transfer",
            "block_num": 95802587,
            "trx_num": 4,
            "timestamp": {"$date": "2025-05-11T17:53:45.000Z"},
            "extensions": [],
            "from": "devtre.v4vapp",
            "to": "devser.v4vapp",
            "amount": {"amount": "50000", "nai": "@@000000021", "precision": 3},
            "memo": "Send opening balance to Live Server",
            "conv": {
                "hive": 50,
                "hbd": 13.200001,
                "usd": 13.200001,
                "sats": 12687,
                "msats": 12687145,
                "msats_fee": 265681,
                "btc": 0.00012687145,
                "sats_hive": 253.7429,
                "sats_hbd": 961.1473,
                "conv_from": "hive",
                "value": 50,
                "source": "Binance",
                "fetch_date": {"$date": "2025-05-11T18:01:27.108Z"},
                "in_limits": True,
            },
            "d_memo": "Send opening balance to Live Server",
            "link": "https://hivehub.dev/tx/6472af41e3236308d08318594e977e18e4de572b",
        },
    },
]

# Convert ledger entries to LedgerEntry objects
ledger_entries = [LedgerEntry(**entry) for entry in ledger_entries_data]

# Create Pandas DataFrame
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

# Filter entries by date
as_of_date = datetime(2025, 5, 11, 23, 59, 59)
df = df[df["timestamp"] <= as_of_date]


# Truncate text function
def truncate_text(text: str, max_length: int) -> str:
    if len(text) > max_length:
        return text[: max_length - 3] + "..."
    return text


# Generate balance sheet using Pandas
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
            "conv_hive",
            "conv_hbd",
            "conv_usd",
        ]
    ].copy()
    debit_df["sats"] = debit_df["conv_sats"].where(
        debit_df["debit_account_type"] == "Asset", -debit_df["conv_sats"]
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
            "conv_hive",
            "conv_hbd",
            "conv_usd",
        ]
    ].copy()
    credit_df["sats"] = -credit_df["conv_sats"].where(
        credit_df["credit_account_type"] == "Asset", credit_df["conv_sats"]
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
        combined_df.groupby(["name", "sub", "account_type"])[["sats", "hive", "hbd", "usd"]]
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
                "sats": round(row["sats"], 2),
                "hive": round(row["hive"], 2),
                "hbd": round(row["hbd"], 2),
                "usd": round(row["usd"], 2),
            }
        elif account_type == "Liability":
            balance_sheet["Liabilities"][name][sub] = {
                "sats": round(row["sats"], 2),
                "hive": round(row["hive"], 2),
                "hbd": round(row["hbd"], 2),
                "usd": round(row["usd"], 2),
            }
        elif account_type == "Equity":
            balance_sheet["Equity"][name][sub] = {
                "sats": round(row["sats"], 2),
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
                "sats": round(total_sats, 2),
                "hive": round(total_hive, 2),
                "hbd": round(total_hbd, 2),
                "usd": round(total_usd, 2),
            }
        total_sats = sum(
            acc["Total"]["sats"] for acc in balance_sheet[category].values() if "Total" in acc
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
            "hive": round(total_hive, 2),
            "hbd": round(total_hbd, 2),
            "usd": round(total_usd, 2),
        }

    balance_sheet["Total Liabilities and Equity"] = {
        "sats": round(
            balance_sheet["Liabilities"]["Total"]["sats"]
            + balance_sheet["Equity"]["Total"]["sats"],
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
    output = ["Balance Sheet in All Currencies"]
    output.append("-" * 100)
    output.append(f"{'Account':<30} {'Sub':<20} {'SATS':>12} {'HIVE':>12} {'HBD':>12} {'USD':>12}")
    output.append("-" * 100)

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
                            f"{balance['sats']:>12,.2f} "
                            f"{balance['hive']:>12,.2f} "
                            f"{balance['hbd']:>12,.2f} "
                            f"{balance['usd']:>12,.2f}"
                        )
                total = sub_accounts["Total"]
                output.append(
                    f"{'Total ' + truncate_text(account_name, 24):<30} "
                    f"{'':<20} "
                    f"{total['sats']:>12,.2f} "
                    f"{total['hive']:>12,.2f} "
                    f"{total['hbd']:>12,.2f} "
                    f"{total['usd']:>12,.2f}"
                )
        total = balance_sheet[category]["Total"]
        output.append(
            f"{'Total ' + category:<30} "
            f"{'':<20} "
            f"{total['sats']:>12,.2f} "
            f"{total['hive']:>12,.2f} "
            f"{total['hbd']:>12,.2f} "
            f"{total['usd']:>12,.2f}"
        )

    total = balance_sheet["Total Liabilities and Equity"]
    output.append("-" * 100)
    output.append(
        f"{'Total Liab. & Equity':<30} "
        f"{'':<20} "
        f"{total['sats']:>12,.2f} "
        f"{total['hive']:>12,.2f} "
        f"{total['hbd']:>12,.2f} "
        f"{total['usd']:>12,.2f}"
    )

    return "\n".join(output)


# Generate balance sheet
balance_sheet = generate_balance_sheet_pandas(df)

# Print formatted balance sheet (USD only)
balance_sheet_str = format_balance_sheet(balance_sheet, as_of_date)
print(balance_sheet_str)

# Print all currencies table
print("\n")
print(format_all_currencies(balance_sheet))

# Print DataFrame to show original currency
print("\nLedger Entries with Original Currency:")
print(
    df[
        [
            "group_id",
            "description",
            "amount",
            "unit",
            "conv_usd",
            "debit_name",
            "debit_sub",
            "credit_name",
            "credit_sub",
        ]
    ].to_string(index=False)
)
