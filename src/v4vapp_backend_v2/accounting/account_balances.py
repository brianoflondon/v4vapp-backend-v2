from asyncio import TaskGroup
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd

from v4vapp_backend_v2.accounting.accounting_classes import (
    AccountBalanceSummary,
    ConvertedSummary,
    LedgerConvSummary,
    LightningLimitSummary,
    UnitSummary,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LedgerAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_dataframe
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import (
    filter_sum_credit_debit_pipeline,
    list_all_accounts_pipeline,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db import get_mongodb_client_defaults
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, truncate_text
from v4vapp_backend_v2.hive.v4v_config import V4VConfig

UNIT_TOLERANCE = {
    "HIVE": 0.001,
    "HBD": 0.001,
    "MSATS": 10,
}


async def get_account_balance(
    account: LedgerAccount,
    df: pd.DataFrame = pd.DataFrame(),
    full_history: bool = False,
    as_of_date: datetime | None = None,
) -> pd.DataFrame:
    """
    Asynchronously retrieves and computes the account balance transactions for a given ledger account.

    Args:
        account (LedgerAccount): The ledger account for which to retrieve the balance.
        df (pd.DataFrame, optional): An optional DataFrame containing ledger transactions. If not provided or empty, transactions will be fetched.
        full_history (bool, optional): If True, retrieves the full transaction history. (Currently unused in this function.)
        as_of_date (datetime | None, optional): The cutoff date for transactions. Defaults to the current UTC time if not provided.

    Returns:
        pd.DataFrame: A DataFrame containing the account's debit and credit transactions, with signed amounts calculated according to account type.
        If no transactions are found, returns an empty DataFrame.

    Notes:
        - The function filters transactions for the specified account and sub-account (if provided).
        - Signed amounts are positive for debits to Asset accounts and negative for credits; the opposite applies for other account types.
        - The resulting DataFrame includes both debit and credit transactions, sorted by timestamp.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    if df.empty:
        df = await get_ledger_dataframe(
            as_of_date=as_of_date,
            filter_by_account=account,
        )

    if df.empty:
        logger.debug(
            f"No transactions found for account {account.name} up to {as_of_date}.",
            extra={"notification": False, "account": account.name, "as_of_date": as_of_date},
        )
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
            "ledger_type",
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
            "ledger_type",
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


async def get_account_balance_printout(
    account: LedgerAccount,
    df: pd.DataFrame = pd.DataFrame(),
    line_items: bool = False,
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
    max_width = 135
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc) + timedelta(hours=1)

    combined_df = await get_account_balance(
        account=account,
        df=df,
        full_history=line_items,
        as_of_date=as_of_date,
    )
    if combined_df.empty:
        logger.info(f"No transactions found for account {account.name} up to {as_of_date}.")
        return "No transactions found for this account up to today.", AccountBalanceSummary()

    # Group by unit (process debit_unit and credit_unit separately)
    units = set(combined_df["debit_unit"].dropna().unique()).union(
        set(combined_df["credit_unit"].dropna().unique())
    )
    title_line = f"Balance for {account}"
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
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

        # Fill NaN in conv fields with 0 to avoid NaN propagation
        conv_columns = [
            "debit_conv_hive",
            "debit_conv_hbd",
            "debit_conv_usd",
            "debit_conv_sats",
            "debit_conv_msats",
            "credit_conv_hive",
            "credit_conv_hbd",
            "credit_conv_usd",
            "credit_conv_sats",
            "credit_conv_msats",
        ]
        unit_df[conv_columns] = unit_df[conv_columns].fillna(0)

        # Calculate running balance for this unit
        unit_df["running_balance"] = unit_df["signed_amount"].cumsum()

        # Determine display unit: if MSATS, display as SATS
        display_unit = "SATS" if unit.upper() == "MSATS" else unit.upper()
        conversion_factor = 1000 if unit.upper() == "MSATS" else 1  # Convert MSATS to SATS

        # Format output for this unit
        output.append(f"\nUnit: {display_unit}")
        output.append("-" * 10)
        if line_items:
            for _, row in unit_df.iterrows():
                contra_str = (
                    "(-)"
                    if (pd.notna(row["credit_contra"]) and row["credit_contra"])
                    or (pd.notna(row["debit_contra"]) and row["debit_contra"])
                    else "   "
                )
                timestamp = row["timestamp"].strftime("%Y-%m-%d %H:%M")
                description = truncate_text(row["description"], 45)
                ledger_type = row["ledger_type"]
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
                    f"{timestamp:<18} "
                    f"{description:<45} "
                    f"{contra_str} "
                    f"{debit_str:>12} "
                    f"{credit_str:>12} "
                    f"{balance_str:>12} "
                    f"{short_id:>15} "
                    f"{ledger_type:>11}"
                )
                summary.line_items.append(line)
                output.append(line)

        # Get the final balance for this unit and calculate converted values
        final_balance = unit_df["running_balance"].iloc[-1]
        unit_balances[unit] = final_balance

        if abs(final_balance) > UNIT_TOLERANCE.get(unit, 0):
            # Sum signed conversions row by row
            total_hive = 0.0
            total_hbd = 0.0
            total_usd_for_unit = 0.0
            total_sats_for_unit = 0.0
            total_msats = 0.0

            for _, row in unit_df.iterrows():
                sign_factor = 1 if row["signed_amount"] > 0 else -1

                conv_hive = sign_factor * max(row["debit_conv_hive"], row["credit_conv_hive"])
                conv_hbd = sign_factor * max(row["debit_conv_hbd"], row["credit_conv_hbd"])
                conv_usd = sign_factor * max(row["debit_conv_usd"], row["credit_conv_usd"])
                conv_sats = sign_factor * max(row["debit_conv_sats"], row["credit_conv_sats"])
                conv_msats = sign_factor * max(row["debit_conv_msats"], row["credit_conv_msats"])

                total_hive += conv_hive
                total_hbd += conv_hbd
                total_usd_for_unit += conv_usd
                total_sats_for_unit += conv_sats
                total_msats += conv_msats

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
            summary.total_hive += total_hive
            summary.total_hbd += total_hbd
            summary.total_msats += total_msats

            output.append("-" * max_width)
            output.append(
                f"{'Converted    ':<10} "
                f"{total_hive:>15,.3f} HIVE "
                f"{total_hbd:>12,.3f} HBD "
                f"{total_usd_for_unit:>12,.3f} USD "
                f"{total_sats_for_unit:>12,.0f} SATS "
                f"{total_msats:>16,.0f} msats"
            )
        else:
            total_usd_for_unit = 0.0
            total_sats_for_unit = 0.0

        output.append("-" * max_width)
        # Display final balance in SATS if unit is MSATS
        display_balance = (
            final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
        )
        balance_str = (
            f"{display_balance:,.0f}" if unit.upper() == "MSATS" else f"{display_balance:>10,.3f}"
        )
        output.append(f"{'Final Balance':<18} {balance_str:>10} {display_unit:<5}")

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


async def get_all_accounts(
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
) -> Dict[LedgerAccount, AccountBalanceSummary]:
    """
    Fetches the balance summaries for all accounts as of a specified date.

    This asynchronous function retrieves all accounts from the ledger, computes their balances as of the given date,
    and returns a dictionary mapping account identifiers to their respective balance summaries.

        as_of_date (datetime, optional): The cutoff date for fetching account balances. Defaults to the current UTC datetime.

        Dict[LedgerAccount, AccountBalanceSummary]: A dictionary where each key is an account identifier and each value is an
        AccountBalanceSummary object representing the account's balance summary as of the specified date.

    Raises:
        Exception: Logs and skips any account for which balance computation fails.

    """
    accounts = await list_all_accounts()
    ledger_df = await get_ledger_dataframe(
        as_of_date=as_of_date,
        filter_by_account=None,  # No specific account filter
    )
    try:
        async with TaskGroup() as tg:
            tasks = {
                account: tg.create_task(
                    get_account_balance_printout(
                        account=account,
                        df=ledger_df,
                        line_items=False,
                        as_of_date=as_of_date,
                    )
                )
                for account in accounts
            }
    except Exception as e:
        logger.exception(
            f"Error creating tasks for accounts: {e}",
            extra={"notification": False},
        )
        return {}
    result = {}
    for account, task in tasks.items():
        try:
            output_text, summary = await task
            summary.output_text = None  # Remove output_text from summary
            result[account] = summary
        except Exception as e:
            logger.error(
                f"Error processing account {account}: {e}",
                extra={"notification": False, "account": account, "error": str(e)},
            )
    return result


async def ledger_pipeline_result(
    cust_id: str,
    account: LedgerAccount,
    pipeline: List[Mapping[str, Any]],
    age: timedelta | None = None,
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
) -> LedgerConvSummary:
    """
    Executes a MongoDB aggregation pipeline and returns the result as a LedgerConvSummary.
    THIS DOES NOT ACCOUNT FOR THE NEGATIVE/POSITIVE AMOUNT FOR DEBITS AND CREDITS

    Args:
        pipeline (Mapping[str, any]): The aggregation pipeline to execute.

    Returns:
        LedgerConvSummary: The result of the aggregation as a LedgerConvSummary.
    """
    # Get a brand new MongoDB client with defaults
    db_client = get_mongodb_client_defaults()
    collection = await db_client.get_collection("ledger")
    cursor = collection.aggregate(pipeline=pipeline)
    ans = LedgerConvSummary(
        cust_id=cust_id,
        as_of_date=as_of_date,
        account=account,
    )
    ans.age = age if age else None
    async for entry in cursor:
        totals_list = entry.get("total", [])
        if not totals_list:
            return ans
        totals = totals_list[0]
        ans = LedgerConvSummary(
            cust_id=cust_id,
            hive=totals.get("credit_total_hive", 0.0),
            hbd=totals.get("credit_total_hbd", 0.0),
            usd=totals.get("credit_total_usd", 0.0),
            sats=totals.get("credit_total_sats", 0.0),
            msats=totals.get("credit_total_msats", 0.0),
        )
        ans.age = age if age else None
        for item in entry.get("by_ledger_type", []):  # Get as a list, not a dict
            ledger_type = item.get("_id", "unknown")  # Get the ledger type from _id
            ans.by_ledger_type[ledger_type] = ConvertedSummary(
                hive=item.get("credit_total_hive", 0.0),
                hbd=item.get("credit_total_hbd", 0.0),
                usd=item.get("credit_total_usd", 0.0),
                sats=item.get("credit_total_sats", 0.0),
                msats=item.get("credit_total_msats", 0.0),
            )
        for item in entry.get("line_items", []):
            ans.ledger_entries.append(item)
    return ans


async def get_account_lightning_conv(
    cust_id: str = "",
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
    age: timedelta = timedelta(hours=4),
    line_items: bool = False,
) -> LedgerConvSummary:
    """
    Retrieves the lightning conversion for a specific customer as of a given date.
    This adds up transactions of type LIGHTNING_OUT and DEPOSIT_KEEPSATS & WITHDRAW_KEEPSATS,
    i.e. conversions from HIVE/HBD to SATS.
    THIS DOES NOT ACCOUNT FOR THE NEGATIVE/POSITIVE AMOUNT FOR DEBITS AND CREDITS

    Args:
        account (LedgerAccount): The account for which to retrieve the lightning spend.
        as_of_date (datetime, optional): The date up to which to calculate the spend. Defaults to the current UTC time.

    Returns:
        Tuple[str, AccountBalanceSummary]: A tuple containing a formatted string of the lightning spend and an AccountBalanceSummary object.
    """

    hive_config = InternalConfig().config.hive
    server_account, treasury_account, funding_account, exchange_account = (
        hive_config.all_account_names
    )
    # This account is the transit point through which all keepsats and conversions happen.
    account = AssetAccount(
        name="Customer Deposits Hive",
        sub=server_account,
    )

    pipeline = filter_sum_credit_debit_pipeline(
        account=account,
        cust_id=cust_id,
        age=age,
        as_of_date=as_of_date,
        ledger_types=[
            LedgerType.CONV_HIVE_TO_KEEPSATS,
            LedgerType.CONV_KEEPSATS_TO_HIVE,
            LedgerType.CONV_HIVE_TO_LIGHTNING,
            LedgerType.CONV_LIGHTNING_TO_HIVE,
        ],
        line_items=line_items,
    )
    ans = await ledger_pipeline_result(
        cust_id=cust_id,
        age=age,
        account=account,
        pipeline=pipeline,
        as_of_date=as_of_date,
    )
    return ans


async def check_hive_conversion_limits(
    hive_accname: str, extra_spend_sats: int = 0, line_items: bool = False
) -> List[LightningLimitSummary]:
    v4v_config = V4VConfig()
    lightning_rate_limits = v4v_config.data.lightning_rate_limits
    ans = []
    if not lightning_rate_limits:
        logger.warning(
            "Lightning rate limits are not configured. Skipping lightning spend checks.",
            extra={"notification": False, "hive_accname": hive_accname},
        )
        return []

    for limit in lightning_rate_limits:
        age = timedelta(hours=limit.hours)
        limit_timedelta = timedelta(hours=limit.hours)
        limit_timedelta_str = format_time_delta(
            limit_timedelta, fractions=True, just_days_or_hours=True
        )
        lightning_conv = await get_account_lightning_conv(
            cust_id=hive_accname, age=age, line_items=line_items
        )
        limit_summary = LightningLimitSummary(
            conv_summary=lightning_conv,
            total_sats=lightning_conv.sats,
            total_msats=lightning_conv.msats,
            output_text=(
                f"Lightning conversions for {hive_accname} in the last {limit_timedelta_str} : "
                f"{lightning_conv.sats:,.0f} sats (limit: {limit.sats:,.0f} sats)\n"
            ),
            limit_ok=(lightning_conv.sats + extra_spend_sats) <= limit.sats,
        )
        ans.append(limit_summary)
    return ans


async def get_keepsats_balance(
    cust_id: str = "",
    as_of_date: datetime = datetime.now(tz=timezone.utc) + timedelta(hours=1),
    line_items: bool = False,
) -> LedgerConvSummary:
    """
    Retrieves the balance of Keepsats for a specific customer as of a given date.
    This looks at the `credit` values because credits to a Liability account
    represent deposits, while debits represent withdrawals.
    Adds a net_balance field to the output summing up deposits and withdrawls

    Args:
        cust_id (str): The customer ID for which to retrieve the Keepsats balance.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to the current UTC time.

    Returns:
        AccountBalanceSummary: An object containing the balance summary for the specified customer.
    """
    account = LiabilityAccount(
        name="Customer Liability",
        sub=cust_id,
    )
    pipeline = filter_sum_credit_debit_pipeline(
        account=account,
        cust_id=cust_id,
        as_of_date=as_of_date,
        ledger_types=[
            LedgerType.DEPOSIT_KEEPSATS,
            LedgerType.WITHDRAW_KEEPSATS,
            LedgerType.CONV_KEEPSATS_TO_HIVE,
            LedgerType.HOLD_KEEPSATS,
        ],
        line_items=line_items,
    )
    ans = await ledger_pipeline_result(
        cust_id=cust_id,
        account=account,
        pipeline=pipeline,
    )
    if line_items:
        ledger_entries = []
        for item in ans.ledger_entries:
            ledger_entry = LedgerEntry.model_validate(item)
            ledger_entries.append(ledger_entry)
        ans.ledger_entries = ledger_entries

    deposit_balance = ans.by_ledger_type.get(LedgerType.DEPOSIT_KEEPSATS.value, ConvertedSummary())
    withdraw_balance = ans.by_ledger_type.get(
        LedgerType.WITHDRAW_KEEPSATS.value, ConvertedSummary()
    )
    net_balance = deposit_balance - withdraw_balance
    ans.net_balance = net_balance

    return ans
