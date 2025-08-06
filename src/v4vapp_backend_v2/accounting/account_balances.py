from datetime import datetime, timedelta, timezone
from typing import Any, List, Mapping, Tuple

from v4vapp_backend_v2.accounting.account_balance_pipelines import (
    all_account_balances_pipeline,
    list_all_accounts_pipeline,
)
from v4vapp_backend_v2.accounting.accounting_classes import (
    AccountBalances,
    ConvertedSummary,
    LedgerAccountDetails,
    LedgerConvSummary,
    LightningLimitSummary,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LedgerAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import (
    filter_sum_credit_debit_pipeline,
)
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    format_time_delta,
    lightning_memo,
    truncate_text,
)
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.models.pydantic_helpers import convert_datetime_fields

UNIT_TOLERANCE = {
    "HIVE": 0.001,
    "HBD": 0.001,
    "MSATS": 10,
}


# @async_time_stats_decorator()
async def all_account_balances(
    as_of_date: datetime = datetime.now(tz=timezone.utc), age: timedelta | None = None
) -> AccountBalances:
    pipeline = all_account_balances_pipeline(as_of_date=as_of_date, age=age)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    clean_results = convert_datetime_fields(results)

    account_balances = AccountBalances.model_validate(clean_results)

    return account_balances


# @async_time_stats_decorator()
async def one_account_balance(
    account: LedgerAccount | str,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta | None = None,
) -> LedgerAccountDetails:
    if isinstance(account, str):
        account = LiabilityAccount(
            name="Customer Liability",
            sub=account,
        )

    pipeline = all_account_balances_pipeline(account=account, as_of_date=as_of_date, age=age)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    if not results:
        logger.warning(f"No results for {account}", extra={"notification": False})
    clean_results = convert_datetime_fields(results)
    account_balance = AccountBalances.model_validate(clean_results)
    return (
        account_balance.root[0]
        if account_balance.root
        else LedgerAccountDetails(
            name=account.name,
            account_type=account.account_type,
            sub=account.sub,
            contra=account.contra,
        )
    )


# @async_time_stats_decorator()
async def account_balance_printout(
    account: LedgerAccount | str,
    line_items: bool = True,
    user_memos: bool = True,
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta = timedelta(seconds=0),
) -> Tuple[str, LedgerAccountDetails]:
    """
    Calculate and display the balance for a specified account (and optional sub-account) from the DataFrame.
    Optionally lists all debit and credit transactions up to today, or shows only the closing balance.
    Properly accounts for assets and liabilities, and includes converted values to other units
    (SATS, HIVE, HBD, USD, msats).

    Args:
        account (Account | str): An Account object specifying the account name, type, and optional sub-account. If
        a str is passed, we assume this is a `Customer Liability` account for customer `account`.
        df (pd.DataFrame): A DataFrame containing transaction data with columns: timestamp, debit_amount, debit_unit, etc.
        full_history (bool, optional): If True, shows the full transaction history with running balances.
                                       If False, shows only the closing balance. Defaults to False.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to None (current date).

    Returns:
        str: A formatted string containing either the full transaction history or the closing balance
             for the specified account and sub-account up to the specified date.
    """
    if isinstance(account, str):
        account = LiabilityAccount(
            name="Customer Liability",
            sub=account,
        )

    max_width = 135
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc) + timedelta(seconds=10)

    ledger_account_details = await one_account_balance(
        account=account, as_of_date=as_of_date, age=age
    )
    units = set(ledger_account_details.balances.keys())

    title_line = f"Balance for {account}"
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
    output.append("-" * max_width)

    if not ledger_account_details.balances:
        output.append("No transactions found for this account up to today.")
        output.append("=" * max_width)
        return "\n".join(output), ledger_account_details

    total_usd = 0.0
    total_sats = 0.0

    for unit in [Currency.HIVE, Currency.HBD, Currency.MSATS]:
        final_balance = 0
        if unit not in units:
            continue
        # Determine display unit: if MSATS, display as SATS
        display_unit = "SATS" if unit.upper() == "MSATS" else unit.upper()
        conversion_factor = 1000 if unit.upper() == "MSATS" else 1  # Convert MSATS to SATS

        # Format output for this unit
        output.append(f"\nUnit: {display_unit}")
        output.append("-" * 10)
        all_rows = ledger_account_details.balances[unit]
        if all_rows:
            # Group transactions by date
            transactions_by_date = {}
            for row in all_rows:
                if row.timestamp:
                    date_str = f"{row.timestamp:%Y-%m-%d}"
                    if date_str not in transactions_by_date:
                        transactions_by_date[date_str] = []
                    transactions_by_date[date_str].append(row)
                else:
                    # Handle entries with no timestamp
                    if "No Date" not in transactions_by_date:
                        transactions_by_date["No Date"] = []
                    transactions_by_date["No Date"].append(row)

            # Display transactions grouped by date
            for date_str, rows in sorted(transactions_by_date.items()):
                # Add date header with a distinctive format
                output.append(f"\n=== {date_str} ===")

                for row in rows:
                    contra_str = "-c-" if row.contra else "   "
                    # Only show time part since date is in the header
                    timestamp = f"{row.timestamp:%H:%M:%S.%f}"[:12] if row.timestamp else "N/A"
                    description = truncate_text(row.description, 45)
                    ledger_type = row.ledger_type
                    debit = row.amount if row.side == "debit" and row.unit == unit else 0.0
                    credit = row.amount if row.side == "credit" and row.unit == unit else 0.0
                    balance = row.amount_running_total
                    short_id = row.short_id
                    if unit.upper() == "MSATS":
                        debit = debit / conversion_factor
                        credit = credit / conversion_factor
                        balance = balance / conversion_factor
                    debit_str = f"{debit:,.0f}" if unit.upper() == "MSATS" else f"{debit:>12,.3f}"
                    credit_str = (
                        f"{credit:,.0f}" if unit.upper() == "MSATS" else f"{credit:>12,.3f}"
                    )
                    balance_str = (
                        f"{balance:,.0f}" if unit.upper() == "MSATS" else f"{balance:>12,.3f}"
                    )
                    line = (
                        f"{timestamp:<14} "  # Shorter timestamp field (time only)
                        f"{description:<49} "
                        f"{contra_str} "
                        f"{debit_str:>12} "
                        f"{credit_str:>12} "
                        f"{balance_str:>12} "
                        f"{short_id:>15} "
                        f"{ledger_type:>11}"
                    )
                    if line_items:
                        output.append(line)
                    if user_memos and row.user_memo:
                        memo = truncate_text(lightning_memo(row.user_memo), 60)
                        output.append(f"{' ' * 14} {memo}")  # Adjusted padding for memo

            final_balance = all_rows[-1].amount_running_total if all_rows else 0.0
            final_conv_balance = (
                all_rows[-1].conv_running_total if all_rows else ConvertedSummary()
            )
            total_hive = final_conv_balance.hive if final_conv_balance else 0.0
            total_hbd = final_conv_balance.hbd if final_conv_balance else 0.0
            total_usd_for_unit = final_conv_balance.usd if final_conv_balance else 0
            total_sats_for_unit = final_conv_balance.sats if final_conv_balance else 0
            total_msats = final_conv_balance.msats if final_conv_balance else 0

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
        output.append(
            f"{'Final Balance ' + f'{display_unit}':<18} {balance_str:>10} {display_unit:<5}"
        )

        total_usd += total_usd_for_unit
        total_sats += total_sats_for_unit

    # WE don't need to do the summing up of totals here, they are performed in the LedgerAccountDetails class
    # assert ledger_account_details.conv_total.usd == total_usd, (
    #     f"Total USD mismatch: {ledger_account_details.conv_total.usd} != {total_usd}"
    # )

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>18,.3f} USD")
    output.append(f"Total SATS: {total_sats:>17,.0f} SATS")
    output.append(title_line)

    output.append("=" * max_width + "\n")
    output_text = "\n".join(output)

    return output_text, ledger_account_details


async def list_all_accounts() -> List[LedgerAccount]:
    """
    Lists all unique accounts in the ledger by aggregating debit and credit accounts.

    Returns:
        List[Account]: A list of unique Account objects sorted by account type, name, and sub-account.
    """
    pipeline = list_all_accounts_pipeline()

    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    accounts = []
    async for doc in cursor:
        account = LedgerAccount.model_validate(doc)
        accounts.append(account)
    return accounts


async def ledger_pipeline_result(
    cust_id: str,
    account: LedgerAccount,
    pipeline: List[Mapping[str, Any]],
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    age: timedelta | None = None,
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
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
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
    as_of_date: datetime = datetime.now(tz=timezone.utc),
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
    """
    Checks if a Hive account's recent Lightning conversions are within configured rate limits.
    Args:
        hive_accname (str): The Hive account name to check conversion limits for.
        extra_spend_sats (int, optional): Additional satoshis to consider in the limit check. Defaults to 0.
        line_items (bool, optional): Whether to include line item details in the conversion summary. Defaults to False.
    Returns:
        List[LightningLimitSummary]: A list of LightningLimitSummary objects, each representing the conversion summary and limit status for a configured time window.
    Raises:
        None
    Notes:
        - If Lightning rate limits are not configured, a warning is logged and an empty list is returned.
        - The function checks conversions for each configured limit window and determines if the account is within limits.
    """

    v4v_config = V4VConfig()
    lightning_rate_limits = v4v_config.data.lightning_rate_limits
    ans = []
    if not lightning_rate_limits:
        logger.warning(
            "Lightning rate limits are not configured. Skipping lightning spend checks.",
            extra={"notification": False, "hive_accname": hive_accname},
        )
        return []

    as_of_date = datetime.now(tz=timezone.utc) + timedelta(seconds=10)
    for limit in lightning_rate_limits:
        age = timedelta(hours=limit.hours)
        limit_timedelta = timedelta(hours=limit.hours)
        limit_timedelta_str = format_time_delta(
            limit_timedelta, fractions=True, just_days_or_hours=True
        )
        lightning_conv = await get_account_lightning_conv(
            as_of_date=as_of_date, cust_id=hive_accname, age=age, line_items=line_items
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
    as_of_date: datetime = datetime.now(tz=timezone.utc),
    line_items: bool = False,
) -> Tuple[int, LedgerAccountDetails]:
    """
    Retrieves the balance of Keepsats for a specific customer as of a given date.
    This looks at the `credit` values because credits to a Liability account
    represent deposits, while debits represent withdrawals.
    Adds a net_balance field to the output summing up deposits and withdrawals

    Args:
        cust_id (str): The customer ID for which to retrieve the Keepsats balance.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to the current UTC time.

    Returns:
        Tuple:
        net_msats (int): The net balance of Keepsats in msatoshis.
        LedgerAccountDetails: An object containing the balance details for the specified customer.
    """
    account = LiabilityAccount(
        name="Customer Liability",
        sub=cust_id,
        contra=False,
    )
    logger.info(account)
    account_balance = await one_account_balance(
        account=account,
        as_of_date=as_of_date + timedelta(days=1),
    )

    net_msats = int(account_balance.balances_net.get(Currency.MSATS, 0))
    return net_msats, account_balance


async def keepsats_balance_printout(
    cust_id: str, previous_msats: int | None = None, line_items: bool = False
) -> Tuple[int, LedgerAccountDetails]:
    """
    Generates and logs a printout of the Keepsats balance for a given customer.

    Args:
        cust_id (str): The customer ID for which to retrieve the Keepsats balance.
        previous_msats (int, optional): The previous balance in msats to compare against. Defaults to 0.

    Returns:
        Tuple[int, LedgerAccountDetails]: A tuple containing the net Keepsats balance in sats and the account balance details.

    Logs:
        - Customer ID and Keepsats balance information.
        - Net balance, previous balance (if provided), and the delta between balances.
    """
    net_msats, account_balance = await get_keepsats_balance(cust_id=cust_id, line_items=line_items)

    logger.info("_" * 50)
    logger.info(f"Customer ID {cust_id} Keepsats balance:")
    logger.info(f"  Net balance:      {net_msats // 1000:,.0f} sats")
    if previous_msats is not None:
        logger.info(f"  Previous balance: {previous_msats // 1000:,.0f} sats")
        logger.info(f"  Delta:           {net_msats - previous_msats:,.0f} sats")
    logger.info("_" * 50)

    return net_msats, account_balance
