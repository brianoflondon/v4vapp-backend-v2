from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, List, Mapping, Tuple

from v4vapp_backend_v2.accounting.account_balance_pipelines import (
    all_account_balances_pipeline,
    list_all_accounts_pipeline,
    list_all_ledger_types_pipeline,
    net_held_msats_balance_pipeline,
)
from v4vapp_backend_v2.accounting.accounting_classes import (
    AccountBalances,
    ConvertedSummary,
    LedgerAccountDetails,
    LedgerConvSummary,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.limit_check_classes import LimitCheckResult
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import limit_check_pipeline
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import async_time_stats_decorator, logger
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import (
    format_time_delta,
    lightning_memo,
    truncate_text,
)
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.models.pydantic_helpers import convert_datetime_fields
from v4vapp_backend_v2.process.lock_str_class import CustIDType

UNIT_TOLERANCE = {
    "HIVE": 0.001,
    "HBD": 0.001,
    "MSATS": 10,
}


# @async_time_stats_decorator()
async def all_account_balances(
    as_of_date: datetime | None = None, age: timedelta | None = None
) -> AccountBalances:
    """
    Retrieve all account balances as of a specified date, optionally aged by a given timedelta.
    Args:
        as_of_date (datetime | None): The date to calculate balances as of. Defaults to current UTC time if not provided.
        age (timedelta | None): Optional age to filter or adjust balances.
    Returns:
        AccountBalances: An object containing the validated account balances.
    Raises:
        ValidationError: If the results cannot be validated into AccountBalances.
    """

    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    pipeline = all_account_balances_pipeline(as_of_date=as_of_date, age=age)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    clean_results = convert_datetime_fields(results)

    account_balances = AccountBalances.model_validate(clean_results)

    # Find the most recent transaction date
    for account in account_balances.root:
        max_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        if account.balances:
            for items in account.balances.values():
                if items:
                    last_item = items[-1]
                    max_timestamp = max(max_timestamp, last_item.timestamp or max_timestamp)
        account.in_progress_msats = await in_progress(account.sub)
        account.last_transaction_date = max_timestamp

    return account_balances


# @async_time_stats_decorator()
async def one_account_balance(
    account: LedgerAccount | str,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
) -> LedgerAccountDetails:
    """
    Retrieve the balance details for a single ledger account as of a specified date.
    Args:
        account (LedgerAccount | str): The ledger account object or its string identifier.
        as_of_date (datetime | None, optional): The date for which to retrieve the account balance. Defaults to current UTC time if not provided.
        age (timedelta | None, optional): Optional age filter for the balance calculation.
    Returns:
        LedgerAccountDetails: The details of the account balance as of the specified date.
    Raises:
        None explicitly, but logs a warning if no results are found for the given account.
    Notes:
        - If `account` is provided as a string, it is converted to a LiabilityAccount.
        - If no balance data is found, returns a default LedgerAccountDetails instance.
    """

    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    if isinstance(account, str):
        account = LiabilityAccount(
            name="VSC Liability",
            sub=account,
        )
    pipeline = all_account_balances_pipeline(account=account, as_of_date=as_of_date, age=age)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    clean_results = convert_datetime_fields(results)
    account_balance = AccountBalances.model_validate(clean_results)
    # If there are multiple entries (e.g., contra and non-contra groups), merge them so both show up
    if account_balance.root and len(account_balance.root) > 0:
        if len(account_balance.root) == 1:
            ledger_details = account_balance.root[0]
        else:
            # Merge balances from multiple groups (preserve per-row contra flag and order)
            merged_balances: dict = {}
            for group in account_balance.root:
                for unit, lines in group.balances.items():
                    merged_balances.setdefault(unit, [])
                    # copy to avoid mutating original objects
                    merged_balances[unit].extend([l.model_copy() for l in lines])

            # Sort and recompute running totals (amount_running_total and conv_running_total)
            from datetime import datetime as _dt

            for unit, rows in merged_balances.items():
                rows.sort(key=lambda x: x.timestamp or _dt.min.replace(tzinfo=_dt.now().tzinfo))
                running_amount = Decimal(0)
                running_conv = ConvertedSummary()
                for row in rows:
                    running_amount += row.amount_signed
                    row.amount_running_total = running_amount
                    running_conv = running_conv + ConvertedSummary.from_crypto_conv(
                        row.conv_signed
                    )
                    row.conv_running_total = running_conv

            ledger_details = LedgerAccountDetails(
                name=account.name,
                account_type=account.account_type,
                sub=account.sub,
                contra=account.contra,
                balances=merged_balances,
            )
    else:
        ledger_details = LedgerAccountDetails(
            name=account.name,
            account_type=account.account_type,
            sub=account.sub,
            contra=account.contra,
        )

    # Find the most recent transaction date
    if ledger_details.balances:
        max_timestamp = None
        for unit, balance_lines in ledger_details.balances.items():
            for line in balance_lines:
                if line.timestamp and (max_timestamp is None or line.timestamp > max_timestamp):
                    max_timestamp = line.timestamp
        ledger_details.last_transaction_date = max_timestamp

    ledger_details.in_progress_msats = await in_progress(account.sub)

    return ledger_details


# @async_time_stats_decorator()
async def account_balance_printout(
    account: LedgerAccount | str,
    line_items: bool = True,
    user_memos: bool = True,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    ledger_account_details: LedgerAccountDetails | None = None,
    quote: QuoteResponse | None = None,
) -> Tuple[str, LedgerAccountDetails]:
    """
    Calculate and display the balance for a specified account (and optional sub-account).
    Optionally lists all debit and credit transactions up to the specified date, or shows only the closing balance.

        account (LedgerAccount | str): A LedgerAccount object specifying the account name, type, and optional sub-account.
                                       If a str is passed, it is treated as a 'VSC Liability' account for the customer specified by the string.
        line_items (bool, optional): If True, includes detailed line items for transactions. Defaults to True.
        user_memos (bool, optional): If True, includes user memos for transactions. Defaults to True.
        as_of_date (datetime | None, optional): The date up to which to calculate the balance. Defaults to None (current UTC date).
        age (timedelta | None, optional): An optional age filter for transactions. Defaults to None.
        ledger_account_details (LedgerAccountDetails | None, optional): Pre-computed ledger account details. If None, it will be fetched. Defaults to None.
        quote (QuoteResponse | None, optional): Pre-fetched quote for currency conversions. If None, it will be updated. Defaults to None.

        Tuple[str, LedgerAccountDetails]: A tuple containing a formatted string with the balance printout and the LedgerAccountDetails object.

    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    if isinstance(account, str):
        account = LiabilityAccount(
            name="VSC Liability",
            sub=account,
        )

    max_width = 135
    if not ledger_account_details:
        ledger_account_details = await one_account_balance(
            account=account, as_of_date=as_of_date, age=age
        )
    units = set(ledger_account_details.balances.keys())
    if not quote:
        quote = await TrackedBaseModel.update_quote()

    title_line = f"{account} balance as of {as_of_date:%Y-%m-%d %H:%M:%S} UTC"
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
    # Clarify that unit sections are separate views and are not additive
    output.append(
        "Note: Unit sections are separate views and are NOT additive. Transactions may appear in multiple unit sections (gross) and net in account totals."
    )
    output.append("-" * max_width)

    if not ledger_account_details.balances:
        output.append("No transactions found for this account up to today.")
        output.append("=" * max_width)
        return "\n".join(output), ledger_account_details

    COL_TS = 12
    COL_DESC = 54
    COL_DEBIT = 11
    COL_CREDIT = 11
    COL_BAL = 11
    COL_SHORT_ID = 15
    COL_LEDGER_TYPE = 11

    total_usd: Decimal = Decimal(0)
    total_msats: Decimal = Decimal(0)

    for unit in [Currency.HIVE, Currency.HBD, Currency.MSATS]:
        if unit not in units:
            continue
        display_unit = "SATS" if unit.upper() == "MSATS" else unit.upper()
        conversion_factor = 1_000 if unit.upper() == "MSATS" else 1

        output.append(f"\nUnit: {display_unit}")
        output.append("-" * 10)
        all_rows = ledger_account_details.balances[unit]
        if all_rows:
            transactions_by_date: dict[str, list] = {}
            transactions_by_cust_id: dict[str, list] = {}
            for row in all_rows:
                date_str = f"{row.timestamp:%Y-%m-%d}" if row.timestamp else "No Date"
                transactions_by_date.setdefault(date_str, []).append(row)
                transactions_by_cust_id.setdefault(row.cust_id, []).append(row)

            for date_str, rows in sorted(transactions_by_date.items()):
                output.append(f"\n=== {date_str} ===")
                for row in rows:
                    contra_str = "-c-" if row.contra else "   "
                    timestamp = f"{row.timestamp:%H:%M:%S.%f}"[:10] if row.timestamp else "N/A"
                    description = truncate_text(row.description, 50)
                    ledger_type = row.ledger_type
                    # Raw numeric values
                    debit_val = (
                        row.amount if row.side == "debit" and row.unit == unit else Decimal(0)
                    )
                    credit_val = (
                        row.amount if row.side == "credit" and row.unit == unit else Decimal(0)
                    )
                    balance_val = row.amount_running_total
                    if unit.upper() == "MSATS":
                        debit_val /= conversion_factor
                        credit_val /= conversion_factor
                        balance_val /= conversion_factor

                    # Number formats
                    if unit.upper() == "MSATS":
                        debit_fmt = f"{debit_val:,.1f}"
                        credit_fmt = f"{credit_val:,.1f}"
                        balance_fmt = f"{balance_val:,.1f}"
                    else:
                        debit_fmt = f"{debit_val:,.3f}" if debit_val != 0 else "0"
                        credit_fmt = f"{credit_val:,.3f}" if credit_val != 0 else "0"
                        balance_fmt = f"{balance_val:,.3f}"

                    line = (
                        f"{timestamp:<{COL_TS}} "
                        f"{description:<{COL_DESC}} "
                        f"{contra_str} "
                        f"{debit_fmt:>{COL_DEBIT}} "
                        f"{credit_fmt:>{COL_CREDIT}} "
                        f"{balance_fmt:>{COL_BAL}} "
                        f"{row.short_id:>{COL_SHORT_ID}} "
                        f"{ledger_type:>{COL_LEDGER_TYPE}}"
                    )
                    if line_items:
                        output.append(line)
                    if user_memos and row.user_memo:
                        memo = truncate_text(lightning_memo(row.user_memo), 60)
                        output.append(f"{' ' * (COL_TS + 1)} {memo}")

        # Perform a conversion with the current quote for this Currency unit
        final_balance = Decimal(ledger_account_details.balances_net.get(unit, 0))
        conversion = CryptoConversion(conv_from=unit, value=final_balance, quote=quote).conversion
        output.append("-" * max_width)
        output.append(
            f"{'Converted':<10} "
            f"{conversion.hive:>15,.3f} HIVE "
            f"{conversion.hbd:>12,.3f} HBD "
            f"{conversion.usd:>12,.3f} USD "
            f"{conversion.sats:>12,.0f} SATS "
            f"{conversion.msats:>16,.0f} msats"
        )
        total_usd += conversion.usd
        total_msats += conversion.msats

        output.append("-" * max_width)
        display_balance = (
            final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
        )
        if unit.upper() == "MSATS":
            balance_fmt = f"{display_balance:,.0f}"
        else:
            balance_fmt = f"{display_balance:,.3f}"
        output.append(f"{'Final Balance ' + display_unit:<18} {balance_fmt:>10} {display_unit:<5}")

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>18,.3f} USD")
    output.append(f"Total SATS: {total_msats / 1000:>17,.3f} SATS")
    output.append(title_line)

    output.append("=" * max_width + "\n")
    output_text = "\n".join(output)

    return output_text, ledger_account_details


# @async_time_stats_decorator()
async def account_balance_printout_grouped_by_customer(
    account: LedgerAccount | str,
    line_items: bool = True,
    user_memos: bool = True,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    ledger_account_details: LedgerAccountDetails | None = None,
) -> Tuple[str, LedgerAccountDetails]:
    """
    Calculate and display the balance for a specified account with transactions grouped by date and customer ID.
    This alternate version recalculates running totals per customer within each date to maintain consistency.
    Properly accounts for assets and liabilities, and includes converted values to other units
    (SATS, HIVE, HBD, USD, msats).

    Args:
        account (Account | str): An Account object specifying the account name, type, and optional sub-account. If
        a str is passed, we assume this is a `VSC Liability` account for customer `account`.
        line_items (bool, optional): If True, shows individual transaction line items. Defaults to True.
        user_memos (bool, optional): If True, shows user memos for transactions. Defaults to True.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to None (current date).
        age (timedelta | None, optional): Optional age filter for the balance calculation.
        ledger_account_details (LedgerAccountDetails | None, optional): Pre-fetched account details.

    Returns:
        str: A formatted string containing the balance with customer-grouped transactions.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)

    if isinstance(account, str):
        account = LiabilityAccount(
            name="VSC Liability",
            sub=account,
        )

    max_width = 135
    if not ledger_account_details:
        ledger_account_details = await one_account_balance(
            account=account, as_of_date=as_of_date, age=age
        )
    units = set(ledger_account_details.balances.keys())
    quote = await TrackedBaseModel.update_quote()

    title_line = (
        f"{account} balance as of {as_of_date:%Y-%m-%d %H:%M:%S} UTC (Grouped by Customer)"
    )
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
    output.append("-" * max_width)

    if not ledger_account_details.balances:
        output.append("No transactions found for this account up to today.")
        output.append("=" * max_width)
        return "\n".join(output), ledger_account_details

    COL_TS = 12
    COL_DESC = 54
    COL_DEBIT = 11
    COL_CREDIT = 11
    COL_BAL = 11
    COL_SHORT_ID = 15
    COL_LEDGER_TYPE = 11

    total_usd: Decimal = Decimal(0)
    total_msats: Decimal = Decimal(0)

    for unit in [Currency.HIVE, Currency.HBD, Currency.MSATS]:
        if unit not in units:
            continue
        display_unit = "SATS" if unit.upper() == "MSATS" else unit.upper()
        conversion_factor = 1_000 if unit.upper() == "MSATS" else 1

        output.append(f"\nUnit: {display_unit}")
        output.append("-" * 10)
        all_rows = ledger_account_details.balances[unit]
        if all_rows:
            # Group by date first
            transactions_by_date: dict[str, list] = {}
            for row in all_rows:
                date_str = f"{row.timestamp:%Y-%m-%d}" if row.timestamp else "No Date"
                transactions_by_date.setdefault(date_str, []).append(row)

            for date_str, date_rows in sorted(transactions_by_date.items()):
                output.append(f"\n=== {date_str} ===")

                # Check if there are multiple cust_ids for this date
                cust_ids = set(row.cust_id for row in date_rows)
                if len(cust_ids) > 1:
                    # Group by cust_id and recalculate running totals per customer
                    for cust_id in sorted(cust_ids):
                        cust_rows = [row for row in date_rows if row.cust_id == cust_id]
                        output.append(f"\n--- Customer: {cust_id} ---")

                        # Recalculate running totals for this customer group
                        running_total = Decimal(0)
                        for row in sorted(
                            cust_rows,
                            key=lambda x: x.timestamp or datetime.min.replace(tzinfo=timezone.utc),
                        ):
                            contra_str = "-c-" if row.contra else "   "
                            timestamp = (
                                f"{row.timestamp:%H:%M:%S.%f}"[:10] if row.timestamp else "N/A"
                            )
                            description = truncate_text(row.description, 50)
                            ledger_type = row.ledger_type

                            # Raw numeric values
                            debit_val = (
                                Decimal(row.amount)
                                if row.side == "debit" and row.unit == unit
                                else Decimal(0)
                            )
                            credit_val = (
                                Decimal(row.amount)
                                if row.side == "credit" and row.unit == unit
                                else Decimal(0)
                            )

                            # Update running total for this customer
                            if row.side == "debit":
                                running_total += row.amount_signed
                            else:  # credit
                                running_total += row.amount_signed

                            balance_val = running_total

                            if unit.upper() == "MSATS":
                                debit_val /= conversion_factor
                                credit_val /= conversion_factor
                                balance_val /= conversion_factor

                            # Number formats
                            if unit.upper() == "MSATS":
                                debit_fmt = f"{debit_val:,.1f}"
                                credit_fmt = f"{credit_val:,.1f}"
                                balance_fmt = f"{balance_val:,.1f}"
                            else:
                                debit_fmt = f"{debit_val:,.3f}" if debit_val != 0 else "0"
                                credit_fmt = f"{credit_val:,.3f}" if credit_val != 0 else "0"
                                balance_fmt = f"{balance_val:,.3f}"

                            line = (
                                f"{timestamp:<{COL_TS}} "
                                f"{description:<{COL_DESC}} "
                                f"{contra_str} "
                                f"{debit_fmt:>{COL_DEBIT}} "
                                f"{credit_fmt:>{COL_CREDIT}} "
                                f"{balance_fmt:>{COL_BAL}} "
                                f"{row.short_id:>{COL_SHORT_ID}} "
                                f"{ledger_type:>{COL_LEDGER_TYPE}}"
                            )
                            if line_items:
                                output.append(line)
                            if user_memos and row.user_memo:
                                memo = truncate_text(lightning_memo(row.user_memo), 60)
                                output.append(f"{' ' * (COL_TS + 1)} {memo}")
                else:
                    # Single cust_id, recalculate running totals for consistency
                    output.append(f"\n--- Customer: {list(cust_ids)[0]} ---")
                    running_total = Decimal(0)
                    for row in sorted(
                        date_rows,
                        key=lambda x: x.timestamp or datetime.min.replace(tzinfo=timezone.utc),
                    ):
                        contra_str = "-c-" if row.contra else "   "
                        timestamp = f"{row.timestamp:%H:%M:%S.%f}"[:10] if row.timestamp else "N/A"
                        description = truncate_text(row.description, 50)
                        ledger_type = row.ledger_type

                        # Raw numeric values
                        debit_val = (
                            row.amount if row.side == "debit" and row.unit == unit else Decimal(0)
                        )
                        credit_val = (
                            row.amount if row.side == "credit" and row.unit == unit else Decimal(0)
                        )

                        # Update running total
                        if row.side == "debit":
                            running_total += row.amount_signed
                        else:  # credit
                            running_total += row.amount_signed

                        balance_val = running_total

                        if unit.upper() == "MSATS":
                            debit_val /= conversion_factor
                            credit_val /= conversion_factor
                            balance_val /= conversion_factor

                        # Number formats
                        if unit.upper() == "MSATS":
                            debit_fmt = f"{debit_val:,.0f}"
                            credit_fmt = f"{credit_val:,.0f}"
                            balance_fmt = f"{balance_val:,.0f}"
                        else:
                            debit_fmt = f"{debit_val:,.3f}"
                            credit_fmt = f"{credit_val:,.3f}"
                            balance_fmt = f"{balance_val:,.3f}"

                        line = (
                            f"{timestamp:<{COL_TS}} "
                            f"{description:<{COL_DESC}} "
                            f"{contra_str} "
                            f"{debit_fmt:>{COL_DEBIT}} "
                            f"{credit_fmt:>{COL_CREDIT}} "
                            f"{balance_fmt:>{COL_BAL}} "
                            f"{row.short_id:>{COL_SHORT_ID}} "
                            f"{ledger_type:>{COL_LEDGER_TYPE}}"
                        )
                        if line_items:
                            output.append(line)
                        if user_memos and row.user_memo:
                            memo = truncate_text(lightning_memo(row.user_memo), 60)
                            output.append(f"{' ' * (COL_TS + 1)} {memo}")

        # Perform a conversion with the current quote for this Currency unit
        final_balance = ledger_account_details.balances_net.get(unit, 0)

        conversion = CryptoConversion(conv_from=unit, value=final_balance, quote=quote).conversion
        output.append("-" * max_width)
        output.append(
            f"{'Converted':<10} "
            f"{conversion.hive:>15,.3f} HIVE "
            f"{conversion.hbd:>12,.3f} HBD "
            f"{conversion.usd:>12,.3f} USD "
            f"{conversion.sats:>12,.0f} SATS "
            f"{conversion.msats:>16,.0f} msats"
        )
        total_usd += conversion.usd
        total_msats += conversion.msats

        output.append("-" * max_width)
        display_balance = (
            final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
        )
        if unit.upper() == "MSATS":
            balance_fmt = f"{display_balance:,.0f}"
        else:
            balance_fmt = f"{display_balance:,.3f}"
        output.append(f"{'Final Balance ' + display_unit:<18} {balance_fmt:>10} {display_unit:<5}")

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>18,.3f} USD")
    output.append(f"Total SATS: {total_msats / 1000:>17,.3f} SATS")
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


@async_time_stats_decorator()
async def list_all_ledger_types() -> List[LedgerType]:
    """
    Lists all unique ledger types in the ledger.

    Returns:
        List[LedgerType]: A list of unique LedgerType objects sorted by name.
    """
    pipeline = list_all_ledger_types_pipeline()
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    ledger_types: List[LedgerType] = []
    async for doc in cursor:
        try:
            ledger_type = LedgerType(doc.get("ledger_type"))
            ledger_types.append(ledger_type)
        except ValueError:
            logger.warning(
                f"Unknown ledger type found in ledger entries: {doc.get('ledger_type')}",
                extra={"notification": False},
            )
    return ledger_types


async def ledger_pipeline_result(
    cust_id: CustIDType,
    account: LedgerAccount,
    pipeline: List[Mapping[str, Any]],
    as_of_date: datetime | None = None,
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
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
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


async def check_hive_conversion_limits(
    cust_id: CustIDType, extra_spend_msats: Decimal = Decimal(0), line_items: bool = False
) -> LimitCheckResult:
    """
    Checks if a Hive account's recent Lightning conversions are within configured rate limits.
    Args:
        cust_id (str): The Hive account name to check conversion limits for.
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
    extra_spend_sats = extra_spend_msats // Decimal(1000)  # Convert msats to sats

    pipeline = limit_check_pipeline(
        cust_id=cust_id, details=False, extra_spend_sats=extra_spend_sats
    )
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list(length=None)
    results = convert_decimal128_to_decimal(results)
    limit_check = LimitCheckResult.model_validate(results[0]) if results else LimitCheckResult()
    if not limit_check.limit_ok:
        expiry_info = await get_next_limit_expiry(cust_id)
        if expiry_info:
            limit_check.expiry, limit_check.sats_freed = expiry_info
            expires_in = limit_check.expiry - datetime.now(tz=timezone.utc)
            limit_check.next_limit_expiry = f"Next limit expires in: {format_time_delta(expires_in)}, freeing {limit_check.sats_freed:,.0f} sats"

    return limit_check


async def get_next_limit_expiry(cust_id: CustIDType) -> Tuple[datetime, Decimal] | None:
    """
    Determines when the next rate limit will expire for a given customer and the amount that will be freed.
    This looks at the first (shortest) rate limit period and finds the oldest transaction
    within that period. The expiry time is when that transaction will be outside the limit window,
    and the amount freed is the sats value of that transaction.

    Args:
        cust_id (str): The customer ID to check the limit expiry for.

    Returns:
        Tuple[datetime, int] | None: A tuple of (expiry_datetime, sats_freed), or None if no limits or no transactions.
    """
    lightning_rate_limits = V4VConfig().data.lightning_rate_limits
    if not lightning_rate_limits:
        return None

    first_limit = min(lightning_rate_limits, key=lambda x: x.hours)

    pipeline = limit_check_pipeline(cust_id=cust_id, details=True)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list(length=None)

    if not results:
        return None

    result = results[0]
    periods = result.get("periods", {})
    first_period_key = str(first_limit.hours)

    if first_period_key not in periods:
        return None

    period_data = periods[first_period_key]
    details = period_data.get("details", [])

    if not details:
        return None

    # Find the oldest transaction
    oldest_entry = min(details, key=lambda x: x["timestamp"])
    oldest_ts: datetime = oldest_entry["timestamp"]
    # Converts on entry to Decimal from Decimal128 out of MongoDB
    sats_freed: Decimal = Decimal(str(oldest_entry["credit_conv"]["msats"])) // Decimal(
        1000
    )  # Convert msats to sats

    expiry: datetime = oldest_ts + timedelta(hours=first_limit.hours)
    return expiry, sats_freed


# @async_time_stats_decorator()
async def keepsats_balance(
    cust_id: CustIDType = "",
    as_of_date: datetime | None = None,
    line_items: bool = False,
) -> Tuple[Decimal, LedgerAccountDetails]:
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
        net_msats (int): The net balance of Keepsats in milisatoshis.
        LedgerAccountDetails: An object containing the balance details for the specified customer.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    account = LiabilityAccount(
        name="VSC Liability",
        sub=cust_id,
        contra=False,
    )
    account_balance = await one_account_balance(
        account=account,
        as_of_date=as_of_date + timedelta(days=1),
    )

    net_msats = account_balance.msats
    if net_msats < Decimal(0) and account_balance.sats == Decimal(0):
        net_msats = Decimal(0)
    return net_msats, account_balance


async def keepsats_balance_printout(
    cust_id: CustIDType, previous_msats: int | Decimal | None = None, line_items: bool = False
) -> Tuple[Decimal, LedgerAccountDetails]:
    """
    Generates and logs a printout of the Keepsats balance for a given customer.

    Args:
        cust_id (str): The customer ID for which to retrieve the Keepsats balance.
        previous_msats (int, optional): The previous balance in msats to compare against. Defaults to 0.

    Returns:
        Tuple[int, LedgerAccountDetails]: A tuple containing the net Keepsats balance in msats and the account balance details.

    Logs:
        - Customer ID and Keepsats balance information.
        - Net balance, previous balance (if provided), and the delta between balances.
    """
    net_msats, account_balance = await keepsats_balance(cust_id=cust_id, line_items=line_items)
    sats = (net_msats / 1000).quantize(Decimal("1."))
    previous_sats = (
        (Decimal(previous_msats) / 1000).quantize(Decimal("1.")) if previous_msats else None
    )
    logger.info("_" * 50)
    logger.info(f"Customer ID {cust_id} Keepsats balance:")
    logger.info(f"  Net balance:      {sats:,.0f} sats")
    if previous_sats is not None:
        logger.info(f"  Previous balance: {previous_sats:,.0f} sats")
        logger.info(f"  Delta:           {sats - previous_sats:,.0f} sats")
    logger.info("_" * 50)

    return net_msats, account_balance


async def in_progress(cust_id: CustIDType) -> Decimal:
    """
    Calculate the in-progress balance for a given customer ID.

    This asynchronous function aggregates ledger entries using a predefined pipeline
    to compute the net held balance in millisatoshis, converts it to satoshis,
    quantizes to whole satoshis, and returns the result as a Decimal.

    Args:
        cust_id (CustIDType): The customer ID for which to calculate the balance.

    Returns:
        Decimal: The in-progress balance in satoshis, quantized to whole units.
                 Returns 0 if no results are found.
    """
    in_progress_pipeline = net_held_msats_balance_pipeline(cust_id=cust_id)
    cursor = await LedgerEntry.collection().aggregate(in_progress_pipeline)
    results = await cursor.to_list(length=None)
    if results and len(results) > 0:
        in_progress_sats = Decimal(results[0].get("net_held", 0) / Decimal(1000)).quantize(
            Decimal("1.")
        )
    else:
        in_progress_sats = Decimal(0)

    return in_progress_sats
