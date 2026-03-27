from datetime import datetime, timedelta, timezone
from decimal import Decimal
from timeit import default_timer as timer
from typing import Any, List, Mapping, Set, Tuple

from v4vapp_backend_v2.accounting.account_balance_pipelines import (
    account_notifications_pipeline,
    active_account_subs_pipeline,
    all_account_balances_pipeline,
    all_account_balances_summary_pipeline,
    list_all_accounts_pipeline,
    list_all_ledger_types_pipeline,
)
from v4vapp_backend_v2.accounting.accounting_classes import (
    AccountBalanceLine,
    AccountBalances,
    ConvertedSummary,
    LedgerAccountDetails,
    LedgerConvSummary,
)
from v4vapp_backend_v2.accounting.in_progress_results_class import (
    InProgressResults,
    all_held_msats,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount, LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_cache import (
    HISTORICAL_TTL_SECONDS,
    LIVE_TTL_SECONDS,
    get_cached_balance,
    set_cached_balance,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.limit_check_classes import LimitCheckResult
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import limit_check_pipeline
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.decorators import async_time_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import format_time_delta, truncate_text
from v4vapp_backend_v2.helpers.lightning_memo_class import LightningMemo
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.models.pydantic_helpers import convert_datetime_fields
from v4vapp_backend_v2.process.lock_str_class import CustIDType

UNIT_TOLERANCE = {
    "HIVE": 0.001,
    "HBD": 0.001,
    "MSATS": 10,
}


# I am not sure what the purpose of this code is, removed from code path.


def _merge_groups(groups: List[LedgerAccountDetails]) -> LedgerAccountDetails:
    """Merge multiple LedgerAccountDetails objects belonging to the same
    account (typically differing only by the ``contra`` flag) into a single
    consolidated object.

    This logic is essentially the same as the merge code in ``one_account_balance``
    but exposed here so it can be reused by ``all_account_balances`` and
    other callers.  The resulting ``LedgerAccountDetails`` will have all of the
    balance lines concatenated, sorted chronologically, and running totals
    recalculated so that the ``msats``/``sats`` values reflect the true net
    balance.
    """
    # trivial case -- nothing to do
    if len(groups) == 1:
        return groups[0]

    merged_balances: dict = {}
    for group in groups:
        for unit, lines in group.balances.items():
            merged_balances.setdefault(unit, [])
            # copy to avoid mutating the originals
            merged_balances[unit].extend([line.model_copy() for line in lines])

    # sort and recompute running totals for each currency
    from datetime import datetime as _dt

    for unit, rows in merged_balances.items():
        rows.sort(key=lambda x: x.timestamp or _dt.min.replace(tzinfo=_dt.now().tzinfo))
        running_amount = Decimal(0)
        running_conv = ConvertedSummary()
        for row in rows:
            running_amount += row.amount_signed
            row.amount_running_total = running_amount
            running_conv = running_conv + ConvertedSummary.from_crypto_conv(row.conv_signed)
            row.conv_running_total = running_conv

    base = groups[0]
    ledger_details = LedgerAccountDetails(
        name=base.name,
        account_type=base.account_type,
        sub=base.sub,
        contra=base.contra,
        balances=merged_balances,
    )

    # recompute last_transaction_date (it will be assigned later by caller but
    # having something sensible here avoids a brief window where it is None)
    max_ts = None
    for unit_lines in ledger_details.balances.values():
        if unit_lines:
            last = unit_lines[-1].timestamp
            if last and (max_ts is None or last > max_ts):
                max_ts = last
    ledger_details.last_transaction_date = max_ts

    return ledger_details


def _merge_duplicate_accounts(account_balances: AccountBalances) -> AccountBalances:
    """Combine root entries returned by ``all_account_balances`` that belong
    to the same account (same ``account_type``, ``name`` and ``sub``).

    The underlying aggregation pipeline splits results into separate documents
    whenever the ``contra`` flag differs.  Consumers of the API usually expect
    one row per account, so we collapse those here.  The merge is performed in-
    memory and does *not* touch the database again.
    """
    seen: dict[tuple, List[LedgerAccountDetails]] = {}
    for acct in account_balances.root:
        key = (acct.account_type, acct.name, acct.sub)
        seen.setdefault(key, []).append(acct)

    merged_root: List[LedgerAccountDetails] = []
    for groups in seen.values():
        if len(groups) == 1:
            merged_root.append(groups[0])
        else:
            merged_root.append(_merge_groups(groups))

    account_balances.root = merged_root
    return account_balances


@async_time_decorator
async def all_account_balances(
    account: LedgerAccount | None = None,
    account_name: str | None = None,
    sub: str | None = None,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    filter: Mapping[str, Any] | None = None,
    cust_ids: Set[str] | None = None,
) -> AccountBalances:
    """
    Retrieve all account balances as of a specified date, optionally aged by a given timedelta.
    The order of precedence for filtering is: `account` > `account_name` > `sub`. If none are provided, the pipeline will include all accounts.
    Args:
        account (LedgerAccount, optional): An instance of LedgerAccount to filter the transactions. If provided, the pipeline will match transactions for this specific account.
        account_name (str, optional): The name of the account to filter transactions. Used if `account` is not provided.
        sub (str, optional): The sub identifier to filter transactions. Used if `account` and `account_name` are not provided.
        as_of_date (datetime, optional): The end date for the balance calculation. Defaults to the current UTC datetime.
        age (timedelta | None, optional): If provided, limits the results to transactions within the specified age (time window) ending at `as_of_date`.
        cust_ids (Set[str] | None, optional): If provided, pre-filters ledger entries by cust_id (indexed) before the expensive aggregation. Used for restricting to active accounts.

    Returns:
        AccountBalances: An object containing the validated account balances.
    Raises:
        ValidationError: If the results cannot be validated into AccountBalances.
    """

    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    _t0 = timer()
    pipeline = all_account_balances_pipeline(
        account=account,
        account_name=account_name,
        sub=sub,
        as_of_date=as_of_date,
        age=age,
        filter=filter,
        cust_ids=cust_ids,
    )
    _t1 = timer()
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    _t2 = timer()
    results = await cursor.to_list()
    _t3 = timer()
    clean_results = convert_datetime_fields(results)
    _t4 = timer()

    account_balances = AccountBalances.model_validate(clean_results)
    # # -- merge any duplicate root entries caused by contra flags --
    # account_balances = _merge_duplicate_accounts(account_balances)
    _t5 = timer()
    all_held_result = await all_held_msats()
    _t6 = timer()
    in_progress = InProgressResults(results=all_held_result)
    _t7 = timer()

    # Find the most recent transaction date
    for account in account_balances.root:
        max_timestamp = datetime.min.replace(tzinfo=timezone.utc)
        if account.balances:
            for items in account.balances.values():
                if items:
                    last_item = items[-1]
                    max_timestamp = max(max_timestamp, last_item.timestamp or max_timestamp)
        account.in_progress_msats = in_progress.get(account.sub).net_held
        account.last_transaction_date = max_timestamp
    _t8 = timer()

    logger.info(
        f"aggregate={(_t2 - _t1):.3f}s, "
        # f"to_list={(_t3 - _t2):.3f}s, "
        # f"validate={(_t5 - _t4):.3f}s, "
        f"held_msats={(_t6 - _t5):.3f}s, "
        # f"in_progress={(_t7 - _t6):.3f}s, "
        # f"post_process={(_t8 - _t7):.3f}s, "
        f"total={(_t8 - _t0):.3f}s "
        f"all_account_balances timing "
        f"({len(account_balances.root)} accounts, {len(results)} result docs)"
    )

    return account_balances


async def all_account_balances_summary(
    account_name: str | None = None,
    cust_ids: Set[str] | None = None,
    as_of_date: datetime | None = None,
) -> AccountBalances:
    """Lightweight bulk balance query.

    Uses a simple ``$group`` aggregation instead of the O(n²) running-total
    pipeline.  Returns the same ``AccountBalances`` type so callers that only
    need final totals (sats, conv_total, has_transactions, last_transaction_date)
    can swap in this function without other changes.

    Per-transaction detail is **not** included — use ``one_account_balance``
    for that.
    """
    if as_of_date is None:
        as_of_date = datetime.now(tz=timezone.utc)
    _t0 = timer()
    pipeline = all_account_balances_summary_pipeline(
        account_name=account_name,
        cust_ids=cust_ids,
        as_of_date=as_of_date,
    )
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    results = convert_decimal128_to_decimal(results)
    _t1 = timer()

    # Group rows by (account_type, name, sub, contra) — same grouping as the
    # full pipeline so contra variants stay separate.
    account_groups: dict[tuple, dict] = {}
    for row in results:
        key = (row["account_type"], row["name"], row["sub"], row.get("contra", False))
        if key not in account_groups:
            account_groups[key] = {
                "account_type": row["account_type"],
                "name": row["name"],
                "sub": row["sub"],
                "contra": row.get("contra", False),
                "units": {},
                "max_timestamp": None,
                "total_count": 0,
                "has_non_opening": False,
            }
        group = account_groups[key]
        group["units"][row["unit"]] = row
        ts = row.get("max_timestamp")
        if ts and (group["max_timestamp"] is None or ts > group["max_timestamp"]):
            group["max_timestamp"] = ts
        group["total_count"] += row.get("count", 0)
        if row.get("has_non_opening"):
            group["has_non_opening"] = True
    _t2 = timer()

    all_held_result = await all_held_msats()
    in_progress = InProgressResults(results=all_held_result)
    _t3 = timer()

    details_list: List[LedgerAccountDetails] = []
    for group in account_groups.values():
        balances: dict[Currency, list[AccountBalanceLine]] = {}
        for unit_str, row in group["units"].items():
            currency = Currency(unit_str)
            ledger_type = (
                "summary" if group["has_non_opening"] else LedgerType.OPENING_BALANCE.value
            )
            conv_summary = ConvertedSummary(
                hive=Decimal(str(row["total_conv_hive"])),
                hbd=Decimal(str(row["total_conv_hbd"])),
                usd=Decimal(str(row["total_conv_usd"])),
                sats=Decimal(str(row["total_conv_sats"])),
                msats=Decimal(str(row["total_conv_msats"])),
            )
            line = AccountBalanceLine(
                ledger_type=ledger_type,
                timestamp=row.get("max_timestamp") or datetime.now(tz=timezone.utc),
                amount_signed=Decimal(str(row["total_amount"])),
                amount_running_total=Decimal(str(row["total_amount"])),
                unit=unit_str,
                conv_signed=CryptoConv(
                    hive=conv_summary.hive,
                    hbd=conv_summary.hbd,
                    usd=conv_summary.usd,
                    sats=conv_summary.sats,
                    msats=conv_summary.msats,
                ),
                conv_running_total=conv_summary,
            )
            balances[currency] = [line]

        details = LedgerAccountDetails(
            name=group["name"],
            account_type=group["account_type"],
            sub=group["sub"],
            contra=group.get("contra", False),
            balances=balances,
        )
        details.last_transaction_date = group["max_timestamp"]
        details.in_progress_msats = in_progress.get_net_held(group["sub"])
        details_list.append(details)
    _t4 = timer()

    account_balances = AccountBalances(root=details_list)

    logger.info(
        f"aggregate={(_t1 - _t0):.3f}s, "
        f"held_msats={(_t3 - _t2):.3f}s, "
        f"total={(_t4 - _t0):.3f}s "
        f"all_account_balances_summary timing "
        f"({len(details_list)} accounts, {len(results)} result docs)"
    )

    return account_balances


# @async_time_decorator
async def one_account_balance(
    account: LedgerAccount | str,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    in_progress: InProgressResults | None = None,
    use_cache: bool = True,
    use_checkpoints: bool = True,
) -> LedgerAccountDetails:
    """
    Retrieve the balance details for a single ledger account as of a specified date.

    Args:
        account (LedgerAccount | str): The ledger account object or its string identifier.
        as_of_date (datetime | None, optional): The date for which to retrieve the account balance. Defaults to current UTC time if not provided.
        age (timedelta | None, optional): Optional age filter for the balance calculation.
        in_progress (InProgressResults | None, optional): Pre-computed in-progress results. If None, fetched fresh.
        use_cache (bool): If True, try Redis cache before hitting the database. Defaults to True.
        use_checkpoints (bool): If True and ``as_of_date`` is provided, try to start from a
            pre-calculated checkpoint and only aggregate the incremental delta.  Defaults to True.
    Returns:
        LedgerAccountDetails: The details of the account balance as of the specified date.
    Raises:
        None explicitly, but logs a warning if no results are found for the given account.
    Notes:
        - If `account` is provided as a string, it is converted to a LiabilityAccount.
        - If no balance data is found, returns a default LedgerAccountDetails instance.
        - Results are cached in Redis.  Most cache invalidations happen
          via ``invalidate_ledger_cache(debit_name, debit_sub, credit_name,
          credit_sub)``, which deletes only the relevant account(s).  A full
          flush can be forced by calling ``invalidate_all_ledger_cache()``.
    """
    _t0 = timer()
    if isinstance(account, str):
        account = LiabilityAccount(
            name="VSC Liability",
            sub=account,
        )

    # --- Cache lookup ---
    if use_cache:
        cached_result = await get_cached_balance(account, as_of_date, age)
        if cached_result is not None:
            # Always refresh in_progress_msats (changes independently of ledger)
            if in_progress is None:
                all_held_result = await all_held_msats()
                in_progress = InProgressResults(results=all_held_result)
            cached_result.in_progress_msats = in_progress.get_net_held(account.sub)
            return cached_result

    # --- Checkpoint lookup (only for explicit historical queries without an age window) ---
    checkpoint = None
    from_date: datetime | None = None
    if use_checkpoints and as_of_date is not None and age is None:
        from v4vapp_backend_v2.accounting.ledger_checkpoints import get_latest_checkpoint_before

        try:
            checkpoint = await get_latest_checkpoint_before(account, as_of_date)
            if checkpoint is not None:
                from_date = checkpoint.period_end
                logger.debug(
                    f"📌 Using checkpoint for {account.name}:{account.sub} "
                    f"@ {checkpoint.period_end.date()} → delta from {from_date.date()} to {as_of_date.date()}",
                    extra={"notification": False},
                )
        except Exception as e:
            logger.debug(
                f"Checkpoint lookup failed for {account.name}:{account.sub}: {e}",
                extra={"notification": False},
            )
            checkpoint = None
            from_date = None

    pipeline = all_account_balances_pipeline(
        account=account,
        as_of_date=as_of_date,
        age=age,
        from_date=from_date,
    )
    _t1 = timer()
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    clean_results = convert_datetime_fields(results)
    _t2 = timer()
    account_balance = AccountBalances.model_validate(clean_results)
    _t3 = timer()
    # If there are multiple entries (e.g., contra and non-contra groups), merge them so both show up
    if account_balance.root and len(account_balance.root) > 0:
        if len(account_balance.root) == 1:
            merged_balances = {
                unit: [line.model_copy() for line in lines]
                for unit, lines in account_balance.root[0].balances.items()
            }
        else:
            # Merge balances from multiple groups (preserve per-row contra flag and order)
            merged_balances = {}
            for group in account_balance.root:
                for unit, lines in group.balances.items():
                    merged_balances.setdefault(unit, [])
                    # copy to avoid mutating original objects
                    merged_balances[unit].extend([line.model_copy() for line in lines])

        # Sort and recompute running totals (amount_running_total and conv_running_total)
        from datetime import datetime as _dt

        for unit, rows in merged_balances.items():
            rows.sort(key=lambda x: x.timestamp or _dt.min.replace(tzinfo=_dt.now().tzinfo))
            running_amount = Decimal(0)
            running_conv = ConvertedSummary()
            for row in rows:
                running_amount += row.amount_signed
                row.amount_running_total = running_amount
                running_conv = running_conv + ConvertedSummary.from_crypto_conv(row.conv_signed)
                row.conv_running_total = running_conv

        # --- Apply checkpoint offsets to running totals ---
        if checkpoint is not None:
            for unit, rows in merged_balances.items():
                cp_net = checkpoint.balances_net.get(str(unit), Decimal(0))
                cp_conv_raw = checkpoint.conv_totals.get(str(unit))
                cp_conv = (
                    cp_conv_raw.to_converted_summary()
                    if cp_conv_raw is not None
                    else ConvertedSummary()
                )
                for row in rows:
                    row.amount_running_total += cp_net
                    row.conv_running_total = row.conv_running_total + cp_conv

        ledger_details = LedgerAccountDetails(
            name=account.name,
            account_type=account.account_type,
            sub=account.sub,
            contra=account.contra,
            balances=merged_balances,
        )
    elif checkpoint is not None:
        # Delta pipeline returned no rows; balance equals checkpoint values.
        # Construct a synthetic set of balance lines so callers see non-zero totals.
        from v4vapp_backend_v2.accounting.accounting_classes import AccountBalanceLine

        synthetic_balances = {}
        for unit_str, net_val in checkpoint.balances_net.items():
            if net_val == Decimal(0):
                continue
            from v4vapp_backend_v2.helpers.currency_class import Currency

            try:
                currency = Currency(unit_str)
            except ValueError:
                continue
            cp_conv_raw = checkpoint.conv_totals.get(unit_str)
            cp_conv = (
                cp_conv_raw.to_converted_summary()
                if cp_conv_raw is not None
                else ConvertedSummary()
            )
            line = AccountBalanceLine(
                timestamp=checkpoint.period_end,
                description="Opening Balance",
                unit=unit_str,
                amount=abs(net_val),
                amount_signed=net_val,
                amount_running_total=net_val,
                conv_running_total=cp_conv,
                account_type=str(account.account_type),
                name=account.name,
                sub=account.sub,
                contra=account.contra,
            )
            synthetic_balances[currency] = [line]
        ledger_details = LedgerAccountDetails(
            name=account.name,
            account_type=account.account_type,
            sub=account.sub,
            contra=account.contra,
            balances=synthetic_balances,
        )
    else:
        ledger_details = LedgerAccountDetails(
            name=account.name,
            account_type=account.account_type,
            sub=account.sub,
            contra=account.contra,
        )
    _t4 = timer()
    # Find the most recent transaction date
    if ledger_details.balances:
        max_timestamp = None
        for unit, balance_lines in ledger_details.balances.items():
            for line in balance_lines:
                if line.timestamp and (max_timestamp is None or line.timestamp > max_timestamp):
                    max_timestamp = line.timestamp
        # When using a checkpoint, the last transaction date could be in the checkpoint
        if checkpoint is not None and checkpoint.last_transaction_date is not None:
            if max_timestamp is None or checkpoint.last_transaction_date > max_timestamp:
                max_timestamp = checkpoint.last_transaction_date
        ledger_details.last_transaction_date = max_timestamp

    if in_progress is None:
        all_held_result = await all_held_msats()
        in_progress = InProgressResults(results=all_held_result)
    ledger_details.in_progress_msats = in_progress.get_net_held(account.sub)
    _t5 = timer()

    # --- Cache store ---
    try:
        ttl = LIVE_TTL_SECONDS if as_of_date is None else HISTORICAL_TTL_SECONDS
        # pass the original intent (None for live) so key doesn't drift
        await set_cached_balance(account, as_of_date, age, ledger_details, ttl=ttl)
    except Exception as e:
        logger.warning(f"Failed to set cache for {account.name}:{account.sub}: {e}")

    return ledger_details


def _add_notes() -> str:
    # Clarify that unit sections are separate views and are not additive
    return (
        "Notes: \n"
        "1.Unit sections are separate views and are NOT additive.\n"
        "2.Transactions may appear in multiple unit sections (gross) and net in account totals.\n"
    )


# Helpers to centralize KSATS logic and formatting for reuse in printouts
def _compute_ksats_settings(ledger_account_details: LedgerAccountDetails, unit: str):
    """Determine conversion factor, whether to display in KSATS, and the display unit string."""
    conversion_factor = 1_000 if unit.upper() == "MSATS" else 1
    final_bal_for_unit = Decimal(ledger_account_details.balances_net.get(unit, 0))
    display_balance_total = (
        (final_bal_for_unit / conversion_factor) if unit.upper() == "MSATS" else final_bal_for_unit
    )
    use_ksats = unit.upper() == "MSATS" and abs(display_balance_total) >= Decimal(1_000_000)
    display_unit = "KSATS" if use_ksats else ("SATS" if unit.upper() == "MSATS" else unit.upper())
    return conversion_factor, use_ksats, display_unit


def _format_converted_line(conversion, use_ksats: bool) -> str:
    """Return the formatted Converted line for a conversion object."""
    if use_ksats:
        sats_str = f"{(conversion.sats / Decimal(1000)):>12,.1f} KSATS "
    else:
        sats_str = f"{conversion.sats:>12,.0f} SATS "
    return (
        f"{'Converted':<10} "
        f"{conversion.hive:>15,.3f} HIVE "
        f"{conversion.hbd:>12,.3f} HBD "
        f"{conversion.usd:>12,.3f} USD "
        f"{sats_str}"
        f"{conversion.msats:>16,.0f} msats"
    )


def _format_final_balance_line(
    display_unit: str, display_balance: Decimal, unit: str, use_ksats: bool
) -> str:
    """Return the Final Balance line text based on unit display preferences."""
    if unit.upper() == "MSATS":
        if use_ksats:
            balance_fmt = f"{(display_balance / Decimal(1000)):,.1f}"
            balance_unit = "KSATS"
        else:
            balance_fmt = f"{display_balance:,.0f}"
            balance_unit = "SATS"
    else:
        balance_fmt = f"{display_balance:,.3f}"
        balance_unit = unit.upper()
    return f"{'Final Balance ' + display_unit:<18} {balance_fmt:>10} {balance_unit:<5}"


def _format_amounts_for_display(
    unit: str,
    debit_val: Decimal,
    credit_val: Decimal,
    balance_val: Decimal,
    conversion_factor: int,
    use_ksats: bool,
    msats_nonks_format: str = "one_decimal",
) -> tuple[str, str, str]:
    """Convert numeric msats values and return formatted strings for debit, credit and balance.

    msats_nonks_format: 'one_decimal' or 'integer' determines how non-KSATS SATS are formatted.
    """
    if unit.upper() == "MSATS":
        # Convert from msats to sats first
        debit_val /= conversion_factor
        credit_val /= conversion_factor
        balance_val /= conversion_factor
        # Optionally convert sats to ksats for display
        if use_ksats:
            debit_val /= Decimal(1000)
            credit_val /= Decimal(1000)
            balance_val /= Decimal(1000)

        if use_ksats:
            debit_fmt = f"{debit_val:,.1f}"
            credit_fmt = f"{credit_val:,.1f}"
            balance_fmt = f"{balance_val:,.1f}"
        else:
            if msats_nonks_format == "one_decimal":
                debit_fmt = f"{debit_val:,.1f}"
                credit_fmt = f"{credit_val:,.1f}"
                balance_fmt = f"{balance_val:,.1f}"
            else:
                debit_fmt = f"{debit_val:,.0f}"
                credit_fmt = f"{credit_val:,.0f}"
                balance_fmt = f"{balance_val:,.0f}"
    else:
        debit_fmt = f"{debit_val:,.3f}" if debit_val != 0 else "0"
        credit_fmt = f"{credit_val:,.3f}" if credit_val != 0 else "0"
        balance_fmt = f"{balance_val:,.3f}"

    return debit_fmt, credit_fmt, balance_fmt


async def account_balance_printout(
    account: LedgerAccount | str,
    line_items: bool = True,
    user_memos: bool = True,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    ledger_account_details: LedgerAccountDetails | None = None,
    quote: QuoteResponse | None = None,
    period_start: datetime | None = None,
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
        period_start (datetime | None, optional): When set and the period produces no transactions, the balance
            at this date is fetched and shown as the carried-forward opening balance.

        Tuple[str, LedgerAccountDetails]: A tuple containing a formatted string with the balance printout and the LedgerAccountDetails object.

    """

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

    # When a period filter is active, fetch the opening balance at period_start and
    # either: (a) use it directly if there are no transactions in the period, or
    # (b) apply it as an offset to all running totals so the display shows cumulative
    # balances from the start of time, not just the incremental period change.
    opening_balance_carried_forward = False
    opening_details: LedgerAccountDetails | None = None
    if period_start is not None:
        opening_details = await one_account_balance(
            account=account, as_of_date=period_start, use_cache=False
        )

    if (
        not ledger_account_details.balances
        and opening_details is not None
        and opening_details.balances
    ):
        ledger_account_details = opening_details
        opening_balance_carried_forward = True
    elif (
        ledger_account_details.balances
        and opening_details is not None
        and opening_details.balances_net
    ):
        # Apply opening balance as offset to every running total in the period window.
        for unit, rows in ledger_account_details.balances.items():
            opening_net = opening_details.balances_net.get(unit, Decimal(0))
            opening_conv = opening_details.balances_totals.get(unit, ConvertedSummary())
            if opening_net == Decimal(0):
                continue
            for row in rows:
                row.amount_running_total += opening_net
                row.conv_running_total = row.conv_running_total + opening_conv
        ledger_account_details._recompute_summaries()

    units = set(ledger_account_details.balances.keys())
    if not quote:
        quote = await TrackedBaseModel.update_quote()

    as_of_date_printout = as_of_date if as_of_date else datetime.now(tz=timezone.utc)
    title_line = f"{account} balance as of {as_of_date_printout:%Y-%m-%d %H:%M:%S} UTC"
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
    if opening_balance_carried_forward:
        output.append(
            f"No new transactions since {period_start.date()} — opening balance carried forward:"
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
        # Compute common KSATS display settings for this unit
        conversion_factor, use_ksats, display_unit = _compute_ksats_settings(
            ledger_account_details, unit
        )

        # Headings on same line as Unit
        left_pad = COL_TS + 1 + COL_DESC + 1 + 4  # Space covering TS, desc, contra and separators
        output.append(
            f"\nUnit: {display_unit:<{left_pad - 6}} "
            f"{'Debit ':>{COL_DEBIT}} "
            f"{'Credit ':>{COL_CREDIT}} "
            f"{'Total ':>{COL_BAL}} "
            f"{'Short ID ':>{COL_SHORT_ID}} "
            f"{'Ledger Type':>{COL_LEDGER_TYPE}}"
        )
        # Underline for Unit and headings
        output.append(
            f"{'-' * 10:<{left_pad}} "
            f"{'-' * COL_DEBIT} "
            f"{'-' * COL_CREDIT} "
            f"{'-' * COL_BAL} "
            f"{'-' * COL_SHORT_ID} "
            f"{'-' * COL_LEDGER_TYPE}"
        )
        all_rows = ledger_account_details.balances[unit]
        if all_rows:
            # If there's an opening balance from a prior period, emit a synthetic
            # "=== <period_start date> ===" block with an "Opening Balance" row first.
            if (
                not opening_balance_carried_forward
                and opening_details is not None
                and opening_details.balances_net
            ):
                ob_net = opening_details.balances_net.get(unit, Decimal(0))
                if ob_net != Decimal(0):
                    ob_date_str = period_start.strftime("%Y-%m-%d")  # type: ignore[union-attr]
                    output.append(f"\n=== {ob_date_str} ===")
                    _, _, ob_bal_fmt = _format_amounts_for_display(
                        unit,
                        Decimal(0),
                        Decimal(0),
                        ob_net,
                        conversion_factor,
                        use_ksats,
                        msats_nonks_format="one_decimal",
                    )
                    ob_desc = truncate_text("Opening Balance", 50)
                    output.append(
                        f"{'':>{COL_TS}} "
                        f"{ob_desc:<{COL_DESC}} "
                        f"{'   '} "
                        f"{'':>{COL_DEBIT}} "
                        f"{'':>{COL_CREDIT}} "
                        f"{ob_bal_fmt:>{COL_BAL}} "
                        f"{'':>{COL_SHORT_ID}} "
                        f"{'ob':>{COL_LEDGER_TYPE}}"
                    )

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

                    # Format values consistently using helper (use one_decimal style for non-KSATS msats in this view)
                    debit_fmt, credit_fmt, balance_fmt = _format_amounts_for_display(
                        unit,
                        debit_val,
                        credit_val,
                        balance_val,
                        conversion_factor,
                        use_ksats,
                        msats_nonks_format="one_decimal",
                    )
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
                        memo = truncate_text(LightningMemo(row.user_memo).short_memo, 60)
                        output.append(f"{' ' * (COL_TS + 1)} {memo}")

        # Perform a conversion with the current quote for this Currency unit
        final_balance = Decimal(ledger_account_details.balances_net.get(unit, 0))
        conversion = CryptoConversion(conv_from=unit, value=final_balance, quote=quote).conversion
        output.append("-" * max_width)
        output.append(_format_converted_line(conversion, use_ksats))
        total_usd += conversion.usd
        total_msats += conversion.msats

        output.append("-" * max_width)
        display_balance = (
            final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
        )
        output.append(_format_final_balance_line(display_unit, display_balance, unit, use_ksats))

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>18,.3f} USD")
    total_sats = total_msats / Decimal(1000)
    if abs(total_sats) >= Decimal(1_000_000):
        output.append(f"Total SATS: {(total_sats / Decimal(1000)):>17,.1f} KSATS")
    else:
        output.append(f"Total SATS: {total_sats:>17,.3f} SATS")
    output.append(_add_notes())

    output.append(title_line)

    output.append("=" * max_width + "\n")
    output_text = "\n".join(output)

    return output_text, ledger_account_details


@async_time_decorator
async def account_balance_printout_grouped_by_customer(
    account: LedgerAccount | str,
    line_items: bool = True,
    user_memos: bool = True,
    as_of_date: datetime | None = None,
    age: timedelta | None = None,
    ledger_account_details: LedgerAccountDetails | None = None,
    period_start: datetime | None = None,
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
        period_start (datetime | None, optional): When set and the period produces no transactions, the balance
            at this date is fetched and shown as the carried-forward opening balance.

    Returns:
        str: A formatted string containing the balance with customer-grouped transactions.
    """

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

    # When a period filter is active, fetch the opening balance at period_start and
    # either: (a) use it directly if there are no transactions in the period, or
    # (b) apply it as an offset to all running totals so the display shows cumulative
    # balances from the start of time, not just the incremental period change.
    opening_balance_carried_forward = False
    opening_details: LedgerAccountDetails | None = None
    if period_start is not None:
        opening_details = await one_account_balance(
            account=account, as_of_date=period_start, use_cache=False
        )

    if (
        not ledger_account_details.balances
        and opening_details is not None
        and opening_details.balances
    ):
        ledger_account_details = opening_details
        opening_balance_carried_forward = True
    elif (
        ledger_account_details.balances
        and opening_details is not None
        and opening_details.balances_net
    ):
        # Apply opening balance as offset to every running total in the period window.
        for unit, rows in ledger_account_details.balances.items():
            opening_net = opening_details.balances_net.get(unit, Decimal(0))
            opening_conv = opening_details.balances_totals.get(unit, ConvertedSummary())
            if opening_net == Decimal(0):
                continue
            for row in rows:
                row.amount_running_total += opening_net
                row.conv_running_total = row.conv_running_total + opening_conv
        ledger_account_details._recompute_summaries()

    units = set(ledger_account_details.balances.keys())
    quote = await TrackedBaseModel.update_quote()

    as_of_date_printout = as_of_date if as_of_date else datetime.now(tz=timezone.utc)
    title_line = f"{account} balance as of {as_of_date_printout:%Y-%m-%d %H:%M:%S} UTC (Grouped by Customer)"
    output = ["_" * max_width]
    output.append(title_line)
    output.append(f"Units: {', '.join(unit.upper() for unit in units)}")
    if opening_balance_carried_forward:
        output.append(
            f"No new transactions since {period_start.date()} — opening balance carried forward:"
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
        conversion_factor = 1_000 if unit.upper() == "MSATS" else 1
        final_bal_for_unit = Decimal(ledger_account_details.balances_net.get(unit, 0))
        display_balance_total = (
            (final_bal_for_unit / conversion_factor)
            if unit.upper() == "MSATS"
            else final_bal_for_unit
        )
        use_ksats = unit.upper() == "MSATS" and abs(display_balance_total) >= Decimal(1_000_000)
        display_unit = (
            "KSATS" if use_ksats else ("SATS" if unit.upper() == "MSATS" else unit.upper())
        )

        # Headings on same line as Unit
        left_pad = COL_TS + 1 + COL_DESC + 1 + 4  # Space covering TS, desc, contra and separators
        output.append(
            f"\nUnit: {display_unit:<{left_pad - 6}} "
            f"{'Debit':>{COL_DEBIT}} "
            f"{'Credit':>{COL_CREDIT}} "
            f"{'Total':>{COL_BAL}} "
            f"{'Short ID':>{COL_SHORT_ID}} "
            f"{'Ledger Type':>{COL_LEDGER_TYPE}}"
        )
        # Underline for Unit and headings
        output.append(
            f"{'-' * 10:<{left_pad}} "
            f"{'-' * COL_DEBIT} "
            f"{'-' * COL_CREDIT} "
            f"{'-' * COL_BAL} "
            f"{'-' * COL_SHORT_ID} "
            f"{'-' * COL_LEDGER_TYPE}"
        )
        all_rows = ledger_account_details.balances[unit]
        if all_rows:
            # If there's an opening balance from a prior period, emit a synthetic
            # "=== <period_start date> ===" block with an "Opening Balance" row first.
            if (
                not opening_balance_carried_forward
                and opening_details is not None
                and opening_details.balances_net
            ):
                ob_net = opening_details.balances_net.get(unit, Decimal(0))
                if ob_net != Decimal(0):
                    ob_date_str = period_start.strftime("%Y-%m-%d")  # type: ignore[union-attr]
                    output.append(f"\n=== {ob_date_str} ===")
                    ob_display = ob_net / conversion_factor if unit.upper() == "MSATS" else ob_net
                    ob_fmt = f"{ob_display:>11,.3f}"
                    ob_desc = truncate_text("Opening Balance", 50)
                    output.append(
                        f"{'':>{COL_TS}} "
                        f"{ob_desc:<{COL_DESC}} "
                        f"{'   '} "
                        f"{'':>{COL_DEBIT}} "
                        f"{'':>{COL_CREDIT}} "
                        f"{ob_fmt:>{COL_BAL}} "
                        f"{'':>{COL_SHORT_ID}} "
                        f"{'ob':>{COL_LEDGER_TYPE}}"
                    )

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
                                # Convert from msats to sats first
                                debit_val /= conversion_factor
                                credit_val /= conversion_factor
                                balance_val /= conversion_factor
                                # If section is KSATS, convert sats to ksats for display
                                if use_ksats:
                                    debit_val /= Decimal(1000)
                                    credit_val /= Decimal(1000)
                                    balance_val /= Decimal(1000)

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
                                memo = truncate_text(LightningMemo(row.user_memo).short_memo, 60)
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
                            # If section is KSATS, convert sats to ksats for display and use 1 decimal
                            if use_ksats:
                                debit_val /= Decimal(1000)
                                credit_val /= Decimal(1000)
                                balance_val /= Decimal(1000)

                        # Number formats
                        if unit.upper() == "MSATS":
                            # Use 1 decimal for KSATS view, otherwise integer sats
                            if use_ksats:
                                debit_fmt = f"{debit_val:,.1f}"
                                credit_fmt = f"{credit_val:,.1f}"
                                balance_fmt = f"{balance_val:,.1f}"
                            else:
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
                            memo = truncate_text(LightningMemo(row.user_memo).short_memo, 60)
                            output.append(f"{' ' * (COL_TS + 1)} {memo}")

        # Perform a conversion with the current quote for this Currency unit
        final_balance = ledger_account_details.balances_net.get(unit, 0)

        conversion = CryptoConversion(conv_from=unit, value=final_balance, quote=quote).conversion
        output.append("-" * max_width)
        output.append(_format_converted_line(conversion, use_ksats))
        total_usd += conversion.usd
        total_msats += conversion.msats

        output.append("-" * max_width)
        display_balance = (
            final_balance / conversion_factor if unit.upper() == "MSATS" else final_balance
        )
        output.append(_format_final_balance_line(display_unit, display_balance, unit, use_ksats))

    output.append("-" * max_width)
    output.append(f"Total USD: {total_usd:>18,.3f} USD")
    total_sats = total_msats / Decimal(1000)
    if abs(total_sats) >= Decimal(1_000_000):
        output.append(f"Total SATS: {(total_sats / Decimal(1000)):>17,.1f} KSATS")
    else:
        output.append(f"Total SATS: {total_sats:>17,.3f} SATS")
    output.append(_add_notes())

    output.append(title_line)

    output.append("=" * max_width + "\n")
    output_text = "\n".join(output)

    return output_text, ledger_account_details


async def list_active_account_subs(
    account_name: str,
    min_transactions: int = 2,
) -> Set[str]:
    """
    Returns the list of account sub identifiers that have at least `min_transactions`
    ledger entries for the given account name.

    This is a lightweight query (no balance computation) intended to pre-filter
    which accounts are worth running the expensive full balance aggregation on.

    Args:
        account_name: The account name to filter by (e.g. "VSC Liability").
        min_transactions: Minimum number of transactions to consider an account active.
            Defaults to 2.

    Returns:
        Set[str]: Set of active sub identifiers.
    """
    pipeline = active_account_subs_pipeline(account_name, min_transactions)
    cursor = await LedgerEntry.collection().aggregate(pipeline=pipeline)
    results = await cursor.to_list()
    return {doc["sub"] for doc in results}


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


@async_time_decorator
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


# @async_time_decorator
async def keepsats_balance(
    cust_id: CustIDType = "",
    as_of_date: datetime | None = None,
    line_items: bool = False,
    notifications: bool = False,
) -> Tuple[Decimal, LedgerAccountDetails]:
    """
    Retrieves the balance of Keepsats for a specific customer as of a given date.
    This looks at the `credit` values because credits to a Liability account
    represent deposits, while debits represent withdrawals.
    Adds a net_balance field to the output summing up deposits and withdrawals

    Args:
        cust_id (str): The customer ID for which to retrieve the Keepsats balance.
        as_of_date (datetime, optional): The date up to which to calculate the balance. Defaults to the current UTC time.
        notifications (bool): If True, include non-financial notification entries in the returned line items.

    Returns:
        Tuple:
        net_msats (int): The net balance of Keepsats in milisatoshis.
        LedgerAccountDetails: An object containing the balance details for the specified customer.
    """
    account = LiabilityAccount(
        name="VSC Liability",
        sub=cust_id,
        contra=False,
    )
    account_balance = await one_account_balance(
        account=account,
        as_of_date=as_of_date,
        age=None,
    )

    if notifications:
        # Add non-financial notifications from hive_ops to the transaction history
        await notification_lines(cust_id=cust_id, account=account, account_balance=account_balance)

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


# @async_time_decorator
async def notification_lines(
    cust_id: str, account: LiabilityAccount, account_balance: LedgerAccountDetails
) -> None:
    """
    Fetches non-financial notifications for a given customer and adds them as synthetic ledger lines to the account balance history.
    This allows the frontend to display notifications alongside financial transactions in the account history.

    Args:
        cust_id (str): The customer ID for which to fetch notifications.
        account (LiabilityAccount): The account for which to add notification lines.
        account_balance (LedgerAccountDetails): The account balance details object to which notification lines will be added.

    Side Effects:
        The combined_balance field will be modified in place.
    """

    pipeline = account_notifications_pipeline(cust_id)
    cursor = await InternalConfig.db["hive_ops"].aggregate(pipeline=pipeline)
    notifications_list = await cursor.to_list(length=None)
    notifications_list = convert_decimal128_to_decimal(notifications_list)

    notification_lines: list[AccountBalanceLine] = []
    for notif in notifications_list:
        timestamp = notif.get("timestamp")
        # Create a synthetic ledger line for the notification so the frontend can render it
        trx_id = notif.get("trx_id", "")
        line = AccountBalanceLine(
            group_id=notif.get("parent_id", ""),
            short_id=notif.get("short_id", ""),
            ledger_type="notification",
            ledger_type_str="Notification",
            icon="🔔",
            timestamp=timestamp,
            timestamp_unix=(timestamp.timestamp() * 1000) if timestamp else 0,
            description=notif.get("memo", ""),
            user_memo=notif.get("memo", ""),
            cust_id=cust_id,
            op_type="notification",
            account_type=account.account_type,
            name=account.name,
            sub=account.sub,
            contra=False,
            amount=Decimal(0),
            amount_signed=Decimal(0),
            unit="",
            conv=None,
            conv_signed=CryptoConv(),
            side="",
            amount_running_total=Decimal(0),
            conv_running_total=ConvertedSummary(),
            trx_id=trx_id,
            link=f"https://hivehub.dev/tx/{trx_id}" if trx_id else "",
            checkCode=notif.get("short_id", ""),
            hiveAccTo=notif.get("hive_accname_to", ""),
            hiveAccFrom=notif.get("hive_accname_from", ""),
            paid=False,
            paidDate=timestamp,
            amountString="",
            currencyToSend="",
            lightning=False,
            usd=Decimal(0),
        )
        notification_lines.append(line)

    # Merge into the combined balance and re-sort
    account_balance.combined_balance.extend(notification_lines)
    account_balance.combined_balance.sort(
        key=lambda x: x.timestamp or datetime.min.replace(tzinfo=timezone.utc)
    )

    # Recompute running totals for the combined history (notifications are zero-value)
    running_conv = ConvertedSummary()
    for line in account_balance.combined_balance:
        if line.timestamp:
            line.timestamp_unix = line.timestamp.timestamp() * 1000
        running_conv = running_conv + ConvertedSummary.from_crypto_conv(line.conv_signed)
        line.conv_running_total = running_conv

    # Update last transaction date to include notifications
    if account_balance.combined_balance:
        account_balance.last_transaction_date = max(
            (line.timestamp for line in account_balance.combined_balance if line.timestamp),
            default=account_balance.last_transaction_date,
        )
