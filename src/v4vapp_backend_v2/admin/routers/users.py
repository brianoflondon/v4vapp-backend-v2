"""
Users Router

Handles routes for displaying VSC Liability user accounts.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from timeit import default_timer as timer
from typing import Any, List, Optional, cast

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import (
    all_account_balances_summary,
    check_hive_conversion_limits,
    list_active_account_subs,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.limit_check_classes import LimitCheckResult
from v4vapp_backend_v2.accounting.pipelines.fee_aggregation_pipelines import (
    FeeAggregationResult,
    fee_aggregation_pipeline,
)
from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive.witness_details import get_witness_voters
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

router = APIRouter()

# Will be set by the main app
templates: Optional[Jinja2Templates] = None
nav_manager: Optional[NavigationManager] = None


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager):
    """Set the templates and navigation manager"""
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


def format_sats_for_label(sats: int) -> str:
    """Format sats for display in labels (e.g., 400000 -> '400k', 1200000 -> '1.2M')"""
    if sats >= 1_000_000:
        return f"{round(sats / 1_000_000, 1):.1f}M"
    elif sats >= 1_000:
        return f"{round(sats // 1_000, 0):.0f}k"
    else:
        return str(sats)


def get_limit_entries():
    """Get lightning rate limits from V4VConfig and format them with labels"""
    lightning_rate_limits = V4VConfig().data.lightning_rate_limits

    return [
        {
            "hours": limit.hours,
            "sats": limit.sats,
            "label": f"{limit.hours}h ({format_sats_for_label(int(limit.sats))})",
        }
        for limit in lightning_rate_limits
    ]


# @async_time_stats_decorator()
@router.get("/data")
async def users_data_api(active_only: bool = True) -> dict[str, Any]:
    """API endpoint to fetch user data asynchronously.

    Args:
        active_only: When True (default), pre-filters to accounts with more than
            1 transaction before running the expensive balance aggregation.
            This significantly speeds up the response for large account sets.
    """
    start = timer()
    witness_voters_task = asyncio.create_task(
        get_witness_voters(witness_name="brianoflondon")
    )  # Start fetching witness voters in the background

    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    # Build a filter to restrict the expensive aggregation to active accounts only
    if active_only:
        active_cust_ids = await list_active_account_subs(
            account_name="VSC Liability", min_transactions=2
        )
        # Remove "OpeningBalance" from the active_cust_ids
        active_cust_ids = {cust_id for cust_id in active_cust_ids if cust_id != "OpeningBalance"}
    else:
        active_cust_ids = None  # No filtering, include all accounts
    account_balances = await all_account_balances_summary(
        account_name="VSC Liability", cust_ids=active_cust_ids
    )

    vsc_liability_balances = account_balances.root
    vsc_liability_balances.sort(key=lambda x: x.sub)

    # Aggregate fee totals for the users returned by the balance summary
    fee_docs = []
    if vsc_liability_balances:
        fee_cursor = await LedgerEntry.collection().aggregate(
            pipeline=fee_aggregation_pipeline([account.sub for account in vsc_liability_balances])
        )
        fee_docs = await fee_cursor.to_list(length=None)
        fee_docs = convert_decimal128_to_decimal(fee_docs)

    fee_map: dict[str, FeeAggregationResult] = {
        doc["cust_id"]: FeeAggregationResult.model_validate(doc)
        for doc in fee_docs
        if doc.get("cust_id")
    }

    # Get balances for each account
    users_data: List[dict[str, Any]] = []

    # Prepare concurrent tasks for limit checks
    limit_check_tasks = [
        check_hive_conversion_limits(cust_id=account.sub) for account in vsc_liability_balances
    ]

    witness_voters = await witness_voters_task
    if not witness_voters:
        logger.warning("No witness voters found for brianoflondon")
        witness_voter_names = set()
    else:
        witness_voter_names = set(witness_voters)
    # Run all limit checks concurrently
    limit_check_results = await asyncio.gather(*limit_check_tasks, return_exceptions=True)
    for i, account in enumerate(vsc_liability_balances):
        try:
            balance_sats = account.sats  # Convert msats to sats
            # Get the limit check result (handle exceptions)
            limit_result = limit_check_results[i]
            if isinstance(limit_result, Exception):
                raise limit_result
            check_limits = cast(LimitCheckResult, limit_result)

            balance_usd = account.conv_total.usd
            balance_usd_fmt = f"{balance_usd:,.2f}"

            # Format the balance for display
            if balance_sats > 0:
                balance_sats_fmt = f"{balance_sats:,.0f}"
            elif balance_sats < 0:
                balance_sats_fmt = f"{balance_sats:,.0f}"
            else:
                balance_sats_fmt = "0"

            fee_summary = fee_map.get(
                account.sub,
                FeeAggregationResult(cust_id=account.sub),
            )
            fee_total = int(fee_summary.total_sats)
            fee_total_fmt = f"{fee_total:,}" if fee_total else "0"

            if witness_voters and account.sub in witness_voter_names:
                witness_voter = True
                voter_data = witness_voters.get(account.sub, {})
                witness_vote_value = voter_data.get("vote_value", voter_data.get("total_value", 0))
                witness_vote_value_str = (
                    f"({witness_vote_value / 1e6:,.0f})" if witness_vote_value else ""
                )

            else:
                witness_voter = False
                witness_vote_value = 0
                witness_vote_value_str = ""

            users_data.append({
                "sub": account.sub,
                "balance_sats": int(balance_sats),
                "balance_sats_fmt": balance_sats_fmt,
                "balance_usd": int(balance_usd),
                "balance_usd_fmt": balance_usd_fmt,
                "total_fees": fee_total,
                "total_fees_fmt": fee_total_fmt,
                "fee_total_usd": fee_summary.total_usd,
                "has_transactions": account.has_transactions,
                "witness_voter": witness_voter,
                "witness_vote_value": witness_vote_value,
                "witness_vote_value_str": witness_vote_value_str,
                "last_transaction_date": account.last_transaction_date.isoformat()
                if account.last_transaction_date
                else None,
                "limit_percents": check_limits.percents,
                "limit_ok": check_limits.limit_ok,
                "limit_sats": check_limits.sats_list_str,
                "next_limit_expiry": check_limits.next_limit_expiry.isoformat()
                if check_limits.next_limit_expiry
                and isinstance(check_limits.next_limit_expiry, datetime)
                else check_limits.next_limit_expiry,
                # "age": format_time_delta(
                #     time_now - account.last_transaction_date, just_days_or_hours=True
                # ),
            })
        except Exception as e:
            logger.exception(
                f"Exception processing account {account.sub}: {e}", extra={"notification": False}
            )
            # If balance lookup fails, still show the user but with error
            users_data.append({
                "sub": account.sub,
                "balance_sats": None,
                "balance_sats_fmt": "Error",
                "total_fees": 0,
                "total_fees_fmt": "0",
                "fee_total_usd": Decimal(0),
                "has_transactions": False,
                "last_transaction_date": None,
                "error": str(e),
            })

    # Calculate summary statistics
    total_users = len(users_data)
    active_users = len([u for u in users_data if u["has_transactions"]])
    total_positive_balance = sum(
        balance
        for u in users_data
        if isinstance(u.get("balance_sats"), (int, float, Decimal)) and u["balance_sats"] > 0
        for balance in [u["balance_sats"]]
    )
    error_count = len([u for u in users_data if u.get("error")])

    # Format total balance
    if total_positive_balance > 0:
        total_positive_balance_fmt = f"{total_positive_balance:,.0f}"
    else:
        total_positive_balance_fmt = "0"

    result = {
        "users_data": users_data,
        "summary": {
            "total_users": total_users,
            "active_users": active_users,
            "total_positive_balance": total_positive_balance,
            "total_positive_balance_fmt": total_positive_balance_fmt,
            "error_count": error_count,
            "processing_time_seconds": round(timer() - start, 4),
        },
        "now": datetime.now(tz=timezone.utc).isoformat(),
    }

    return result


@router.get("/", response_class=HTMLResponse)
async def users_page(request: Request):
    """Main users page showing VSC Liability accounts - renders quickly with async data loading"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    nav_items = nav_manager.get_navigation_items("/admin/users")

    # Return page with empty data - actual data will be loaded via JavaScript
    return templates.TemplateResponse(
        request,
        "users/users.html.jinja",
        {
            "request": request,
            "title": "Users",
            "nav_items": nav_items,
            "users_data": [],  # Empty initially
            "limit_entries": get_limit_entries(),
            "pending_transactions": await PendingTransaction.list_all_str(),
            "now": datetime.now(tz=timezone.utc),
            "summary": {
                "total_users": 0,
                "active_users": 0,
                "total_positive_balance": 0,
                "total_positive_balance_fmt": "0",
                "error_count": 0,
            },
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Users", "url": "/admin/users"},
            ],
            # Provide a minimal empty sanity results model so templates that expect
            # `sanity_results` (base.html) don't fail when it's not provided.
            "sanity_results": SanityCheckResults(),
        },
    )


# Last line

# Last line
