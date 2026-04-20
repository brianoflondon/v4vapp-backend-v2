"""
Account Balances Router

Handles routes for displaying account balances and ledger information.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import Optional
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    account_balance_printout_grouped_by_customer,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_checkpoints import (
    PeriodType,
    build_checkpoints_for_period,
    create_checkpoint,
    last_completed_period_end,
    latest_period_create_checkpoint,
)
from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults, run_all_sanity_checks
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers.helper_functions import get_accounts_by_type_for_selector
from v4vapp_backend_v2.config.setup import InternalConfig, logger
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


@router.get("/", response_class=HTMLResponse)
async def accounts_page(request: Request, flash: str = ""):
    """Main accounts page with account selector"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    accounts_by_type = await get_accounts_by_type_for_selector()

    nav_items = nav_manager.get_navigation_items("/admin/accounts")
    exception_sub_accounts = InternalConfig().config.development.allowed_hive_accounts

    # Fetch pending transactions
    pending_transactions = await PendingTransaction.list_all_str()
    sanity_results = await run_all_sanity_checks()

    checkpoint_message = None
    checkpoint_error = None
    if flash.startswith("checkpoint_created="):
        checkpoint_message = f"✅ {flash.split('=', 1)[1]}"
    elif flash.startswith("checkpoint_error="):
        checkpoint_error = f"❌ Build failed: {flash.split('=', 1)[1]}"

    return templates.TemplateResponse(
        request,
        "accounts/accounts.html.jinja",
        {
            "request": request,
            "title": "Account Balances",
            "nav_items": nav_items,
            "accounts_by_type": accounts_by_type,
            "pending_transactions": pending_transactions,
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Accounts", "url": "/admin/accounts"},
            ],
            "sanity_results": sanity_results,
            "exception_sub_accounts": exception_sub_accounts,
            "checkpoint_message": checkpoint_message,
            "checkpoint_error": checkpoint_error,
        },
    )


def account_balance_printout_by_customer(account, line_items, user_memos, as_of_date, age):
    raise NotImplementedError


@router.get("/balance/user/{acc_name}", response_class=HTMLResponse)
async def get_user_balance_get(
    request: Request,
    acc_name: str,
):
    """Get balance printout for a specific VSC Liability user account (GET version)"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    sanity_results_task = asyncio.create_task(run_all_sanity_checks())

    exception_sub_accounts = InternalConfig().config.development.allowed_hive_accounts

    try:
        # Default parameters for GET request
        line_items_bool = True
        user_memos_bool = True
        customer_grouping_bool = False

        # Construct the account string for VSC Liability account
        account_string = f"VSC Liability (Liability) - Sub: {acc_name}"

        # Parse the account from string
        account = LedgerAccount.from_string(account_string)

        # Use current time for GET requests
        as_of_date = datetime.now(tz=timezone.utc)
        age = timedelta(seconds=0)  # No age for GET requests

        if customer_grouping_bool:
            printout, details = await account_balance_printout_grouped_by_customer(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
            )
        else:
            printout, details = await account_balance_printout(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
            )

        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        accounts_by_type = await get_accounts_by_type_for_selector()

        # Convert details to JSON-serializable format
        details_json = None
        if details:
            try:
                # Convert Pydantic model to dictionary, then to JSON-serializable format
                details_json = details.model_dump(mode="json")
            except Exception as e:
                # Fallback: try to convert to dict
                try:
                    details_json = dict(details)
                except Exception:
                    # Last resort: convert to string representation
                    details_json = {
                        "error": f"Could not serialize details: {str(e)}",
                        "string_repr": str(details),
                    }
        sanity_results = await sanity_results_task
        return templates.TemplateResponse(
            request,
            "accounts/balance_result.html.jinja",
            {
                "request": request,
                "title": f"Balance: VSC Liability (Liability) - Sub: {acc_name}",
                "nav_items": nav_items,
                "account": account,
                "account_string": account_string,
                "printout": printout,
                "details": details_json,
                "line_items": line_items_bool,
                "user_memos": user_memos_bool,
                "as_of_date": as_of_date,
                "display_period": "all",
                "accounts_by_type": accounts_by_type,
                "exception_sub_accounts": exception_sub_accounts,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": f"VSC Liability ({acc_name})", "url": "#"},
                ],
                "sanity_results": sanity_results,
            },
        )

    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        return templates.TemplateResponse(
            request,
            "accounts/balance_error.html.jinja",
            {
                "request": request,
                "title": "Balance Error",
                "nav_items": nav_items,
                "error": str(e),
                "account_string": f"VSC Liability (Liability) - Sub: {acc_name}",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": "Error", "url": "#"},
                ],
                "sanity_results": SanityCheckResults(),
            },
        )


@router.post("/balance/user/{acc_name}", response_class=HTMLResponse)
async def get_user_balance(
    request: Request,
    acc_name: str,
    line_items: Optional[str] = Form("true"),
    user_memos: Optional[str] = Form("true"),
    customer_grouping: Optional[str] = Form("false"),
    as_of_date_str: Optional[str] = Form(None),
    display_period: Optional[str] = Form("all"),
):
    """Get balance printout for a specific VSC Liability user account"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Convert string form values to booleans
        line_items_bool = bool(line_items and line_items.lower() in ("true", "on", "1"))
        user_memos_bool = bool(user_memos and user_memos.lower() in ("true", "on", "1"))
        customer_grouping_bool = bool(
            customer_grouping and customer_grouping.lower() in ("true", "on", "1")
        )

        # Enforce dependency: user memos only make sense when line items are shown.
        # If line items are disabled, force user_memos to False server-side as well.
        if not line_items_bool:
            user_memos_bool = False

        # Construct the account string for VSC Liability account
        account_string = f"VSC Liability (Liability) - Sub: {acc_name}"

        # Parse the account from string
        account = LedgerAccount.from_string(account_string)
        exception_sub_accounts = InternalConfig().config.development.allowed_hive_accounts

        # # Flush the Redis cache for this account so the display always reflects
        # # the latest ledger state.
        # await invalidate_ledger_cache(account.name, account.sub, account.name, account.sub)

        # Parse the as_of_date if provided
        as_of_date = datetime.now(tz=timezone.utc)
        if as_of_date_str:
            try:
                as_of_date = datetime.fromisoformat(as_of_date_str.replace("Z", "+00:00"))
            except ValueError:
                # If parsing fails, use current time
                pass

        # Compute age from calendar period boundaries
        age: timedelta | None = None
        period_start: datetime | None = None
        if display_period and display_period != "all":
            from v4vapp_backend_v2.accounting.ledger_checkpoints import (
                PeriodType,
                last_completed_period_end,
            )

            try:
                from v4vapp_backend_v2.accounting.ledger_checkpoints import create_checkpoint

                period_type = PeriodType(display_period)
                period_start = last_completed_period_end(period_type, as_of_date)
                age = as_of_date - period_start
                await create_checkpoint(account, period_type, period_start)
            except (ValueError, Exception):
                pass

        # Get the balance printout - choose function based on customer_grouping parameter
        if customer_grouping_bool:
            printout, details = await account_balance_printout_grouped_by_customer(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
                period_start=period_start,
            )
        else:
            printout, details = await account_balance_printout(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
                period_start=period_start,
            )

        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        accounts_by_type = await get_accounts_by_type_for_selector()

        # Convert details to JSON-serializable format
        details_json = None
        if details:
            try:
                # Convert Pydantic model to dictionary, then to JSON-serializable format
                details_json = details.model_dump(mode="json")
            except Exception as e:
                # Fallback: try to convert to dict
                try:
                    details_json = dict(details)
                except Exception:
                    # Last resort: convert to string representation
                    details_json = {
                        "error": f"Could not serialize details: {str(e)}",
                        "string_repr": str(details),
                    }
        sanity_results = await run_all_sanity_checks()
        return templates.TemplateResponse(
            request,
            "accounts/balance_result.html.jinja",
            {
                "request": request,
                "title": f"Balance: VSC Liability (Liability) - Sub: {acc_name}",
                "nav_items": nav_items,
                "account": account,
                "account_string": account_string,
                "printout": printout,
                "details": details_json,
                "line_items": line_items_bool,
                "user_memos": user_memos_bool,
                "customer_grouping": customer_grouping_bool,
                "as_of_date": as_of_date,
                "display_period": display_period or "all",
                "accounts_by_type": accounts_by_type,
                "exception_sub_accounts": exception_sub_accounts,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": f"VSC Liability ({acc_name})", "url": "#"},
                ],
                "sanity_results": sanity_results,
            },
        )

    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        return templates.TemplateResponse(
            request,
            "accounts/balance_error.html.jinja",
            {
                "request": request,
                "title": "Balance Error",
                "nav_items": nav_items,
                "error": str(e),
                "account_string": f"VSC Liability (Liability) - Sub: {acc_name}",
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": "Error", "url": "#"},
                ],
                "sanity_results": SanityCheckResults(),
            },
        )


@router.post("/balance", response_class=HTMLResponse)
async def get_account_balance(
    request: Request,
    account_string: str = Form(...),
    line_items: Optional[str] = Form("true"),
    user_memos: Optional[str] = Form("true"),
    customer_grouping: Optional[str] = Form("false"),
    as_of_date_str: Optional[str] = Form(None),
    display_period: Optional[str] = Form("all"),
):
    """Get balance printout for a specific account"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Convert string form values to booleans
        line_items_bool = bool(line_items and line_items.lower() in ("true", "on", "1"))
        user_memos_bool = bool(user_memos and user_memos.lower() in ("true", "on", "1"))
        customer_grouping_bool = bool(
            customer_grouping and customer_grouping.lower() in ("true", "on", "1")
        )

        # Enforce dependency: user memos only make sense when line items are shown.
        # If line items are disabled, force user_memos to False server-side as well.
        if not line_items_bool:
            user_memos_bool = False

        # Parse the account from string
        account = LedgerAccount.from_string(account_string)

        # Flush the Redis cache for this account so the display always reflects
        # the latest ledger state (avoids stale data when navigating back to the page).
        # await invalidate_ledger_cache(account.name, account.sub)

        as_of_date = None
        if as_of_date_str:
            try:
                as_of_date = datetime.fromisoformat(as_of_date_str.replace("Z", "+00:00"))
            except ValueError:
                # If parsing fails, use current time
                pass

        # Compute age from calendar period boundaries so that "monthly" means
        # "since last month end", not a rolling 30-day window.
        age: timedelta | None = None
        period_start: datetime | None = None
        if display_period and display_period != "all":
            try:
                checkpoint, created, age, period_start = await latest_period_create_checkpoint(
                    account, PeriodType(display_period)
                )
            except (ValueError, Exception):
                pass

        # Get the balance printout - choose function based on customer_grouping parameter
        if customer_grouping_bool:
            printout, details = await account_balance_printout_grouped_by_customer(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
                period_start=period_start,
            )
        else:
            printout, details = await account_balance_printout(
                account=account,
                line_items=line_items_bool,
                user_memos=user_memos_bool,
                as_of_date=as_of_date,
                age=age,
                period_start=period_start,
            )

        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        accounts_by_type = await get_accounts_by_type_for_selector()

        # Convert details to JSON-serializable format
        details_json = None
        if details:
            try:
                # Convert Pydantic model to dictionary, then to JSON-serializable format
                details_json = details.model_dump(mode="json")
            except Exception as e:
                # Fallback: try to convert to dict
                try:
                    details_json = dict(details)
                except Exception:
                    # Last resort: convert to string representation
                    details_json = {
                        "error": f"Could not serialize details: {str(e)}",
                        "string_repr": str(details),
                    }
        sanity_results = await run_all_sanity_checks()
        if as_of_date is None:
            as_of_date = datetime.now(tz=timezone.utc)
        exception_sub_accounts = InternalConfig().config.development.allowed_hive_accounts
        return templates.TemplateResponse(
            request,
            "accounts/balance_result.html.jinja",
            {
                "request": request,
                "title": f"Balance: {account}",
                "nav_items": nav_items,
                "account": account,
                "account_string": account_string,
                "printout": printout,
                "details": details_json,
                "line_items": line_items_bool,
                "user_memos": user_memos_bool,
                "customer_grouping": customer_grouping_bool,
                "as_of_date": as_of_date,
                "display_period": display_period or "all",
                "accounts_by_type": accounts_by_type,
                "exception_sub_accounts": exception_sub_accounts,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": f"{account.name} ({account.sub})", "url": "#"},
                ],
                "sanity_results": sanity_results,
            },
        )

    except Exception as e:
        logger.error("Error fetching account balance")
        logger.exception(e)
        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        return templates.TemplateResponse(
            request,
            "accounts/balance_error.html.jinja",
            {
                "request": request,
                "title": "Balance Error",
                "nav_items": nav_items,
                "error": str(e),
                "account_string": account_string,
                "pending_transactions": await PendingTransaction.list_all_str(),
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": "Error", "url": "#"},
                ],
                "sanity_results": SanityCheckResults(),
            },
        )


# ---------------------------------------------------------------------------
# Checkpoint creation endpoint
# ---------------------------------------------------------------------------


@router.post("/balance/checkpoint", response_class=RedirectResponse)
async def create_account_checkpoint(
    request: Request,
    account_string: str = Form(...),
    period_type_str: str = Form("monthly"),
) -> RedirectResponse:
    """Create a balance checkpoint for the given account at the end of the last completed period."""

    try:
        now = datetime.now(tz=timezone.utc)
        account = LedgerAccount.from_string(account_string)
        period_type = PeriodType(period_type_str)
        period_end = last_completed_period_end(period_type, now)
        checkpoint, new_checkpoint = await create_checkpoint(account, period_type, period_end)
        logger.info(
            f"📌 Admin {'created' if new_checkpoint else 'fetched'} {period_type} checkpoint for "
            f"{account.name}:{account.sub} @ {checkpoint.period_end.date()}",
            extra={"notification": False},
        )
        flash_msg = f"checkpoint_created={period_type}:{checkpoint.period_end.date()}"
    except Exception as e:
        logger.exception(f"Failed to create checkpoint: {e}")
        flash_msg = "checkpoint_error=Failed to create checkpoint"

    params = urlencode({"account_string": account_string, "flash": flash_msg})
    return RedirectResponse(url=f"/admin/accounts/balance/post?{params}", status_code=303)


# ---------------------------------------------------------------------------
# Build all checkpoints endpoint
# ---------------------------------------------------------------------------


@router.post("/balance/build-checkpoints", response_class=HTMLResponse)
async def build_all_checkpoints(
    request: Request,
    account_string: str = Form(""),
    period_type_str: str = Form("monthly"),
):
    """Run build_checkpoints_for_period for all accounts for the selected period type."""

    try:
        period_type = PeriodType(period_type_str)
        start = timer()
        total = await build_checkpoints_for_period(period_type)
        elapsed = timer() - start
        logger.info(
            f"📌 Admin triggered build_checkpoints_for_period({period_type}): {total} written in {elapsed:.2f} s.",
            extra={"notification": False},
        )
        flash_msg = f"checkpoint_created=Built {total} {period_type} checkpoints for all accounts in {elapsed:.2f} s."
    except Exception as e:
        logger.exception(f"Failed to build checkpoints: {e}")
        flash_msg = "checkpoint_error=Failed to build checkpoints"

    params = urlencode({"flash": flash_msg})
    return RedirectResponse(url=f"/admin/accounts?{params}", status_code=303)


@router.get("/balance/post", response_class=HTMLResponse)
async def balance_after_redirect(
    request: Request,
    account_string: str = "",
    flash: str = "",
):
    """GET landing page after a checkpoint redirect — auto-submits back to the balance POST."""
    nav_items = nav_manager.get_navigation_items("/admin/accounts")
    sanity_results = await run_all_sanity_checks()

    checkpoint_message = None
    checkpoint_error = None
    if flash.startswith("checkpoint_created="):
        checkpoint_message = f"✅ Checkpoint created: {flash.split('=', 1)[1]}"
    elif flash.startswith("checkpoint_error="):
        checkpoint_error = f"❌ Checkpoint failed: {flash.split('=', 1)[1]}"

    return templates.TemplateResponse(
        request,
        "accounts/balance_post_redirect.html.jinja",
        {
            "request": request,
            "title": "Balance",
            "nav_items": nav_items,
            "account_string": account_string,
            "checkpoint_message": checkpoint_message,
            "checkpoint_error": checkpoint_error,
            "pending_transactions": await PendingTransaction.list_all_str(),
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Accounts", "url": "/admin/accounts"},
                {"name": "Balance", "url": "#"},
            ],
            "sanity_results": sanity_results,
        },
    )
