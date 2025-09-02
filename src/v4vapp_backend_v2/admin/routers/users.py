"""
Users Router

Handles routes for displaying VSC Liability user accounts.
"""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    keepsats_balance,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.hive.v4v_config import V4VConfig

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
        return f"{sats / 1_000_000:.1f}M"
    elif sats >= 1_000:
        return f"{sats // 1_000}k"
    else:
        return str(sats)


def get_limit_entries():
    """Get lightning rate limits from V4VConfig and format them with labels"""
    lightning_rate_limits = V4VConfig().data.lightning_rate_limits
    return [
        {
            "hours": limit.hours,
            "sats": limit.sats,
            "label": f"{limit.hours}h ({format_sats_for_label(limit.sats)})",
        }
        for limit in lightning_rate_limits
    ]


@router.get("/", response_class=HTMLResponse)
async def users_page(request: Request):
    """Main users page showing VSC Liability accounts"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Get all accounts
        all_accounts = await list_all_accounts()
    except Exception:
        # If database is not available, show mock data for demo
        all_accounts = [
            LiabilityAccount(name="VSC Liability", sub="v4vapp-test"),
            LiabilityAccount(name="VSC Liability", sub="v4vapp.qrc"),
            LiabilityAccount(name="VSC Liability", sub="brianoflondon"),
        ]

    # Filter for VSC Liability accounts only
    vsc_liability_accounts = [
        account for account in all_accounts if account.name == "VSC Liability" and account.sub
    ]

    # Sort by sub account name
    vsc_liability_accounts.sort(key=lambda x: x.sub)

    # Get balances for each account
    users_data = []
    for account in vsc_liability_accounts:
        try:
            # Get the balance in msats and convert to sats
            net_msats, account_details = await keepsats_balance(
                cust_id=account.sub, as_of_date=datetime.now(tz=timezone.utc)
            )
            balance_sats = net_msats // 1000  # Convert msats to sats
            check_limits = await check_hive_conversion_limits(cust_id=account.sub)

            # Format the balance for display
            if balance_sats > 0:
                balance_sats_fmt = f"{balance_sats:,.0f}"
            elif balance_sats < 0:
                balance_sats_fmt = f"{balance_sats:,.0f}"
            else:
                balance_sats_fmt = "0"

            users_data.append(
                {
                    "sub": account.sub,
                    "balance_sats": balance_sats,
                    "balance_sats_fmt": balance_sats_fmt,
                    "has_transactions": balance_sats != 0 or bool(account_details.balances),
                    "limit_percents": check_limits.percents,
                    "limit_ok": check_limits.limit_ok,
                    "limit_sats": check_limits.sats_list_str,
                    "next_limit_expiry": check_limits.next_limit_expiry,
                }
            )
        except Exception as e:
            # If balance lookup fails, still show the user but with error
            users_data.append(
                {
                    "sub": account.sub,
                    "balance_sats": None,
                    "balance_sats_fmt": "Error",
                    "has_transactions": False,
                    "error": str(e),
                }
            )

    # Calculate summary statistics
    total_users = len(users_data)
    active_users = len([u for u in users_data if u["has_transactions"]])
    total_positive_balance = sum(
        u["balance_sats"]
        for u in users_data
        if u.get("balance_sats") and u["balance_sats"] > 0  # Use .get() for safety
    )
    error_count = len([u for u in users_data if u.get("error")])

    # Format total balance
    if total_positive_balance > 0:
        total_positive_balance_fmt = f"{total_positive_balance:,.0f}"
    else:
        total_positive_balance_fmt = "0"

    nav_items = nav_manager.get_navigation_items("/admin/users")

    return templates.TemplateResponse(
        "users/users.html",
        {
            "request": request,
            "title": "Users",
            "nav_items": nav_items,
            "users_data": users_data,
            "limit_entries": get_limit_entries(),
            "summary": {
                "total_users": total_users,
                "active_users": active_users,
                "total_positive_balance": total_positive_balance,
                "total_positive_balance_fmt": total_positive_balance_fmt,
                "error_count": error_count,
            },
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Users", "url": "/admin/users"},
            ],
        },
    )
