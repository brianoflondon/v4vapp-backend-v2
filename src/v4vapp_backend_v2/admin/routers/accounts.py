"""
Account Balances Router

Handles routes for displaying account balances and ledger information.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.admin.navigation import NavigationManager

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
async def accounts_page(request: Request):
    """Main accounts page with account selector"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Get all accounts grouped by type
        all_accounts = await list_all_accounts()
    except Exception:
        # If database is not available, show mock data for demo
        from v4vapp_backend_v2.accounting.ledger_account_classes import (
            AssetAccount,
            ExpenseAccount,
            LiabilityAccount,
            RevenueAccount,
        )

        all_accounts = [
            AssetAccount(name="Customer Deposits Hive", sub="devser.v4vapp"),
            AssetAccount(name="Treasury Lightning", sub="from_keepsats"),
            LiabilityAccount(name="VSC Liability", sub="v4vapp-test"),
            LiabilityAccount(name="VSC Liability", sub="v4vapp.qrc"),
            RevenueAccount(name="Fee Income Keepsats", sub="from_keepsats"),
            ExpenseAccount(name="Fee Expenses Lightning", sub=""),
        ]

    # Group accounts by account_type
    accounts_by_type: dict[str, list[LedgerAccount]] = {}
    for account in all_accounts:
        account_type = account.account_type.value
        if account_type not in accounts_by_type:
            accounts_by_type[account_type] = []
        accounts_by_type[account_type].append(account)

    # Sort each group by name and sub
    for account_type in accounts_by_type:
        accounts_by_type[account_type].sort(key=lambda x: (x.name, x.sub))

    nav_items = nav_manager.get_navigation_items("/admin/accounts")

    return templates.TemplateResponse(
        "accounts/accounts.html",
        {
            "request": request,
            "title": "Account Balances",
            "nav_items": nav_items,
            "accounts_by_type": accounts_by_type,
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Accounts", "url": "/admin/accounts"},
            ],
        },
    )


@router.post("/balance", response_class=HTMLResponse)
async def get_account_balance(
    request: Request,
    account_string: str = Form(...),
    line_items: Optional[str] = Form("true"),
    user_memos: Optional[str] = Form("true"),
    as_of_date_str: Optional[str] = Form(None),
    age_hours: Optional[int] = Form(0),
):
    """Get balance printout for a specific account"""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    try:
        # Convert string form values to booleans
        line_items_bool = bool(line_items and line_items.lower() in ("true", "on", "1"))
        user_memos_bool = bool(user_memos and user_memos.lower() in ("true", "on", "1"))

        # Enforce dependency: user memos only make sense when line items are shown.
        # If line items are disabled, force user_memos to False server-side as well.
        if not line_items_bool:
            user_memos_bool = False

        # Parse the account from string
        account = LedgerAccount.from_string(account_string)

        # Parse the as_of_date if provided
        as_of_date = datetime.now(tz=timezone.utc)
        if as_of_date_str:
            try:
                as_of_date = datetime.fromisoformat(as_of_date_str.replace("Z", "+00:00"))
            except ValueError:
                # If parsing fails, use current time
                pass

        # Create age timedelta
        age = timedelta(hours=age_hours) if age_hours and age_hours > 0 else timedelta(seconds=0)

        # Get the balance printout
        printout, details = await account_balance_printout(
            account=account,
            line_items=line_items_bool,
            user_memos=user_memos_bool,
            as_of_date=as_of_date,
            age=age,
        )

        nav_items = nav_manager.get_navigation_items("/admin/accounts")

        # Get all accounts for the selector
        try:
            all_accounts = await list_all_accounts()
        except Exception:
            # If database is not available, use mock data
            from v4vapp_backend_v2.accounting.ledger_account_classes import (
                AssetAccount,
                ExpenseAccount,
                LiabilityAccount,
                RevenueAccount,
            )

            all_accounts = [
                AssetAccount(name="Customer Deposits Hive", sub="devser.v4vapp"),
                AssetAccount(name="Treasury Lightning", sub="from_keepsats"),
                LiabilityAccount(name="VSC Liability", sub="v4vapp-test"),
                LiabilityAccount(name="VSC Liability", sub="v4vapp.qrc"),
                RevenueAccount(name="Fee Income Keepsats", sub="from_keepsats"),
                ExpenseAccount(name="Fee Expenses Lightning", sub=""),
            ]

        # Group accounts by type for the selector
        accounts_by_type = {}
        for acc in all_accounts:
            account_type = acc.account_type.value
            if account_type not in accounts_by_type:
                accounts_by_type[account_type] = []
            accounts_by_type[account_type].append(acc)

        # Sort each group
        for account_type in accounts_by_type:
            accounts_by_type[account_type].sort(key=lambda x: (x.name, x.sub))

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
                except:
                    # Last resort: convert to string representation
                    details_json = {
                        "error": f"Could not serialize details: {str(e)}",
                        "string_repr": str(details),
                    }

        return templates.TemplateResponse(
            "accounts/balance_result.html",
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
                "as_of_date": as_of_date,
                "age_hours": age_hours,
                "accounts_by_type": accounts_by_type,
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": f"{account.name} ({account.sub})", "url": "#"},
                ],
            },
        )

    except Exception as e:
        nav_items = nav_manager.get_navigation_items("/admin/accounts")
        return templates.TemplateResponse(
            "accounts/balance_error.html",
            {
                "request": request,
                "title": "Balance Error",
                "nav_items": nav_items,
                "error": str(e),
                "account_string": account_string,
                "breadcrumbs": [
                    {"name": "Admin", "url": "/admin"},
                    {"name": "Accounts", "url": "/admin/accounts"},
                    {"name": "Error", "url": "#"},
                ],
            },
        )
