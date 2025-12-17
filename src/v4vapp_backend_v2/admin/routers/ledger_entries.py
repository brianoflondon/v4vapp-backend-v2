"""
Ledger Entries Router

Provides a simple page and data endpoint to browse ledger entries.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_entries
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

router = APIRouter()

# Will be set by the main app
templates: Optional[Jinja2Templates] = None
nav_manager: Optional[NavigationManager] = None


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager):
    """Set templates and navigation manager"""
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


@router.get("/data")
async def ledger_entries_data(
    account_string: Optional[str] = None,
    short_id: Optional[str] = None,
    group_id: Optional[str] = None,
    as_of_date_str: Optional[str] = None,
    age_hours: Optional[int] = 0,
):
    """Return ledger entries in JSON form for AJAX or API use."""
    as_of_date = datetime.now(tz=timezone.utc)
    if as_of_date_str:
        try:
            as_of_date = datetime.fromisoformat(as_of_date_str.replace("Z", "+00:00"))
        except Exception:
            pass

    account = None
    if account_string:
        try:
            account = LedgerAccount.from_string(account_string)
        except Exception:
            account = None

    age = timedelta(hours=age_hours) if age_hours and age_hours > 0 else None

    ledger_entries = await get_ledger_entries(
        as_of_date=as_of_date, filter_by_account=account, group_id=group_id, short_id=short_id
    )

    # Minimal JSON-serializable representation
    entries = []
    for e in ledger_entries:
        entries.append(
            {
                "group_id": e.group_id,
                "short_id": e.short_id,
                "timestamp": e.timestamp.isoformat(),
                "ledger_type": e.ledger_type.name,
                "description": e.description,
                "link": getattr(e, "link", ""),
            }
        )

    return JSONResponse({"count": len(entries), "entries": entries})


@router.get("/", response_class=HTMLResponse)
async def ledger_entries_page(
    request: Request,
    account_string: Optional[str] = None,
    short_id: Optional[str] = None,
    group_id: Optional[str] = None,
    as_of_date_str: Optional[str] = None,
    age_hours: Optional[int] = 0,
):
    """Render ledger entries page. Supports simple GET search parameters."""
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    # Get accounts for selector (reuse list_all_accounts if available lazily)
    from v4vapp_backend_v2.accounting.account_balances import list_all_accounts

    try:
        all_accounts = await list_all_accounts()
    except Exception:
        all_accounts = []

    # Group accounts by type similar to accounts page
    accounts_by_type: dict[str, list[LedgerAccount]] = {}
    for acc in all_accounts:
        account_type = acc.account_type.value
        if account_type not in accounts_by_type:
            accounts_by_type[account_type] = []
        accounts_by_type[account_type].append(acc)

    # Sort each group
    for account_type in accounts_by_type:
        accounts_by_type[account_type].sort(key=lambda x: (x.name, x.sub))

    account = None
    if account_string:
        try:
            account = LedgerAccount.from_string(account_string)
        except Exception:
            account = None

    as_of_date = datetime.now(tz=timezone.utc)
    if as_of_date_str:
        try:
            as_of_date = datetime.fromisoformat(as_of_date_str.replace("Z", "+00:00"))
        except Exception:
            pass

    ledger_entries = []
    try:
        ledger_entries = await get_ledger_entries(
            as_of_date=as_of_date, filter_by_account=account, group_id=group_id, short_id=short_id
        )
    except Exception:
        # swallow DB errors and render page
        ledger_entries = []

    nav_items = nav_manager.get_navigation_items("/admin/ledger-entries")

    return templates.TemplateResponse(
        "ledger_entries/entries.html",
        {
            "request": request,
            "title": "Ledger Entries",
            "nav_items": nav_items,
            "accounts_by_type": accounts_by_type,
            "entries": ledger_entries,
            "short_id": short_id or "",
            "group_id": group_id or "",
            "account_string": account_string or "",
            "as_of_date": as_of_date,
            "age_hours": age_hours or 0,
            "pending_transactions": await PendingTransaction.list_all_str(),
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Ledger Entries", "url": "/admin/ledger-entries"},
            ],
        },
    )
