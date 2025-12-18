"""
Ledger Entries Router

Provides a simple page and data endpoint to browse ledger entries.
"""

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.account_balances import list_all_ledger_types
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_entries import get_ledger_entries
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.pipelines.simple_pipelines import (
    filter_by_account_as_of_date_query,
)
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.config.setup import async_time_stats_decorator
from v4vapp_backend_v2.helpers.general_purpose_funcs import parse_dt_with_tz
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
@async_time_stats_decorator()
async def ledger_entries_data(
    account_string: Optional[str] = None,
    sub_filter: Optional[str] = None,
    short_id: Optional[str] = None,
    group_id: Optional[str] = None,
    from_date_str: Optional[str] = None,
    to_date_str: Optional[str] = None,
    ledger_type: Optional[str] = None,
    general_search: Optional[str] = None,
    age_hours: int = 0,
    limit: int = 10,
    offset: int = 0,
):
    """Return ledger entries in JSON form for AJAX or API use. Supports pagination via limit/offset."""
    # Determine date range using from/to inputs. We use the existing helper which
    # accepts an as_of_date (end) and an age (timedelta) to represent the range.
    # Default end date to current UTC time
    to_date = datetime.now(tz=timezone.utc)
    from_date = None

    # Helper: parse incoming ISO strings. If they include an explicit timezone (Z or +HH:MM),
    # use fromisoformat directly (after normalizing Z to +00:00). If they are naive (no TZ),
    # interpret them as local datetimes and attach the local timezone info so all datetimes
    # are timezone-aware.

    if to_date_str:
        parsed = parse_dt_with_tz(to_date_str)
        if parsed:
            to_date = parsed
    if from_date_str:
        parsed = parse_dt_with_tz(from_date_str)
        if parsed:
            from_date = parsed

    account = None
    if account_string:
        try:
            account = LedgerAccount.from_string(account_string)
        except Exception:
            account = None

    # Build age from from_date/to_date if both provided (or from_date to now)
    age = None
    try:
        age_val = int(age_hours or 0)
    except Exception:
        age_val = 0
    if age_val and age_val > 0:
        age = timedelta(hours=age_val)
    elif from_date:
        # If from_date is provided, compute age as difference
        age = to_date - from_date if to_date and from_date else None

    # Convert ledger_type param to LedgerType enum when present
    ledger_types = None
    if ledger_type:
        try:
            # Try by value (e.g., "deposit_h")
            ledger_types = [LedgerType(ledger_type)]
        except Exception:
            try:
                # Try by enum name
                ledger_types = [LedgerType[ledger_type]]
            except Exception:
                ledger_types = None

    query = filter_by_account_as_of_date_query(
        account=account,
        cust_id=None,
        as_of_date=to_date,
        ledger_types=ledger_types,
        group_id=group_id,
        short_id=short_id,
        sub_account=(None if account else sub_filter),
        age=age,
    )

    # If a general_search term was provided, add an ANDed $or regex condition
    if general_search:
        search_or = [
            {"short_id": {"$regex": general_search}},
            {"user_memo": {"$regex": general_search}},
            {"description": {"$regex": general_search}},
        ]
        query = {"$and": [query, {"$or": search_or}]}

    # Count total matching documents
    total = await LedgerEntry.collection().count_documents(query)

    # Fetch paginated documents
    cursor = (
        LedgerEntry.collection()
        .find(filter=query)
        .sort([("timestamp", -1)])
        .skip(offset)
        .limit(limit)
    )

    ledger_entries = []
    async for e in cursor:
        try:
            ledger_entries.append(LedgerEntry.model_validate(e))
        except Exception:
            continue

    # Return a structured JSON representation (including nested debit/credit and conv)
    def conv_to_dict(conv):
        if conv is None:
            return None
        # Try model_dump when available (Pydantic models)
        try:
            return conv.model_dump(mode="json")
        except Exception:
            # Fallback to attribute extraction
            keys = [
                "hive",
                "hbd",
                "usd",
                "sats",
                "sats_rounded",
                "btc",
                "sats_hive",
                "sats_hbd",
                "fetch_date",
                "source",
            ]
            d = {}
            for k in keys:
                if hasattr(conv, k):
                    v = getattr(conv, k)
                    if hasattr(v, "isoformat"):
                        try:
                            d[k] = v.isoformat()
                        except Exception:
                            d[k] = str(v)
                    else:
                        d[k] = v
            return d

    def acct_to_dict(acc):
        if acc is None:
            return None
        return {
            "name": getattr(acc, "name", ""),
            "sub": getattr(acc, "sub", ""),
            "account_type": getattr(acc, "account_type", ""),
            "contra": getattr(acc, "contra", False),
        }

    entries = []
    for e in ledger_entries:
        debit = acct_to_dict(getattr(e, "debit", None))
        credit = acct_to_dict(getattr(e, "credit", None))
        entries.append(
            {
                "group_id": e.group_id,
                "short_id": e.short_id,
                "timestamp": e.timestamp.isoformat() if getattr(e, "timestamp", None) else None,
                "ledger_type": e.ledger_type.name if getattr(e, "ledger_type", None) else None,
                "ledger_type_str": getattr(e, "ledger_type", None).printout
                if getattr(e, "ledger_type", None)
                else None,
                "description": e.description,
                "link": getattr(e, "link", ""),
                "cust_id": getattr(e, "cust_id", ""),
                "debit": {
                    **debit,
                    "amount": str(getattr(e, "debit_amount", None)),
                    "unit": getattr(e, "debit_unit", "").value
                    if getattr(e, "debit_unit", None)
                    else "",
                    "conv": conv_to_dict(getattr(e, "debit_conv", None)),
                },
                "credit": {
                    **credit,
                    "amount": str(getattr(e, "credit_amount", None)),
                    "unit": getattr(e, "credit_unit", "").value
                    if getattr(e, "credit_unit", None)
                    else "",
                    "conv": conv_to_dict(getattr(e, "credit_conv", None)),
                },
                "conversion": {
                    "debit": conv_to_dict(getattr(e, "debit_conv", None)),
                    "credit": conv_to_dict(getattr(e, "credit_conv", None)),
                },
                # Provide the textual journal for reference (not used for primary rendering)
                "user_memo": getattr(e, "user_memo", ""),
                "journal": e.print_journal_entry() if hasattr(e, "print_journal_entry") else None,
                "op_type": getattr(e, "op_type", ""),
            }
        )

    return JSONResponse({"count": total, "entries": entries})


@router.get("/", response_class=HTMLResponse)
async def ledger_entries_page(
    request: Request,
    account_string: Optional[str] = None,
    sub_filter: Optional[str] = None,
    short_id: Optional[str] = None,
    group_id: Optional[str] = None,
    from_date_str: Optional[str] = None,
    to_date_str: Optional[str] = None,
    ledger_type: Optional[str] = None,
    general_search: Optional[str] = None,
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

    try:
        ledger_type_options = await list_all_ledger_types()
    except Exception:
        ledger_type_options = list(LedgerType)

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
    # legacy as_of_date removed; page uses from_date_str/to_date_str via client-side API. No server-side cutoff applied here for initial render.
    ledger_entries = []
    try:
        ledger_entries = await get_ledger_entries(
            as_of_date=None,  # page renders server entries only for non-JS fallback; we rely on client data endpoint for full results
            filter_by_account=account,
            group_id=group_id,
            short_id=short_id,
            sub_account=(None if account else sub_filter),
            age_hours=age_hours,
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
            "sub_filter": sub_filter or "",
            "entries": ledger_entries,
            "short_id": short_id or "",
            "group_id": group_id or "",
            "account_string": account_string or "",
            "from_date_str": from_date_str or "",
            "to_date_str": to_date_str or "",
            "ledger_type": ledger_type or "",
            "general_search": general_search or "",
            "ledger_type_options": ledger_type_options,
            "age_hours": age_hours or 0,
            "pending_transactions": await PendingTransaction.list_all_str(),
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Ledger Entries", "url": "/admin/ledger-entries"},
            ],
        },
    )
