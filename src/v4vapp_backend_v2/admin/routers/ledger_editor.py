"""
Ledger Editor Router

Provides endpoints to load, edit, and create ledger entries.
Supports editing debit/credit accounts, description, ledger type,
reversed status, and extra_data.  Also allows creation of new entries
with on-the-fly conversion calculation.

Quick-action "presets" can be defined server-side so common multi-entry
adjustments (e.g. exchange rebalance with fee) are one-click operations.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AccountType,
    AssetAccount,
    EquityAccount,
    ExpenseAccount,
    LedgerAccountAny,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import (
    LedgerType,
    LedgerTypeIcon,
    LedgerTypeStr,
    list_all_ledger_type_details,
)
from v4vapp_backend_v2.accounting.sanity_checks import run_all_sanity_checks
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers.helper_functions import get_accounts_by_type_for_selector
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import get_exchange_adapter
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

router = APIRouter()

# Will be set by the main app
templates: Optional[Jinja2Templates] = None
nav_manager: Optional[NavigationManager] = None


def set_templates_and_nav(tmpl: Jinja2Templates, nav: NavigationManager):
    global templates, nav_manager
    templates = tmpl
    nav_manager = nav


# ---------------------------------------------------------------------------
# Quick-action presets (built dynamically from config)
# ---------------------------------------------------------------------------


def _get_exchange_sub() -> str:
    """Resolve the exchange sub-account name from config."""
    try:
        return get_exchange_adapter().exchange_name
    except Exception:
        return "binance_convert"  # safe fallback


def _get_node_name() -> str:
    """Resolve the Lightning node name from config."""
    try:
        return InternalConfig().node_name
    except Exception:
        return "voltage"  # safe fallback


def _build_editor_presets() -> List[Dict[str, Any]]:
    """Build presets using the config-driven exchange name."""
    exchange_sub = _get_exchange_sub()
    node_name = _get_node_name()
    return [
        {
            "id": "exchange_to_lightning",
            "label": f"Exchange → Lightning ({exchange_sub}→{node_name})",
            "icon": "⚡",
            "description": (
                f"Move sats from Exchange Holdings ({exchange_sub}) "
                f"to External Lightning Payments ({node_name}). "
                f"Records withdrawal fee paid to {exchange_sub}."
            ),
            "entries": [
                {
                    "ledger_type": LedgerType.EXCHANGE_TO_NODE.value,
                    "description": f"Transfer sats from {exchange_sub} to {node_name} node",
                    "debit_account_type": "Asset",
                    "debit_name": "External Lightning Payments",
                    "debit_sub": node_name,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": node_name,
                },
                {
                    "ledger_type": LedgerType.EXCHANGE_FEES.value,
                    "description": "Exchange Withdrawal fee paid",
                    "debit_account_type": "Expense",
                    "debit_name": "Withdrawal Fees Paid",
                    "debit_sub": exchange_sub,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": exchange_sub,
                },
            ],
        },
        {
            "id": "exchange_fee",
            "label": "Exchange Withdrawal Fee",
            "icon": "💸",
            "description": (f"Record a fee charged by {exchange_sub} for a withdrawal."),
            "entries": [
                {
                    "ledger_type": LedgerType.EXCHANGE_FEES.value,
                    "description": "Exchange Withdrawal fee paid",
                    "debit_account_type": "Expense",
                    "debit_name": "Withdrawal Fees Paid",
                    "debit_sub": exchange_sub,
                    "credit_account_type": "Asset",
                    "credit_name": "Exchange Holdings",
                    "credit_sub": exchange_sub,
                    "currency": "sats",
                    "cust_id": exchange_sub,
                },
            ],
        },
    ]


def _account_class_for_type(account_type: str) -> type[LedgerAccountAny]:
    try:
        normalized_account_type = AccountType(account_type)
    except ValueError as exc:
        raise ValueError(f"Unknown account type: {account_type}") from exc

    mapping: dict[AccountType, type[LedgerAccountAny]] = {
        AccountType.ASSET: AssetAccount,
        AccountType.LIABILITY: LiabilityAccount,
        AccountType.EQUITY: EquityAccount,
        AccountType.REVENUE: RevenueAccount,
        AccountType.EXPENSE: ExpenseAccount,
    }
    return mapping[normalized_account_type]


def _build_account(account_type: str, name: str, sub: str = "") -> LedgerAccountAny:
    cls = _account_class_for_type(account_type)
    return cls(name=name, sub=sub)


# ---------------------------------------------------------------------------
# Helper: all allowed account names grouped by type
# ---------------------------------------------------------------------------


def _all_allowed_account_names() -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    for cls, atype in [
        (AssetAccount, AccountType.ASSET),
        (LiabilityAccount, AccountType.LIABILITY),
        (EquityAccount, AccountType.EQUITY),
        (RevenueAccount, AccountType.REVENUE),
        (ExpenseAccount, AccountType.EXPENSE),
    ]:
        names = sorted(cls.allowed_names())
        result[atype.value] = names
    return result


# ---------------------------------------------------------------------------
# Page endpoint
# ---------------------------------------------------------------------------


@router.get("/", response_class=HTMLResponse)
async def ledger_editor_page(
    request: Request,
    group_id: Optional[str] = None,
):
    if not templates or not nav_manager:
        raise RuntimeError("Templates and navigation not initialized")

    sanity_task = asyncio.create_task(run_all_sanity_checks())

    # Fetch accounts and ledger types in parallel
    accounts_by_type = await get_accounts_by_type_for_selector()

    ledger_type_options = []
    try:
        details = list_all_ledger_type_details()
    except Exception:
        details = list(LedgerType)
    for lt in details:
        ledger = getattr(lt, "ledger_type", lt)
        ledger_type_options.append({
            "value": getattr(lt, "value", getattr(lt, "name", "")),
            "full": (
                getattr(lt, "capitalized", None)
                or getattr(ledger, "capitalized", None)
                or getattr(lt, "name", "").replace("_", " ").title()
            ),
            "label": LedgerTypeStr.get(getattr(lt, "ledger_type", lt)) or "",
            "icon": (
                getattr(lt, "icon", None) or LedgerTypeIcon.get(getattr(lt, "ledger_type", lt), "")
            ),
        })

    # Allowed account names keyed by type (for JS dropdowns)
    allowed_names = _all_allowed_account_names()

    # Currency options
    currency_options = [c.value for c in Currency]

    nav_items = nav_manager.get_navigation_items("/admin/ledger-editor")
    sanity_results = await sanity_task

    return templates.TemplateResponse(
        request,
        "ledger_editor/editor.html.jinja",
        {
            "request": request,
            "title": "Ledger Editor",
            "nav_items": nav_items,
            "accounts_by_type": accounts_by_type,
            "allowed_names": allowed_names,
            "ledger_type_options": ledger_type_options,
            "currency_options": currency_options,
            "presets": _build_editor_presets(),
            "load_group_id": group_id or "",
            "breadcrumbs": [
                {"name": "Admin", "url": "/admin"},
                {"name": "Ledger Editor", "url": "/admin/ledger-editor"},
            ],
            "sanity_results": sanity_results,
            "pending_transactions": await PendingTransaction.list_all_str(),
        },
    )


# ---------------------------------------------------------------------------
# API: Load an existing entry
# ---------------------------------------------------------------------------


@router.get("/api/load")
async def load_entry(group_id: str) -> JSONResponse:
    entry = await LedgerEntry.load(group_id)
    if not entry:
        return JSONResponse({"error": "Entry not found"}, status_code=404)

    def _acct(acc):
        if acc is None:
            return None
        return {
            "name": getattr(acc, "name", ""),
            "sub": getattr(acc, "sub", ""),
            "account_type": getattr(acc, "account_type", ""),
            "contra": getattr(acc, "contra", False),
        }

    def _conv(conv):
        if conv is None:
            return None
        try:
            return conv.model_dump(mode="json")
        except Exception:
            return {}

    return JSONResponse({
        "group_id": entry.group_id,
        "short_id": entry.short_id,
        "timestamp": entry.timestamp.isoformat() if entry.timestamp else None,
        "ledger_type": entry.ledger_type.value if entry.ledger_type else None,
        "description": entry.description,
        "user_memo": entry.user_memo or "",
        "cust_id": entry.cust_id or "",
        "cust_id_from": getattr(entry, "cust_id_from", ""),
        "cust_id_to": getattr(entry, "cust_id_to", ""),
        "debit": {
            **_acct(entry.debit),
            "amount": str(entry.debit_amount),
            "unit": entry.debit_unit.value if entry.debit_unit else "",
            "conv": _conv(entry.debit_conv),
        },
        "credit": {
            **_acct(entry.credit),
            "amount": str(entry.credit_amount),
            "unit": entry.credit_unit.value if entry.credit_unit else "",
            "conv": _conv(entry.credit_conv),
        },
        "reversed": entry.reversed.isoformat() if entry.reversed else None,
        "extra_data": entry.extra_data or [],
        "link": getattr(entry, "link", ""),
        "op_type": entry.op_type or "",
    })


# ---------------------------------------------------------------------------
# API: Compute conversion from amount + currency
# ---------------------------------------------------------------------------


@router.post("/api/compute-conversion")
async def compute_conversion(
    amount: float = Body(...),
    currency: str = Body(...),
) -> JSONResponse:
    try:
        cur = Currency(currency)
    except Exception:
        return JSONResponse({"error": f"Unknown currency: {currency}"}, status_code=400)

    try:
        conversion = CryptoConversion(conv_from=cur, value=Decimal(str(amount)))
        await conversion.get_quote()
        conv = conversion.conversion
        return JSONResponse(conv.model_dump(mode="json"))
    except Exception as e:
        logger.exception("Conversion computation failed: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# API: Update an existing entry (edit debit/credit accounts, description, etc.)
# ---------------------------------------------------------------------------


@router.post("/api/update")
async def update_entry(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    group_id = payload.get("group_id")
    if not group_id:
        return JSONResponse({"error": "group_id is required"}, status_code=400)

    entry = await LedgerEntry.load(group_id)
    if not entry:
        return JSONResponse({"error": "Entry not found"}, status_code=404)

    changes: Dict[str, Any] = {}

    # Editable fields
    if "description" in payload:
        changes["description"] = str(payload["description"])
    if "user_memo" in payload:
        changes["user_memo"] = str(payload["user_memo"])
    if "ledger_type" in payload:
        try:
            changes["ledger_type"] = LedgerType(payload["ledger_type"]).value
        except Exception:
            return JSONResponse(
                {"error": f"Invalid ledger_type: {payload['ledger_type']}"},
                status_code=400,
            )
    if "extra_data" in payload:
        changes["extra_data"] = payload["extra_data"]

    # reversed: accept ISO string, null, or "now"
    if "reversed" in payload:
        rev = payload["reversed"]
        if rev is None or rev == "":
            changes["reversed"] = None
        elif rev == "now":
            changes["reversed"] = datetime.now(tz=timezone.utc)
        else:
            try:
                parsed_reversed = datetime.fromisoformat(str(rev))
                if parsed_reversed.tzinfo is None:
                    parsed_reversed = parsed_reversed.replace(tzinfo=timezone.utc)
                else:
                    parsed_reversed = parsed_reversed.astimezone(timezone.utc)
                changes["reversed"] = parsed_reversed
            except Exception:
                return JSONResponse({"error": f"Invalid reversed value: {rev}"}, status_code=400)

    # Debit account
    if "debit" in payload:
        d = payload["debit"]
        try:
            acc = _build_account(d["account_type"], d["name"], d.get("sub", ""))
            changes["debit"] = acc.model_dump()
        except Exception as e:
            return JSONResponse({"error": f"Invalid debit account: {e}"}, status_code=400)

    # Credit account
    if "credit" in payload:
        c = payload["credit"]
        try:
            acc = _build_account(c["account_type"], c["name"], c.get("sub", ""))
            changes["credit"] = acc.model_dump()
        except Exception as e:
            return JSONResponse({"error": f"Invalid credit account: {e}"}, status_code=400)

    if not changes:
        return JSONResponse({"error": "No changes provided"}, status_code=400)

    try:
        from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb

        result = await LedgerEntry.collection().update_one(
            {"group_id": group_id},
            {"$set": convert_decimals_for_mongodb(changes)},
        )
        if result.modified_count == 0 and result.matched_count == 0:
            return JSONResponse({"error": "Entry not found in DB"}, status_code=404)

        # Invalidate cache for affected accounts
        from v4vapp_backend_v2.accounting.ledger_cache import invalidate_ledger_cache

        await invalidate_ledger_cache(
            debit_name=entry.debit.name,
            debit_sub=entry.debit.sub,
            credit_name=entry.credit.name,
            credit_sub=entry.credit.sub,
        )
        # Also invalidate for new accounts if changed
        if "debit" in payload:
            await invalidate_ledger_cache(
                debit_name=payload["debit"]["name"],
                debit_sub=payload["debit"].get("sub", ""),
                credit_name=entry.credit.name,
                credit_sub=entry.credit.sub,
            )
        if "credit" in payload:
            await invalidate_ledger_cache(
                debit_name=entry.debit.name,
                debit_sub=entry.debit.sub,
                credit_name=payload["credit"]["name"],
                credit_sub=payload["credit"].get("sub", ""),
            )

        logger.info(
            f"Ledger entry updated via editor: {group_id}",
            extra={"notification": True, "changes": changes},
        )
        return JSONResponse({"status": "ok", "modified": result.modified_count})
    except Exception as e:
        logger.exception("Error updating ledger entry: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# API: Create a new ledger entry
# ---------------------------------------------------------------------------


@router.post("/api/create")
async def create_entry(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        # Build accounts
        debit_data = payload.get("debit", {})
        credit_data = payload.get("credit", {})
        debit_acc = _build_account(
            debit_data["account_type"], debit_data["name"], debit_data.get("sub", "")
        )
        credit_acc = _build_account(
            credit_data["account_type"], credit_data["name"], credit_data.get("sub", "")
        )

        amount = Decimal(str(payload.get("amount", 0)))
        currency_str = payload.get("currency", "sats")
        try:
            currency = Currency(currency_str)
        except Exception:
            return JSONResponse({"error": f"Unknown currency: {currency_str}"}, status_code=400)

        # Compute conversion
        conversion = CryptoConversion(conv_from=currency, value=amount)
        await conversion.get_quote()
        conv = conversion.conversion

        # Parse ledger type
        try:
            ledger_type = LedgerType(payload.get("ledger_type", LedgerType.UNSET.value))
        except Exception:
            ledger_type = LedgerType.UNSET

        # Parse timestamp
        ts = payload.get("timestamp")
        if ts:
            try:
                timestamp = datetime.fromisoformat(str(ts))
                if timestamp.tzinfo is None or timestamp.utcoffset() is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            except Exception:
                timestamp = datetime.now(tz=timezone.utc)
        else:
            timestamp = datetime.now(tz=timezone.utc)

        # Build the group_id
        group_id = payload.get("group_id", "")
        if not group_id:
            # Generate a unique group_id for manual entries
            group_id = f"manual_{datetime.now(tz=timezone.utc).strftime('%Y%m%d_%H%M%S')}_{ledger_type.value}"

        entry = LedgerEntry(
            group_id=group_id,
            short_id=payload.get("short_id", group_id[:16]),
            ledger_type=ledger_type,
            timestamp=timestamp,
            description=payload.get("description", "Manual entry via admin editor"),
            user_memo=payload.get("user_memo", ""),
            cust_id=payload.get("cust_id", ""),
            debit_amount=amount,
            debit_unit=currency,
            debit_conv=conv,
            credit_amount=amount,
            credit_unit=currency,
            credit_conv=conv,
            debit=debit_acc,
            credit=credit_acc,
            extra_data=payload.get("extra_data", [{"source": "admin_editor"}]),
            link=payload.get("link", ""),
        )

        result = await entry.save()
        logger.info(
            f"Ledger entry created via editor: {entry.group_id}",
            extra={"notification": True, **entry.log_extra},
        )
        return JSONResponse({"status": "ok", "group_id": entry.group_id})

    except Exception as e:
        logger.exception("Error creating ledger entry: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# API: Get presets
# ---------------------------------------------------------------------------


@router.get("/api/presets")
async def get_presets() -> JSONResponse:
    presets = _build_editor_presets()
    return JSONResponse([
        {
            "id": p["id"],
            "label": p["label"],
            "icon": p["icon"],
            "description": p["description"],
            "entries": p["entries"],
        }
        for p in presets
    ])
