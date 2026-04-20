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
import uuid
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

    # Currency options – user-facing input currencies
    # sats input is automatically converted to msats on save
    currency_options = [
        Currency.SATS.value,
        Currency.MSATS.value,
        Currency.HIVE.value,
        Currency.HBD.value,
    ]

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
        # Merge changes into the loaded entry's data, then re-validate via
        # Pydantic to catch any invalid combinations.  save(upsert=True)
        # handles the DB write and all cache/checkpoint invalidation.
        entry_data = entry.model_dump()
        entry_data.update(changes)
        updated_entry = LedgerEntry.model_validate(entry_data)

        await updated_entry.save(upsert=True)

        logger.info(
            f"Ledger entry updated via editor: {group_id}",
            extra={"notification": True, "changes": list(changes.keys())},
        )
        return JSONResponse({"status": "ok", "modified": 1})
    except ValueError as e:
        return JSONResponse({"error": f"Validation error: {e}"}, status_code=400)
    except Exception as e:
        logger.exception("Error updating ledger entry: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# API: Create a new ledger entry
# ---------------------------------------------------------------------------


async def _validate_and_build_entry(
    payload: Dict[str, Any],
) -> tuple[LedgerEntry, None] | tuple[None, str]:
    """Validate a single entry payload and build a LedgerEntry.

    Returns (entry, None) on success or (None, error_message) on failure.
    Does NOT save the entry — caller is responsible for persisting.
    """
    try:
        debit_data = payload.get("debit", {})
        credit_data = payload.get("credit", {})

        if not debit_data.get("name"):
            return None, "Debit account name is required"
        if not credit_data.get("name"):
            return None, "Credit account name is required"

        debit_acc = _build_account(
            debit_data["account_type"], debit_data["name"], debit_data.get("sub", "")
        )
        credit_acc = _build_account(
            credit_data["account_type"], credit_data["name"], credit_data.get("sub", "")
        )

        amount = Decimal(str(payload.get("amount", 0)))
        if amount <= 0:
            return None, "Amount must be greater than 0"

        currency_str = payload.get("currency", "sats")
        try:
            currency = Currency(currency_str)
        except Exception:
            return None, f"Unknown currency: {currency_str}"

        # Normalise to storage units: only msats, hive, hbd are stored.
        # If the user entered sats, convert to msats (×1000).
        if currency == Currency.SATS:
            amount = amount * 1000
            currency = Currency.MSATS
        elif currency not in (Currency.HIVE, Currency.HBD, Currency.MSATS):
            return None, (
                f"Currency {currency.value} cannot be stored directly. Use sats, hive, or hbd."
            )

        # Compute conversion using the storage currency/amount
        conversion = CryptoConversion(conv_from=currency, value=amount)
        await conversion.get_quote()
        conv = conversion.conversion

        # Parse ledger type
        raw_ledger_type = payload.get("ledger_type")
        if raw_ledger_type is None:
            ledger_type = LedgerType.UNSET
        else:
            try:
                ledger_type = LedgerType(raw_ledger_type)
            except Exception:
                valid_ledger_types = [lt.value for lt in LedgerType]
                return None, (
                    f"Unknown ledger_type: {raw_ledger_type}. "
                    f"Valid values are: {valid_ledger_types}"
                )

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
        group_id = payload.get("group_id", "").strip()
        if group_id:
            group_id = f"{group_id}_manual_{ledger_type.value}"
        else:
            group_id = f"{uuid.uuid4().hex[:10]}_manual_{ledger_type.value}"

        short_id = group_id[:10]

        entry = LedgerEntry(
            group_id=group_id,
            short_id=short_id,
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
        return entry, None

    except Exception as e:
        return None, str(e)


@router.post("/api/create")
async def create_entry(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    entry, error = await _validate_and_build_entry(payload)
    if error:
        return JSONResponse({"error": error}, status_code=400)
    assert entry is not None

    try:
        await entry.save()
        logger.info(
            f"Ledger entry created via editor: {entry.group_id}",
            extra={"notification": True, **entry.log_extra},
        )
        return JSONResponse({"status": "ok", "group_id": entry.group_id})
    except Exception as e:
        logger.exception("Error creating ledger entry: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# API: Create multiple entries atomically (validate all before saving any)
# ---------------------------------------------------------------------------


@router.post("/api/create-batch")
async def create_batch(entries: List[Dict[str, Any]] = Body(...)) -> JSONResponse:
    """Validate all entries first. Only save if every entry passes validation."""
    if not entries:
        return JSONResponse({"error": "No entries provided"}, status_code=400)

    # Generate a shared auto group_id prefix for entries that don't supply one
    shared_auto_prefix = uuid.uuid4().hex[:10]

    # Phase 1: validate and build all entries
    built: List[LedgerEntry] = []
    errors: List[str] = []

    for i, payload in enumerate(entries, start=1):
        # If no group_id supplied, inject the shared auto prefix
        if not payload.get("group_id", "").strip():
            payload = {**payload, "group_id": shared_auto_prefix}
        entry, error = await _validate_and_build_entry(payload)
        if error:
            errors.append(f"Entry #{i}: {error}")
        else:
            assert entry is not None
            built.append(entry)

    if errors:
        return JSONResponse(
            {"error": "Validation failed — no entries were saved", "details": errors},
            status_code=400,
        )

    # Phase 2: all valid — save them
    saved_ids: List[str] = []
    try:
        for entry in built:
            await entry.save()
            saved_ids.append(entry.group_id)
            logger.info(
                f"Ledger entry created via editor (batch): {entry.group_id}",
                extra={"notification": True, **entry.log_extra},
            )
    except Exception as e:
        logger.exception("Error saving batch entry: %s", e)
        return JSONResponse(
            {
                "error": f"Saved {len(saved_ids)} of {len(built)} entries before failure: {e}",
                "saved": saved_ids,
            },
            status_code=500,
        )

    return JSONResponse({"status": "ok", "group_ids": saved_ids})


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
