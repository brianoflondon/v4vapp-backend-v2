"""Admin MCP tools.

Read tools return the same data as the admin pages. The two write tools change
live state and do nothing unless ``confirm`` is true. Ledger edits, reversals,
deposit replay, and fee updates stay on the web UI.
"""

import functools
import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import HTTPException
from pydantic import Field

from v4vapp_backend_v2 import __version__ as project_version
from v4vapp_backend_v2.accounting.account_balances import (
    account_balance_printout,
    account_balance_printout_grouped_by_customer,
    list_all_accounts,
)
from v4vapp_backend_v2.accounting.balance_sheet import (
    balance_sheet_all_currencies_printout,
    generate_balance_sheet_mongodb,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import LedgerAccount
from v4vapp_backend_v2.accounting.ledger_cache import invalidate_all_ledger_cache
from v4vapp_backend_v2.accounting.ledger_checkpoints import (
    PeriodType,
    last_completed_period_end,
)
from v4vapp_backend_v2.accounting.ledger_type_class import list_all_ledger_type_details
from v4vapp_backend_v2.accounting.profit_and_loss import (
    generate_profit_and_loss_report,
    profit_and_loss_printout,
)
from v4vapp_backend_v2.accounting.trading_pnl import (
    generate_trading_pnl_report,
    trading_pnl_printout,
)
from v4vapp_backend_v2.admin import __version__ as admin_version
from v4vapp_backend_v2.admin.mcp_server.runtime import (
    cap_text,
    confirmation_required,
    ensure_ready,
    to_jsonable,
    unwrap,
)
from v4vapp_backend_v2.admin.navigation import NavigationManager
from v4vapp_backend_v2.admin.routers.dashboard_api import (
    dashboard_exchange_balances,
    dashboard_financial_summary,
    dashboard_hive_balances,
    dashboard_lnd_info,
    dashboard_magi_balances,
    dashboard_sanity,
)
from v4vapp_backend_v2.admin.routers.ledger_editor import load_entry
from v4vapp_backend_v2.admin.routers.ledger_entries import ledger_entries_data
from v4vapp_backend_v2.admin.routers.quote_sources import (
    get_quote_sources_api,
    update_quote_sources_api,
)
from v4vapp_backend_v2.admin.routers.replay_deposit import inspect_deposit, search_deposits
from v4vapp_backend_v2.admin.routers.users import users_data_api
from v4vapp_backend_v2.admin.routers.v4vconfig import get_v4v_config as load_v4v_config
from v4vapp_backend_v2.admin.routers.v4vconfig import get_v4vconfig_api
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_to_float_or_int

ToolFn = Callable[..., Awaitable[dict[str, Any]]]

NOT_EXPOSED = [
    "Creating, editing, or reversing ledger entries",
    "Replaying Hive deposits or retrying Lightning payments",
    "Changing V4V fees or limits",
    "Building ledger checkpoints or archiving hold entries",
]


def mcp_tool(fn: ToolFn) -> ToolFn:
    """Catch failures and return them as JSON instead of dropping the session."""
    signature = inspect.signature(fn, eval_str=True)

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> dict[str, Any]:
        try:
            await ensure_ready()
            return to_jsonable(await fn(*args, **kwargs))
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
            return {"success": False, "error": detail, "status_code": exc.status_code}
        except Exception as exc:
            logger.exception(
                "Admin MCP tool %s failed",
                fn.__name__,
                extra={"notification": False},
            )
            return {"success": False, "error": f"{type(exc).__name__}: {exc}"}

    wrapper.__signature__ = signature  # type: ignore[attr-defined]
    return wrapper


def summarize_account(details: Any) -> dict[str, Any]:
    """Totals for one ledger account, without the per-row balance lines."""
    last = getattr(details, "last_transaction_date", None)
    nets = getattr(details, "balances_net", {}) or {}
    summary: dict[str, Any] = {
        "account": str(details),
        "hive": getattr(details, "hive", None),
        "hbd": getattr(details, "hbd", None),
        "usd": getattr(details, "usd", None),
        "sats": getattr(details, "sats", None),
        "msats": getattr(details, "msats", None),
        "dash": getattr(details, "dash", None),
        "duffs": getattr(details, "duffs", None),
        "magi_btc_sats": getattr(details, "magi_btc_sats", None),
        "in_progress_msats": getattr(details, "in_progress_msats", None),
        "net": {str(currency): amount for currency, amount in nets.items()},
        "last_transaction_date": last.isoformat() if hasattr(last, "isoformat") else None,
        "has_transactions": bool(getattr(details, "has_transactions", False)),
    }
    conv = getattr(details, "conv_total", None)
    if conv is not None and hasattr(conv, "model_dump"):
        summary["conv_total"] = conv.model_dump(mode="json")
    return summary


def _user_summary(users: list[dict[str, Any]]) -> dict[str, Any]:
    positive = 0
    for user in users:
        balance = user.get("balance_sats")
        if isinstance(balance, (int, float)) and balance > 0:
            positive += balance
    return {
        "total_users": len(users),
        "active_users": sum(1 for user in users if user.get("has_transactions")),
        "total_positive_sats": positive,
        "error_count": sum(1 for user in users if user.get("error")),
    }


async def _report(text: str, data: Any, include_data: bool) -> dict[str, Any]:
    payload: dict[str, Any] = {"text": cap_text(text)}
    if include_data:
        payload["data"] = data
    return payload


@mcp_tool
async def admin_overview() -> dict[str, Any]:
    """Versions, server identity, admin sections, and which writes this server allows."""
    config = InternalConfig()
    nav = NavigationManager()
    return {
        "admin_version": admin_version,
        "project_version": project_version,
        "config_file": config.config_filename,
        "server_id": config.server_id,
        "node_name": config.node_name,
        "local_machine_name": config.local_machine_name,
        "sections": [
            {"name": item.name, "description": item.description}
            for item in nav.get_navigation_items()
        ],
        "writes": {
            "flush_ledger_cache": "Drops cached ledger balances. Requires confirm=true.",
            "set_quote_sources": "Saves quote-service switches to Hive. Requires confirm=true.",
            "set_gateway": (
                "Opens or closes Hive → Lightning and Lightning → Hive. Requires confirm=true."
            ),
        },
        "not_exposed": NOT_EXPOSED,
    }


@mcp_tool
async def admin_sanity() -> dict[str, Any]:
    """Sanity checks and pending Hive transactions from the admin dashboard."""
    return unwrap(await dashboard_sanity())


@mcp_tool
async def hive_balances() -> dict[str, Any]:
    """Hive and HBD balances for the admin highlight accounts."""
    return unwrap(await dashboard_hive_balances())


@mcp_tool
async def magi_balances() -> dict[str, Any]:
    """Magi sats balances for the admin highlight accounts."""
    return unwrap(await dashboard_magi_balances())


@mcp_tool
async def lightning_node() -> dict[str, Any]:
    """Lightning channel balance and the matching treasury and external ledger totals."""
    return unwrap(await dashboard_lnd_info())


@mcp_tool
async def exchange_balances() -> dict[str, Any]:
    """Binance wallet balances and the Treasury Dash ledger balance."""
    return unwrap(await dashboard_exchange_balances())


@mcp_tool
async def financial_summary() -> dict[str, Any]:
    """Net income and trading P&L in USD from the admin dashboard."""
    return unwrap(await dashboard_financial_summary())


@mcp_tool
async def list_ledger_accounts() -> dict[str, Any]:
    """Ledger accounts grouped by type.

    Each string is an account label. Pass that label to account_balance.
    """
    accounts = await list_all_accounts()
    grouped: dict[str, list[str]] = {}
    for account in accounts:
        grouped.setdefault(account.account_type.value, []).append(str(account))
    for labels in grouped.values():
        labels.sort()
    return {
        "count": sum(len(labels) for labels in grouped.values()),
        "accounts": grouped,
    }


@mcp_tool
async def account_balance(
    account: Annotated[
        str,
        Field(
            description=(
                "Account label from list_ledger_accounts, for example "
                "'Customer Deposits Hive (Asset) - Sub: v4vapp'."
            )
        ),
    ],
    as_of: Annotated[
        str,
        Field(description="ISO datetime. Empty means now."),
    ] = "",
    period: Annotated[
        str,
        Field(description="all, daily, weekly, or monthly."),
    ] = "all",
    line_items: Annotated[
        bool,
        Field(description="Include the transaction printout. Off keeps the result to totals."),
    ] = False,
    user_memos: Annotated[
        bool,
        Field(description="Include user memos. Ignored unless line_items is true."),
    ] = False,
    group_by_customer: Annotated[
        bool,
        Field(description="Group the printout by customer. Used with line_items."),
    ] = False,
) -> dict[str, Any]:
    """Balance totals for one ledger account, with an optional transaction printout."""
    try:
        parsed = LedgerAccount.from_string(account)
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    as_of_date = None
    if as_of.strip():
        try:
            as_of_date = datetime.fromisoformat(as_of.strip())
        except ValueError as exc:
            return {"success": False, "error": f"Could not parse as_of: {exc}"}

    age: timedelta | None = None
    period_start: datetime | None = None
    warning = None
    if period and period != "all":
        # The admin page creates a checkpoint for the period. This read does not.
        try:
            now = datetime.now(tz=UTC)
            period_start = last_completed_period_end(PeriodType(period), now)
            age = now - period_start
        except Exception as exc:
            warning = f"Period {period} was ignored: {exc}"

    show_lines = line_items
    show_memos = user_memos and show_lines
    if group_by_customer:
        printout, details = await account_balance_printout_grouped_by_customer(
            account=parsed,
            line_items=show_lines,
            user_memos=show_memos,
            as_of_date=as_of_date,
            age=age,
            period_start=period_start,
        )
    else:
        printout, details = await account_balance_printout(
            account=parsed,
            line_items=show_lines,
            user_memos=show_memos,
            as_of_date=as_of_date,
            age=age,
            period_start=period_start,
        )

    payload: dict[str, Any] = {
        "success": True,
        "account": str(parsed),
        "period": period or "all",
        "summary": summarize_account(details) if details is not None else None,
    }
    if warning:
        payload["warning"] = warning
    if show_lines and printout:
        payload["printout"] = cap_text(printout)
    return payload


@mcp_tool
async def list_users(
    active_only: Annotated[
        bool,
        Field(description="Skip VSC Liability accounts with fewer than two ledger rows."),
    ] = True,
    name_contains: Annotated[
        str,
        Field(description="Case-insensitive filter on the Hive account name."),
    ] = "",
    limit: Annotated[
        int,
        Field(description="Maximum users to return. The summary still covers every match."),
    ] = 50,
) -> dict[str, Any]:
    """VSC Liability users with sats balances, fees, and Lightning limit headroom."""
    payload = await users_data_api(active_only=active_only)
    users = list(payload.get("users_data") or [])
    needle = name_contains.strip().lower()
    if needle:
        users = [user for user in users if needle in str(user.get("sub", "")).lower()]
    capped = max(0, min(int(limit), 200))
    return {
        "summary": _user_summary(users),
        "unfiltered_summary": payload.get("summary"),
        "matched": len(users),
        "returned": min(capped, len(users)),
        "truncated": len(users) > capped,
        "as_of": payload.get("now"),
        "users": users[:capped],
    }


@mcp_tool
async def search_ledger_entries(
    account: Annotated[
        str,
        Field(description="Account label. Empty searches every account."),
    ] = "",
    sub: Annotated[str, Field(description="Sub-account contains this text.")] = "",
    short_id: Annotated[str, Field(description="Ledger short id.")] = "",
    group_id: Annotated[str, Field(description="Ledger group id.")] = "",
    op_type: Annotated[str, Field(description="Hive operation type, such as transfer.")] = "",
    ledger_type: Annotated[
        str,
        Field(description="Ledger type value or enum name from list_ledger_types."),
    ] = "",
    search: Annotated[
        str,
        Field(description="Matches short id, user memo, or description."),
    ] = "",
    from_date: Annotated[str, Field(description="ISO start datetime.")] = "",
    to_date: Annotated[str, Field(description="ISO end datetime. Empty means now.")] = "",
    age_hours: Annotated[
        int, Field(description="Look back this many hours. 0 uses the dates.")
    ] = 0,
    limit: Annotated[int, Field(description="Page size. Capped at 50.")] = 10,
    offset: Annotated[int, Field(description="Rows to skip.")] = 0,
) -> dict[str, Any]:
    """Search ledger entries. Returns one page plus totals for the whole match."""
    page_size = max(1, min(int(limit), 50))
    skip = max(0, int(offset))
    result = await ledger_entries_data(
        account_string=account or None,
        sub_filter=sub or None,
        short_id=short_id or None,
        group_id=group_id or None,
        op_type=op_type or None,
        ledger_type=ledger_type or None,
        general_search=search or None,
        from_date_str=from_date or None,
        to_date_str=to_date or None,
        age_hours=age_hours,
        limit=page_size,
        offset=skip,
    )
    payload = unwrap(result)
    if isinstance(payload, dict):
        payload["limit"] = page_size
        payload["offset"] = skip
    return payload


@mcp_tool
async def get_ledger_entry(
    group_id: Annotated[str, Field(description="Ledger group id.")],
) -> dict[str, Any]:
    """Load one ledger entry by group id."""
    return unwrap(await load_entry(group_id))


@mcp_tool
async def list_ledger_types() -> dict[str, Any]:
    """Ledger type names, stored values, and labels used by search_ledger_entries."""
    details = list_all_ledger_type_details()
    return {
        "ledger_types": [
            {"name": item.name, "value": item.value, "label": item.label} for item in details
        ]
    }


@mcp_tool
async def balance_sheet(
    include_data: Annotated[
        bool,
        Field(description="Also return the raw balance-sheet object."),
    ] = False,
) -> dict[str, Any]:
    """Balance sheet printout, the same report as the admin financial reports page."""
    sheet = convert_decimals_to_float_or_int(await generate_balance_sheet_mongodb())
    text = balance_sheet_all_currencies_printout(sheet)
    return await _report(text, sheet, include_data)


@mcp_tool
async def profit_and_loss(
    include_data: Annotated[
        bool,
        Field(description="Also return the raw profit-and-loss object."),
    ] = False,
) -> dict[str, Any]:
    """Profit and loss printout."""
    report = convert_decimals_to_float_or_int(await generate_profit_and_loss_report())
    text = await profit_and_loss_printout(pl_report=report)
    return await _report(text, report, include_data)


@mcp_tool
async def trading_pnl(
    sub: Annotated[
        str,
        Field(description="Exchange Holdings sub-account. Empty includes every sub."),
    ] = "",
    include_data: Annotated[
        bool,
        Field(description="Also return the raw trading P&L object."),
    ] = False,
) -> dict[str, Any]:
    """Trading P&L for Exchange Holdings."""
    subs = [sub] if sub.strip() else None
    report = convert_decimals_to_float_or_int(await generate_trading_pnl_report(subs=subs))
    text = trading_pnl_printout(report)
    return await _report(text, report, include_data)


@mcp_tool
async def get_v4v_config() -> dict[str, Any]:
    """Current V4V fee, limit, and gateway configuration stored on Hive."""
    return await get_v4vconfig_api()


@mcp_tool
async def get_quote_sources() -> dict[str, Any]:
    """Quote services saved on Hive and the set the next price fetch will call."""
    return await get_quote_sources_api()


@mcp_tool
async def search_hive_deposits(
    query: Annotated[
        str,
        Field(description="Short id, group id, Hive account, or memo text."),
    ] = "",
    limit: Annotated[int, Field(description="Maximum deposits. Capped at 50.")] = 25,
) -> dict[str, Any]:
    """Find customer Hive or HBD deposit ledger rows. Does not retry a payment."""
    return unwrap(await search_deposits(q=query, limit=max(1, min(int(limit), 50))))


@mcp_tool
async def inspect_hive_deposit(
    short_id: Annotated[
        str, Field(description="Deposit short id. Optional if group_id is set.")
    ] = "",
    group_id: Annotated[
        str, Field(description="Deposit group id. Optional if short_id is set.")
    ] = "",
) -> dict[str, Any]:
    """Read one deposit, its follow-on rows, invoice state, and Lightning health.

    This does not send a payment or clear a reply.
    """
    return unwrap(await inspect_deposit(short_id=short_id, group_id=group_id))


@mcp_tool
async def flush_ledger_cache(
    confirm: Annotated[
        bool,
        Field(description="Must be true. The cache is left unchanged otherwise."),
    ] = False,
) -> dict[str, Any]:
    """Drop cached ledger balances so the next read rebuilds them."""
    if not confirm:
        return confirmation_required("flush_ledger_cache")
    generation = await invalidate_all_ledger_cache()
    return {"success": True, "generation": generation}


@mcp_tool
async def set_quote_sources(
    confirm: Annotated[
        bool,
        Field(description="Must be true. Nothing is written to Hive otherwise."),
    ] = False,
    coin_gecko: Annotated[
        bool | None,
        Field(description="CoinGecko. Omit to leave the saved switch unchanged."),
    ] = None,
    binance: Annotated[
        bool | None,
        Field(description="Binance. Omit to leave the saved switch unchanged."),
    ] = None,
    coin_market_cap: Annotated[
        bool | None,
        Field(description="CoinMarketCap. Omit to leave the saved switch unchanged."),
    ] = None,
    hive_internal_market: Annotated[
        bool | None,
        Field(description="Hive internal market. Omit to leave the saved switch unchanged."),
    ] = None,
) -> dict[str, Any]:
    """Turn price quote services on or off. Omitted services keep their current value."""
    requested = {
        "CoinGecko": coin_gecko,
        "Binance": binance,
        "CoinMarketCap": coin_market_cap,
        "HiveInternalMarket": hive_internal_market,
    }
    changes = {name: enabled for name, enabled in requested.items() if enabled is not None}
    if not changes:
        return {
            "success": False,
            "error": (
                "Pass at least one of coin_gecko, binance, "
                "coin_market_cap, or hive_internal_market."
            ),
        }
    if not confirm:
        return confirmation_required("set_quote_sources", changes)
    return await update_quote_sources_api(changes)


def gateway_state(closed_get_lnd: bool, closed_get_hive: bool) -> dict[str, Any]:
    """Map the stored closed flags onto the two admin gateway switches.

    ``closed_get_lnd`` closes Hive → Lightning. ``closed_get_hive`` closes
    Lightning → Hive. An open switch is the one customers can pay through.
    """
    hive_open = not closed_get_lnd
    lightning_open = not closed_get_hive
    return {
        "hive_to_lightning": "open" if hive_open else "closed",
        "lightning_to_hive": "open" if lightning_open else "closed",
        "hive_to_lightning_open": hive_open,
        "lightning_to_hive_open": lightning_open,
    }


def apply_gateway(
    data: Any,
    hive_to_lightning: bool | None,
    lightning_to_hive: bool | None,
) -> Any:
    """Return a config copy with the requested switches applied.

    True opens that direction. False closes it. None leaves the stored flag.
    """
    updated = data.model_copy(deep=True)
    if hive_to_lightning is not None:
        updated.closed_get_lnd = not hive_to_lightning
    if lightning_to_hive is not None:
        updated.closed_get_hive = not lightning_to_hive
    return updated


@mcp_tool
async def get_gateway() -> dict[str, Any]:
    """Whether Hive → Lightning and Lightning → Hive are open or closed."""
    config = load_v4v_config()
    config.check()
    return {
        "success": True,
        "server_account": config.server_accname,
        **gateway_state(config.data.closed_get_lnd, config.data.closed_get_hive),
    }


@mcp_tool
async def set_gateway(
    confirm: Annotated[
        bool,
        Field(description="Must be true. Nothing is written to Hive otherwise."),
    ] = False,
    hive_to_lightning: Annotated[
        bool | None,
        Field(description="True opens Hive → Lightning. False closes it. Omit to leave it."),
    ] = None,
    lightning_to_hive: Annotated[
        bool | None,
        Field(description="True opens Lightning → Hive. False closes it. Omit to leave it."),
    ] = None,
) -> dict[str, Any]:
    """Open or close the two payment gateways. Pass both values to set them together."""
    if hive_to_lightning is None and lightning_to_hive is None:
        return {
            "success": False,
            "error": "Pass hive_to_lightning, lightning_to_hive, or both.",
        }
    requested = {
        key: value
        for key, value in {
            "hive_to_lightning": hive_to_lightning,
            "lightning_to_hive": lightning_to_hive,
        }.items()
        if value is not None
    }
    if not confirm:
        return confirmation_required("set_gateway", requested)

    config = load_v4v_config()
    config.check()
    config.data = apply_gateway(config.data, hive_to_lightning, lightning_to_hive)
    saved = await config.put()
    logger.info(
        "Gateway switches updated via admin MCP",
        extra={
            "notification": True,
            "server_account": config.server_accname,
            **gateway_state(config.data.closed_get_lnd, config.data.closed_get_hive),
        },
    )
    return {
        "success": bool(saved),
        "hive_saved": bool(saved),
        "server_account": config.server_accname,
        **gateway_state(config.data.closed_get_lnd, config.data.closed_get_hive),
    }


READ_TOOLS: list[ToolFn] = [
    admin_overview,
    admin_sanity,
    hive_balances,
    magi_balances,
    lightning_node,
    exchange_balances,
    financial_summary,
    list_ledger_accounts,
    account_balance,
    list_users,
    search_ledger_entries,
    get_ledger_entry,
    list_ledger_types,
    balance_sheet,
    profit_and_loss,
    trading_pnl,
    get_v4v_config,
    get_gateway,
    get_quote_sources,
    search_hive_deposits,
    inspect_hive_deposit,
]

WRITE_TOOLS: list[ToolFn] = [
    flush_ledger_cache,
    set_quote_sources,
    set_gateway,
]
