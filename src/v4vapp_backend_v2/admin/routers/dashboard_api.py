"""
Dashboard API Router

Provides JSON endpoints for progressive loading of admin dashboard sections.
Each endpoint returns data for a specific dashboard section, allowing the
browser to fetch them in parallel and render as they arrive.
"""

import asyncio
import json
from asyncio import TaskGroup
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.ledger_checkpoints import (
    PeriodType,
    latest_period_create_checkpoint,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.accounting.profit_and_loss import generate_profit_and_loss_report
from v4vapp_backend_v2.accounting.sanity_checks import (
    SanityCheckResult,
    SanityCheckResults,
    log_all_sanity_checks,
)
from v4vapp_backend_v2.accounting.trading_pnl import generate_trading_pnl_report
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    ExchangeConnectionError,
    format_base_asset,
    format_quote_asset,
    get_exchange_adapter,
)
from v4vapp_backend_v2.conversion.exchange_rebalance import PendingRebalance
from v4vapp_backend_v2.dash.amounts import DUFFS_PER_DASH
from v4vapp_backend_v2.dash.settings import dash_connection
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances_async, get_transfer_cust_id
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction
from v4vapp_backend_v2.magi.magi_balances import get_magi_btc_balance_by_account
from v4vapp_backend_v2.models.lnd_balance_models import NodeBalances
from v4vapp_backend_v2.process.process_dash import treasury_sub

router = APIRouter()


def _msat_to_sats_int(msat_val) -> int | None:
    try:
        return int(
            (Decimal(msat_val) / Decimal(1000)).quantize(Decimal(1), rounding=ROUND_HALF_UP)
        )
    except Exception:
        return None


def _fmt_sats(n) -> str:
    try:
        return f"{int(n):,}"
    except Exception:
        return "N/A"


@router.get("/hive-balances")
async def dashboard_hive_balances() -> JSONResponse:
    """Fetch Hive balances for all highlight_users."""
    highlight_users = InternalConfig().config.admin_config.highlight_users

    async def _safe_balance(acc: str):
        try:
            balances = await account_hive_balances_async(acc)
            return balances
        except Exception as e:
            logger.warning(
                f"Hive balance for {acc} failed: {e}",
                extra={"notification": False},
            )
            return {"error": str(e)}

    tasks = {}
    async with TaskGroup() as tg:
        for acc in highlight_users:
            tasks[acc] = tg.create_task(_safe_balance(acc))

    balances = {}
    for acc, task in tasks.items():
        try:
            result = task.result()
            if "error" in result:
                balances[acc] = result
            else:
                balances[acc] = {
                    "HIVE_fmt": result.get("HIVE_fmt", "0.000"),
                    "HBD_fmt": result.get("HBD_fmt", "0.000"),
                }
        except Exception as e:
            balances[acc] = {"error": str(e)}

    return JSONResponse({"balances": balances})


@router.get("/magi-balances")
async def dashboard_magi_balances() -> JSONResponse:
    """
    Fetch Magi Sats balances for all highlight_users.
    """

    async def _safe_magi_balance(acc: str) -> dict[str, str] | None:
        try:
            balance = await get_magi_btc_balance_by_account(acc)
            if balance.error:
                return {"error": balance.error}

            return {
                "Magi Sats": f"{int(balance.balance_sats):,}",
            }
        except Exception as e:
            logger.warning(
                f"Magi balance for {acc} failed: {e}",
                extra={"notification": False},
            )
            return {"error": str(e)}

    highlight_users = InternalConfig().config.admin_config.highlight_users
    tasks = {}
    async with TaskGroup() as tg:
        for acc in highlight_users:
            tasks[acc] = tg.create_task(_safe_magi_balance(acc))

    balances = {}
    for acc, task in tasks.items():
        try:
            balances[acc] = task.result()
        except Exception as e:
            logger.warning(
                f"Magi balance task for {acc} failed: {e}",
                extra={"notification": False},
            )
            balances[acc] = {"error": str(e)}

    return JSONResponse({"balances": balances})


@router.get("/lnd-info")
async def dashboard_lnd_info() -> JSONResponse:
    """Fetch LND node balances and ledger details."""
    node_name = InternalConfig().node_name
    nb = NodeBalances(node=node_name)

    # @async_time_decorator
    async def _safe_fetch(nb_obj):
        try:
            await nb_obj.fetch_balances()
        except Exception as e:
            logger.warning(f"LND fetch failed: {e}", extra={"notification": False})
        return nb_obj

    # @async_time_decorator
    async def _safe_ledger(asset: AssetAccount):
        try:
            await latest_period_create_checkpoint(account=asset, period_type=PeriodType.DAILY)
            return await one_account_balance(account=asset, as_of_date=datetime.now(UTC))
        except Exception as e:
            logger.warning(f"Ledger lookup failed: {e}", extra={"notification": False})
            return None

    async with TaskGroup() as tg:
        fetch_task = tg.create_task(_safe_fetch(nb))
        ext_task = tg.create_task(
            _safe_ledger(AssetAccount(name="External Lightning Payments", sub=node_name))
        )
        treasury_task = tg.create_task(
            _safe_ledger(AssetAccount(name="Treasury Lightning", sub=node_name))
        )

    await fetch_task
    ext_details = ext_task.result()
    treasury_details = treasury_task.result()

    info = {
        "node": node_name,
        "node_balance_fmt": "N/A",
        "external_sats_fmt": "N/A",
        "treasury_sats_fmt": "N/A",
        "delta_fmt": "N/A",
    }

    try:
        node_sats = None
        if nb.channel and nb.channel.local_balance:
            node_sats = int(nb.channel.local_balance.sat)
            info["node_balance_fmt"] = _fmt_sats(node_sats)

        ext_msat = (
            int(ext_details.msats) if ext_details and ext_details.msats is not None else None
        )
        treasury_msat = (
            int(treasury_details.msats)
            if treasury_details and treasury_details.msats is not None
            else None
        )

        if ext_msat is not None:
            ext_sats = _msat_to_sats_int(ext_msat)
            info["external_sats_fmt"] = _fmt_sats(ext_sats)
        if treasury_msat is not None:
            treasury_sats = _msat_to_sats_int(treasury_msat)
            info["treasury_sats_fmt"] = _fmt_sats(treasury_sats)

        node_msat = node_sats * 1000 if node_sats is not None else None
        if node_msat is not None and ext_msat is not None and treasury_msat is not None:
            delta_msat = node_msat - (ext_msat + treasury_msat)
            info["delta_fmt"] = f"{delta_msat / Decimal(1000):,.3f}"
    except Exception as e:
        logger.warning(f"LND info computation failed: {e}", extra={"notification": False})

    return JSONResponse({"lnd_info": info})


@router.get("/exchange-balances")
async def dashboard_exchange_balances() -> JSONResponse:
    """Binance wallet snapshot plus Treasury Dash ledger balance."""
    info: dict[str, Any] = {
        "BTC": "N/A",
        "HIVE": "N/A",
        "DASH": "N/A",
        "SATS": "N/A",
        "USDT": "N/A",
        "treasury_dash": "N/A",
        "treasury_dash_sub": "",
        "error": None,
    }
    try:
        adapter = get_exchange_adapter()
        balances = adapter.get_balances(["BTC", "HIVE", "DASH", "USDT"])
        info["BTC"] = f"{float(balances.get('BTC', 0)):.8f}"
        info["HIVE"] = f"{float(balances.get('HIVE', 0)):,.3f}"
        info["DASH"] = f"{float(balances.get('DASH', 0)):,.3f}"
        info["USDT"] = f"{float(balances.get('USDT', 0)):,.2f}"
        info["SATS"] = f"{int(balances.get('SATS', 0)):,}"
    except ExchangeConnectionError as e:
        info["error"] = str(e)
    except Exception as e:
        logger.warning(f"Exchange balances failed: {e}", extra={"notification": False})
        info["error"] = str(e)

    try:
        conn = dash_connection()
        sub = treasury_sub(conn.network if conn else None)
        info["treasury_dash_sub"] = sub
        details = await one_account_balance(
            account=AssetAccount(name="Treasury Dash", sub=sub),
            as_of_date=datetime.now(UTC),
        )
        dash_amt = getattr(details, "dash", None) if details else None
        if dash_amt is None and details and getattr(details, "duffs", None):
            dash_amt = details.duffs / DUFFS_PER_DASH
        if dash_amt is not None:
            info["treasury_dash"] = f"{float(dash_amt):,.3f}"
    except Exception as e:
        logger.warning(f"Treasury Dash lookup failed: {e}", extra={"notification": False})

    return JSONResponse({"exchange": info})


@router.get("/financial-summary")
async def dashboard_financial_summary() -> JSONResponse:
    """Fetch P&L and Trading PnL summaries in parallel."""
    profit_loss_usd = None
    trading_pnl_usd = None

    async def _safe_pl():
        try:
            report = await generate_profit_and_loss_report()
            report = convert_decimal128_to_decimal(report)
            return float(report.get("Net Income", {}).get("Total", {}).get("usd", 0))
        except Exception as e:
            logger.warning(f"P&L failed: {e}", extra={"notification": False})
            return None

    async def _safe_trading():
        try:
            report = await generate_trading_pnl_report()
            report = convert_decimal128_to_decimal(report)
            return float(report.get("totals", {}).get("total_trading_pnl_usd", 0))
        except Exception as e:
            logger.warning(f"Trading PnL failed: {e}", extra={"notification": False})
            return None

    async with TaskGroup() as tg:
        pl_task = tg.create_task(_safe_pl())
        tp_task = tg.create_task(_safe_trading())

    profit_loss_usd = pl_task.result()
    trading_pnl_usd = tp_task.result()

    return JSONResponse(
        {
            "profit_loss_usd": profit_loss_usd,
            "trading_pnl_usd": trading_pnl_usd,
        }
    )


@router.get("/sanity")
async def dashboard_sanity() -> JSONResponse:
    """Fetch sanity check results and pending transactions."""
    try:
        async with asyncio.timeout(20):
            async with TaskGroup() as tg:
                sanity_task = tg.create_task(
                    log_all_sanity_checks(
                        local_logger=logger,
                        log_only_failures=True,
                        notification=False,
                        cache_failures=True,
                    )
                )
                pending_task = tg.create_task(PendingTransaction.list_all_str())

        sanity_results: SanityCheckResults = sanity_task.result()
        pending_transactions = pending_task.result()
    except Exception as e:
        logger.exception(
            f"dashboard_sanity endpoint failed to gather data: {e}",
            extra={"notification": False},
        )
        failure_result = SanityCheckResult(
            name="dashboard_sanity",
            is_valid=False,
            details=f"Failed to load sanity data: {e}",
        )
        sanity_results = SanityCheckResults(
            passed=[],
            failed=[("dashboard_sanity", failure_result)],
            results=[("dashboard_sanity", failure_result)],
        )
        pending_transactions = []

    # Determine server balance check status
    server_balance_check = {"status": "unknown", "icon": "❓"}

    # Check if hive balance sanity passed/failed
    try:
        if "server_account_hive_balances" in [name for name, _ in sanity_results.failed]:
            server_balance_check = {"status": "mismatch", "icon": "❌"}
        else:
            server_balance_check = {"status": "match", "icon": "✅"}
    except Exception:
        server_balance_check = {"status": "error", "icon": "⚠️"}

    # Serialize sanity results
    results_data = []
    for name, result in sanity_results.results:
        results_data.append(
            {
                "name": name,
                "is_valid": result.is_valid,
                "details": result.details,
            }
        )

    failed_data = []
    for name, result in sanity_results.failed:
        failed_data.append(
            {
                "name": name,
                "details": result.details,
            }
        )

    return JSONResponse(
        {
            "server_balance_check": server_balance_check,
            "sanity_results": results_data,
            "sanity_failed": failed_data,
            "pending_transactions": pending_transactions,
        }
    )


def _ratio_pct(amount: Decimal, minimum: Decimal) -> Decimal | None:
    """Percent of a minimum already accumulated. None when that minimum is unset."""
    if minimum <= 0:
        return None
    if amount <= 0:
        return Decimal(0)
    return (amount / minimum) * Decimal(100)


def _binding_pct(record: PendingRebalance) -> Decimal:
    """Progress toward execution. Both minimums must be met, so the smaller one binds."""
    measured = [
        pct
        for pct in (
            _ratio_pct(record.pending_qty, record.min_qty_threshold),
            _ratio_pct(record.pending_quote_value, record.min_notional_threshold),
        )
        if pct is not None
    ]
    if not measured:
        return Decimal(100) if record.pending_qty > 0 else Decimal(0)
    return min(measured)


def _pct_number(value: Decimal | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 1)


def _needs_text(record: PendingRebalance) -> str:
    can_execute, reason = record.can_execute()
    if can_execute:
        return "Ready to execute"
    if record.pending_qty <= 0:
        return "Nothing accumulated"
    shortfalls: list[str] = []
    if record.min_qty_threshold > 0 and record.pending_qty < record.min_qty_threshold:
        shortfalls.append(
            format_base_asset(record.min_qty_threshold - record.pending_qty, record.base_asset)
        )
    if (
        record.min_notional_threshold > 0
        and record.pending_quote_value < record.min_notional_threshold
    ):
        shortfalls.append(
            format_quote_asset(
                record.min_notional_threshold - record.pending_quote_value,
                record.quote_asset,
            )
        )
    if shortfalls:
        return "Needs " + " and ".join(shortfalls)
    return reason


# Conversion rows share the pending rebalance short id and carry the customer account
# plus the amount that was added to the buy or sell.
_ORDER_LEDGER_RANK = {
    LedgerType.CONV_HIVE_TO_KEEPSATS: 0,
    LedgerType.CONV_KEEPSATS_TO_HIVE: 0,
    LedgerType.CONV_DASH_TO_SATS: 0,
    LedgerType.CONV_CUSTOMER: 1,
}
_HIVE_UNITS = {Currency.HIVE, Currency.HBD, Currency.DASH, Currency.DUFFS}


def _format_order_amount(amount: Decimal, unit: Currency) -> str:
    if unit == Currency.MSATS:
        sats = (amount / Decimal(1000)).quantize(Decimal(1), rounding=ROUND_HALF_UP)
        return f"{int(sats):,} sats"
    if unit == Currency.SATS:
        return f"{int(amount):,} sats"
    if unit == Currency.DUFFS:
        return f"{amount / DUFFS_PER_DASH:,.3f} DASH"
    if unit in (Currency.HIVE, Currency.HBD, Currency.DASH):
        return f"{amount:,.3f} {unit.value.upper()}"
    return f"{amount} {unit.value}"


def _ledger_order_amount(entry: LedgerEntry) -> tuple[Decimal, Currency]:
    """Amount of the customer order that was added to this rebalance."""
    if entry.ledger_type == LedgerType.CONV_HIVE_TO_KEEPSATS:
        return entry.credit_amount, entry.credit_unit
    if entry.ledger_type in (LedgerType.CONV_KEEPSATS_TO_HIVE, LedgerType.CONV_DASH_TO_SATS):
        return entry.debit_amount, entry.debit_unit
    for amount, unit in (
        (entry.debit_amount, entry.debit_unit),
        (entry.credit_amount, entry.credit_unit),
    ):
        if unit in _HIVE_UNITS and amount:
            return amount, unit
    if entry.debit_unit in (Currency.MSATS, Currency.SATS) and entry.debit_amount:
        return entry.debit_amount, entry.debit_unit
    return entry.credit_amount, entry.credit_unit


def _ledger_account(entry: LedgerEntry) -> str:
    for value in (entry.cust_id, entry.cust_id_to, entry.cust_id_from):
        text = str(value or "").strip()
        if text:
            return text
    return ""


def choose_order_entry(entries: list[LedgerEntry]) -> LedgerEntry | None:
    """Prefer the conversion row over the later customer deposit for the same short id."""
    ranked = [
        entry
        for entry in entries
        if entry.reversed is None and entry.ledger_type in _ORDER_LEDGER_RANK
    ]
    if not ranked:
        return None
    return min(ranked, key=lambda entry: _ORDER_LEDGER_RANK[entry.ledger_type])


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _format_order_age(when: datetime | None, *, now: datetime | None = None) -> str:
    """Age of an order as hours and minutes, for example '2h 14m' or '45m'."""
    if when is None:
        return ""
    current = now or datetime.now(tz=UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    minutes = int((current - when).total_seconds() // 60)
    if minutes < 1:
        return "<1m"
    hours, mins = divmod(minutes, 60)
    if hours == 0:
        return f"{mins}m"
    if mins == 0:
        return f"{hours}h"
    return f"{hours}h {mins}m"


def _format_sats_per_hive(rate: Decimal) -> str:
    if rate <= 0:
        return ""
    quantized = rate.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    text = f"{quantized:,.3f}".rstrip("0").rstrip(".")
    return f"{text} sats/HIVE"


def _decimal_or_zero(value: Any) -> Decimal:
    if value is None or value == "":
        return Decimal(0)
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(0)


def _conv_sats_per_hive(entry: LedgerEntry) -> Decimal:
    """Sats per HIVE from the quote stored on the conversion that booked this order."""
    for conv in (entry.debit_conv, entry.credit_conv):
        if conv is not None and conv.sats_hive > 0:
            return conv.sats_hive
    return Decimal(0)


def _order_detail(
    *,
    account: str,
    value_fmt: str,
    when: datetime | None,
    sats_per_hive: Decimal,
    now: datetime | None = None,
) -> dict[str, str]:
    return {
        "account": account,
        "value_fmt": value_fmt,
        "age": _format_order_age(when, now=now),
        "rate_fmt": _format_sats_per_hive(sats_per_hive),
    }


def describe_ledger_order(entry: LedgerEntry, *, now: datetime | None = None) -> dict[str, str]:
    amount, unit = _ledger_order_amount(entry)
    value_fmt = _format_order_amount(amount, unit) if amount else ""
    return _order_detail(
        account=_ledger_account(entry),
        value_fmt=value_fmt,
        when=_as_datetime(entry.timestamp),
        sats_per_hive=_conv_sats_per_hive(entry),
        now=now,
    )


def _doc_sats_per_hive(doc: dict[str, Any]) -> Decimal:
    for key in ("conv", "debit_conv", "credit_conv"):
        conv = doc.get(key)
        if isinstance(conv, dict):
            rate = _decimal_or_zero(conv.get("sats_hive"))
            if rate > 0:
                return rate
    return Decimal(0)


def describe_hive_op(doc: dict[str, Any], *, now: datetime | None = None) -> dict[str, str]:
    """Hive account and amount from a hive_ops document when no ledger row exists."""
    when = _as_datetime(doc.get("timestamp"))
    sats_per_hive = _doc_sats_per_hive(doc)
    op_type = str(doc.get("type") or doc.get("op_type") or "")
    if op_type == "custom_json":
        payload = doc.get("json") or {}
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        if not isinstance(payload, dict):
            payload = {}
        account = str(
            payload.get("from_account") or payload.get("to_account") or doc.get("cust_id") or ""
        )
        if payload.get("msats") is not None:
            value_fmt = _format_order_amount(Decimal(str(payload["msats"])), Currency.MSATS)
        elif payload.get("sats") is not None:
            value_fmt = _format_order_amount(Decimal(str(payload["sats"])), Currency.SATS)
        else:
            value_fmt = ""
        return _order_detail(
            account=account,
            value_fmt=value_fmt,
            when=when,
            sats_per_hive=sats_per_hive,
            now=now,
        )

    value_fmt = ""
    amount = doc.get("amount")
    if isinstance(amount, dict) and amount.get("amount") is not None:
        try:
            parsed = AmountPyd.model_validate(amount)
            value_fmt = f"{parsed.amount_decimal:,.3f} {parsed.symbol}"
        except Exception:
            value_fmt = ""
    from_acc = str(doc.get("from") or "")
    to_acc = str(doc.get("to") or "")
    account = from_acc or to_acc
    if from_acc and to_acc:
        try:
            account = str(get_transfer_cust_id(from_acc, to_acc))
        except Exception:
            account = from_acc
    return _order_detail(
        account=account,
        value_fmt=value_fmt,
        when=when,
        sats_per_hive=sats_per_hive,
        now=now,
    )


def _orders_payload(
    transaction_ids: list[str],
    order_details: dict[str, dict[str, str]] | None,
) -> list[dict[str, str]]:
    details = order_details or {}
    orders: list[dict[str, str]] = []
    for short_id in transaction_ids:
        detail = details.get(short_id) or {}
        orders.append(
            {
                "short_id": short_id,
                "account": detail.get("account") or "",
                "value_fmt": detail.get("value_fmt") or "",
                "age": detail.get("age") or "",
                "rate_fmt": detail.get("rate_fmt") or "",
            }
        )
    return orders


def _empty_side(direction: str, base_asset: str, quote_asset: str) -> dict[str, Any]:
    return {
        "present": False,
        "direction": direction,
        "pending_qty_fmt": format_base_asset(Decimal(0), base_asset),
        "pending_quote_fmt": format_quote_asset(Decimal(0), quote_asset),
        "min_qty_fmt": "—",
        "min_notional_fmt": "—",
        "qty_pct": None,
        "notional_pct": None,
        "progress_pct": 0.0,
        "transaction_count": 0,
        "transaction_ids": [],
        "orders": [],
        "can_execute": False,
        "status": "No orders",
    }


def _side_payload(
    record: PendingRebalance,
    order_details: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    can_execute, _reason = record.can_execute()
    transaction_ids = list(record.transaction_ids)
    return {
        "present": True,
        "direction": record.direction.value,
        "pending_qty_fmt": format_base_asset(record.pending_qty, record.base_asset),
        "pending_quote_fmt": format_quote_asset(record.pending_quote_value, record.quote_asset),
        "min_qty_fmt": format_base_asset(record.min_qty_threshold, record.base_asset),
        "min_notional_fmt": format_quote_asset(record.min_notional_threshold, record.quote_asset),
        "qty_pct": _pct_number(_ratio_pct(record.pending_qty, record.min_qty_threshold)),
        "notional_pct": _pct_number(
            _ratio_pct(record.pending_quote_value, record.min_notional_threshold)
        ),
        "progress_pct": _pct_number(_binding_pct(record)),
        "transaction_count": record.transaction_count,
        "transaction_ids": transaction_ids,
        "orders": _orders_payload(transaction_ids, order_details),
        "can_execute": can_execute,
        "status": _needs_text(record),
    }


def _merged_side(records: list[PendingRebalance]) -> PendingRebalance | None:
    """Sum every pending document for one side of a pair."""
    if not records:
        return None
    if len(records) == 1:
        return records[0]
    latest = max(records, key=lambda record: record.updated_at)
    return PendingRebalance(
        base_asset=latest.base_asset,
        quote_asset=latest.quote_asset,
        direction=latest.direction,
        exchange=latest.exchange,
        pending_qty=sum((record.pending_qty for record in records), Decimal(0)),
        pending_quote_value=sum(
            (record.pending_quote_value for record in records),
            Decimal(0),
        ),
        min_qty_threshold=max(record.min_qty_threshold for record in records),
        min_notional_threshold=max(record.min_notional_threshold for record in records),
        transaction_count=sum(record.transaction_count for record in records),
        transaction_ids=[
            transaction_id for record in records for transaction_id in record.transaction_ids
        ],
        updated_at=latest.updated_at,
    )


def summarize_pending_rebalances(
    records: list[PendingRebalance],
    order_details: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Group pending rebalances into buy and sell sums with progress to the next order.

    A side executes on its own once its quantity and notional both reach the
    minimums stored on the pending record.
    """
    grouped: dict[tuple[str, str, str], dict[str, list[PendingRebalance]]] = {}
    for record in records:
        key = (record.exchange, record.base_asset, record.quote_asset)
        sides = grouped.setdefault(key, {"buy": [], "sell": []})
        sides[record.direction.value].append(record)

    pairs: list[dict[str, Any]] = []
    closest: dict[str, Any] | None = None
    for exchange, base_asset, quote_asset in sorted(grouped):
        sides = grouped[(exchange, base_asset, quote_asset)]
        buy_record = _merged_side(sides["buy"])
        sell_record = _merged_side(sides["sell"])
        buy = (
            _side_payload(buy_record, order_details)
            if buy_record is not None
            else _empty_side("buy", base_asset, quote_asset)
        )
        sell = (
            _side_payload(sell_record, order_details)
            if sell_record is not None
            else _empty_side("sell", base_asset, quote_asset)
        )
        pair = {
            "exchange": exchange,
            "base_asset": base_asset,
            "quote_asset": quote_asset,
            "symbol": f"{base_asset}/{quote_asset}",
            "buy": buy,
            "sell": sell,
        }
        pairs.append(pair)
        for side in (buy, sell):
            if not side["present"]:
                continue
            if side["progress_pct"] <= 0 and not side["can_execute"]:
                continue
            candidate = {
                "symbol": pair["symbol"],
                "exchange": exchange,
                "direction": side["direction"],
                "label": f"{side['direction'].upper()} {pair['symbol']}",
                "progress_pct": side["progress_pct"],
                "can_execute": side["can_execute"],
                "status": side["status"],
            }
            if closest is None or (
                candidate["can_execute"],
                candidate["progress_pct"],
            ) > (
                closest["can_execute"],
                closest["progress_pct"],
            ):
                closest = candidate

    return {"pairs": pairs, "closest": closest, "error": None}


async def lookup_rebalance_orders(short_ids: list[str]) -> dict[str, dict[str, str]]:
    """Map each pending short id to the Hive account and order value that created it."""
    ids = list(dict.fromkeys(short_id for short_id in short_ids if short_id))
    if not ids:
        return {}

    grouped: dict[str, list[LedgerEntry]] = {}
    cursor = LedgerEntry.collection().find(
        {
            "short_id": {"$in": ids},
            "ledger_type": {"$in": [ledger_type.value for ledger_type in _ORDER_LEDGER_RANK]},
        }
    )
    async for doc in cursor:
        try:
            entry = LedgerEntry.model_validate(doc)
        except Exception as e:
            logger.warning(
                f"Skipping ledger row for rebalance order: {e}",
                extra={"notification": False},
            )
            continue
        grouped.setdefault(entry.short_id, []).append(entry)

    details: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for short_id in ids:
        chosen = choose_order_entry(grouped.get(short_id, []))
        if chosen is None:
            missing.append(short_id)
            continue
        details[short_id] = describe_ledger_order(chosen)

    if missing:
        hive_cursor = InternalConfig.db["hive_ops"].find({"short_id": {"$in": missing}})
        async for doc in hive_cursor:
            short_id = str(doc.get("short_id") or "")
            if not short_id or short_id in details:
                continue
            details[short_id] = describe_hive_op(doc)
    return details


async def list_pending_rebalances() -> list[PendingRebalance]:
    """Read every pending rebalance document, including sides waiting at zero."""
    collection = PendingRebalance.collection()
    cursor = collection.find({})
    results: list[PendingRebalance] = []
    async for doc in cursor:
        results.append(PendingRebalance.model_validate(doc))
    return results


@router.get("/pending-rebalances")
async def dashboard_pending_rebalances() -> JSONResponse:
    """Buy and sell sums from pending_rebalances, and how close each side is to an order."""
    try:
        records = await list_pending_rebalances()
    except Exception as e:
        logger.warning(f"Pending rebalances failed: {e}", extra={"notification": False})
        return JSONResponse({"pairs": [], "closest": None, "error": str(e)})
    short_ids = [short_id for record in records for short_id in record.transaction_ids]
    try:
        order_details = await lookup_rebalance_orders(short_ids)
    except Exception as e:
        logger.warning(f"Rebalance order lookup failed: {e}", extra={"notification": False})
        order_details = {}
    return JSONResponse(summarize_pending_rebalances(records, order_details))
