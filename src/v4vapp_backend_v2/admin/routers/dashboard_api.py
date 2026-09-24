"""
Dashboard API Router

Provides JSON endpoints for progressive loading of admin dashboard sections.
Each endpoint returns data for a specific dashboard section, allowing the
browser to fetch them in parallel and render as they arrive.
"""

import asyncio
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
    get_exchange_adapter,
)
from v4vapp_backend_v2.dash.amounts import DUFFS_PER_DASH
from v4vapp_backend_v2.dash.settings import dash_connection
from v4vapp_backend_v2.database.db_tools import convert_decimal128_to_decimal
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances_async
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
