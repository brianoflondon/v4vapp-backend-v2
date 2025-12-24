from asyncio import TaskGroup
from dataclasses import dataclass
from pprint import pprint

from fastapi.concurrency import run_in_threadpool

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.accounting_classes import LedgerAccountDetails
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults, log_all_sanity_checks
from v4vapp_backend_v2.config.decorators import async_time_stats_decorator
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingTransaction

# LND and accounting helpers used on dashboard
from v4vapp_backend_v2.models.lnd_balance_models import NodeBalances


@dataclass
class AdminDataHelper:
    node_balances: NodeBalances
    ledger_details: LedgerAccountDetails
    pending_transactions: list[str]
    sanity_results: SanityCheckResults
    hive_balances: dict[str, dict[str, str | float]]
    lnd_info: dict[str, str | int | None]
    server_balance_check: dict[str, str]


@async_time_stats_decorator(runs=10)
async def admin_data_helper() -> AdminDataHelper:
    """Helper function for admin data operations"""
    node_name = InternalConfig().node_name
    nb = NodeBalances(node=node_name)

    async with TaskGroup() as tg:
        sanity_task = tg.create_task(
            log_all_sanity_checks(local_logger=logger, log_only_failures=True, notification=False)
        )
        # Fetch pending transactions
        pending_transactions_task = tg.create_task(PendingTransaction.list_all_str())
        # Attempt to read latest stored node balances first (fast)
        fetch_balances_task = tg.create_task(nb.fetch_balances())
        asset = AssetAccount(name="External Lightning Payments", sub=node_name)
        ledger_details_task = tg.create_task(one_account_balance(account=asset))
        balance_tasks = {}
        for acc in InternalConfig().config.admin_config.highlight_users:
            balance_tasks[acc] = tg.create_task(run_in_threadpool(account_hive_balances, acc))

    sanity_results = await sanity_task
    pending_transactions = await pending_transactions_task
    ledger_details = await ledger_details_task
    await fetch_balances_task

    hive_balances = {}
    for acc, task in balance_tasks.items():
        try:
            hive_balances[acc] = await task
        except Exception as e:
            logger.error(f"Failed to fetch balance for {acc}: {e}")

    # LND / External balances for System Information
    lnd_info = {
        "node": None,
        "node_balance": None,
        "node_balance_fmt": "N/A",
        "external_sats": None,
        "external_sats_fmt": "N/A",
        "delta": None,
        "delta_fmt": "N/A",
    }

    try:
        # Get configured default LND node (if any)
        lnd_info["node"] = node_name
        if lnd_info["node"]:
            # Attempt to read latest stored node balances first (fast)
            try:
                if nb.channel and nb.channel.local_balance:
                    lnd_info["node_balance"] = int(nb.channel.local_balance.sat)

            except Exception:
                # Non-fatal: leave node_balance as None
                lnd_info["node_balance"] = None

            # External Lightning Payments asset balance (sats)
            try:
                lnd_info["external_sats"] = (
                    int(ledger_details.sats)
                    if ledger_details and ledger_details.sats is not None
                    else None
                )
            except Exception:
                lnd_info["external_sats"] = None

            # Compute delta if possible
            try:
                if lnd_info["node_balance"] is not None and lnd_info["external_sats"] is not None:
                    lnd_info["delta"] = int(lnd_info["node_balance"] - lnd_info["external_sats"])
            except Exception:
                lnd_info["delta"] = None

            # Formatting helpers
            def fmt_sats(x):
                try:
                    return f"{int(x):,}"
                except Exception:
                    return "N/A"

            lnd_info["node_balance_fmt"] = (
                fmt_sats(lnd_info["node_balance"])
                if lnd_info["node_balance"] is not None
                else "N/A"
            )
            lnd_info["external_sats_fmt"] = (
                fmt_sats(lnd_info["external_sats"])
                if lnd_info["external_sats"] is not None
                else "N/A"
            )
            lnd_info["delta_fmt"] = (
                fmt_sats(lnd_info["delta"]) if lnd_info["delta"] is not None else "N/A"
            )
    except Exception as e:
        logger.warning(
            f"Failed to fetch LND/external balances for admin dashboard: {e}",
            extra={"notification": False},
        )
        # Use defaults in lnd_info
        pass

    server_id = InternalConfig().server_id
    server_balance_check = {"status": "unknown", "icon": "❓"}
    if (
        server_id in hive_balances
        and "error" not in hive_balances[server_id]
    ):
        try:
            if "server_account_hive_balances" in [
                name for name, _ in sanity_results.failed
            ]:
                server_balance_check = {"status": "mismatch", "icon": "❌"}
            else:
                server_balance_check = {"status": "match", "icon": "✅"}

        except Exception as e:
            logger.warning(
                f"Failed to check customer deposits balance: {e}",
                extra={"notification": False},
            )
            server_balance_check = {"status": "error", "icon": "⚠️"}

    return AdminDataHelper(
        node_balances=nb,
        ledger_details=ledger_details,
        pending_transactions=pending_transactions,
        sanity_results=sanity_results,
        hive_balances=hive_balances,
        lnd_info=lnd_info,
        server_balance_check=server_balance_check,
    )

    pprint(
        {
            "node_balances": nb,
            "ledger_details": ledger_details,
            "pending_transactions": pending_transactions,
            "sanity_checks": sanity_results,
            "hive_balances": hive_balances,
        }
    )
