from asyncio import TaskGroup
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal

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
    """
    Asynchronously gather and assemble data needed for the admin dashboard.

    This coroutine launches several concurrent tasks to collect status and balance
    information about the node, ledger and highlighted user accounts, then
    aggregates those results into an AdminDataHelper instance.

    Behavior and tasks:
    - Uses InternalConfig() to determine node_name and server_id.
    - Runs the following tasks concurrently via a TaskGroup:
        - log_all_sanity_checks(...) to run/collect sanity checks (logging only failures).
        - PendingTransaction.list_all_str() to fetch pending transactions.
        - NodeBalances.fetch_balances() to read the latest stored node/channel balances.
        - one_account_balance(account=AssetAccount(...)) to fetch the External Lightning Payments asset balance.
        - For each account in admin_config.highlight_users, schedules account_hive_balances on a threadpool.
    - Collects task results, logging and swallowing non-fatal exceptions so the admin view can still be constructed even if some fetches fail.
    - Per-account hive balance fetches: failures are logged and the failing account will not appear with a valid balance in hive_balances.

    Return value:
    Returns an AdminDataHelper object with the following populated fields:
    - node_balances: NodeBalances instance (may reflect previously stored values if fetch failed).
    - ledger_details: result of one_account_balance(...) for the External Lightning Payments asset (or None).
    - pending_transactions: list/representation of pending transactions (may be empty).
    - sanity_results: results from log_all_sanity_checks (contains lists of failed checks).
    - hive_balances: mapping account -> balances or error info for highlight_users (individual failures logged).
    - lnd_info: dict describing LND/external balances and simple formatted strings:
            - node: configured node_name
            - node_balance / node_balance_fmt: sats and formatted sats (or None/"N/A")
            - external_sats / external_sats_fmt: External Lightning Payments sats and formatted value (or None/"N/A")
            - delta / delta_fmt: computed node_balance - external_sats (or None/"N/A")
        All fields are guarded: exceptions during computation are caught and logged and defaults are used.
    - server_balance_check: one of {"status": "unknown"|"mismatch"|"match"|"error", "icon": ...}
        Determined by presence of server_id in hive_balances and whether the corresponding sanity check
        (server_account_hive_balances) failed.

    Side effects and logging:
    - Logs warnings/errors for fetch/formatting failures, but does not propagate these exceptions.
    - Uses extra={"notification": False} on some log calls to suppress notifications.
    - No external state is modified by this function (it only reads configuration and data sources).

    Errors:
    - Designed to be resilient: internal exceptions are logged and suppressed so the dashboard can still render.
    - The coroutine itself does not raise for transient failures in the data collection steps.
    """

    node_name = InternalConfig().node_name
    nb = NodeBalances(node=node_name)

    async def _safe_fetch_balances(nb_obj: NodeBalances):
        try:
            await nb_obj.fetch_balances()
        except Exception as e:
            logger.warning(f"Safe fetch balances failed: {e}", extra={"notification": False})
        return nb_obj

    async def _safe_ledger_details(asset: AssetAccount):
        try:
            return await one_account_balance(account=asset)
        except Exception as e:
            logger.warning(f"Safe ledger lookup failed: {e}", extra={"notification": False})
            return None

    async def _safe_account_balance(acc: str):
        try:
            return await run_in_threadpool(account_hive_balances, acc)
        except Exception as e:
            logger.warning(
                f"Safe account balance for {acc} failed: {e}", extra={"notification": False}
            )
            return {"error": str(e)}

    async with TaskGroup() as tg:
        sanity_task = tg.create_task(
            log_all_sanity_checks(local_logger=logger, log_only_failures=True, notification=False)
        )
        # Fetch pending transactions
        pending_transactions_task = tg.create_task(PendingTransaction.list_all_str())
        # Attempt to read latest stored node balances first (fast) using safe wrappers
        fetch_balances_task = tg.create_task(_safe_fetch_balances(nb))
        # Fetch the External Lightning Payments asset balance for the ledger details section
        asset = AssetAccount(name="External Lightning Payments", sub=node_name)
        ledger_details_task = tg.create_task(_safe_ledger_details(asset))
        # Fetch the Treasury Lightning for the node (this will hold lightning fees)
        treasury_asset = AssetAccount(name="Treasury Lightning", sub=node_name)
        treasury_ledger_details_task = tg.create_task(_safe_ledger_details(treasury_asset))
        balance_tasks = {}
        for acc in InternalConfig().config.admin_config.highlight_users:
            balance_tasks[acc] = tg.create_task(_safe_account_balance(acc))

    sanity_results = await sanity_task
    pending_transactions = await pending_transactions_task
    ledger_details = await ledger_details_task
    treasury_ledger_details = await treasury_ledger_details_task
    await fetch_balances_task

    hive_balances = {}
    for acc, task in balance_tasks.items():
        try:
            hive_balances[acc] = await task
        except Exception as e:
            logger.error(f"Failed to fetch balance for {acc}: {e}")

    # LND / External balances for System Information (store both msat and sat values; convert/format using Decimal)
    lnd_info = {
        "node": None,
        # node balance (both msat and sat)
        "node_balance_msat": None,
        "node_balance": None,
        "node_balance_fmt": "N/A",
        # external ledger balances (msat and sat)
        "external_msat": None,
        "external_sats": None,
        "external_sats_fmt": "N/A",
        # treasury ledger balances (msat and sat)
        "treasury_msat": None,
        "treasury_sats": None,
        "treasury_sats_fmt": "N/A",
        # delta (msat and sat)
        "delta_msat": None,
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
                    node_sats = int(nb.channel.local_balance.sat)
                    lnd_info["node_balance"] = node_sats
                    lnd_info["node_balance_msat"] = node_sats * 1000
            except Exception:
                # Non-fatal: leave node balances as None
                lnd_info["node_balance"] = None
                lnd_info["node_balance_msat"] = None

            # External Lightning Payments asset balance (msats)
            try:
                lnd_info["external_msat"] = (
                    int(ledger_details.msats)
                    if ledger_details and ledger_details.msats is not None
                    else None
                )
                lnd_info["treasury_msat"] = (
                    int(treasury_ledger_details.msats)
                    if treasury_ledger_details and treasury_ledger_details.msats is not None
                    else None
                )
            except Exception:
                lnd_info["external_msat"] = None
                lnd_info["treasury_msat"] = None

            # Helper to convert msat -> sats (int) using Decimal rounding half-up
            def msat_to_sats_int(msat_val):
                try:
                    return int(
                        (Decimal(msat_val) / Decimal(1000)).quantize(
                            Decimal("1"), rounding=ROUND_HALF_UP
                        )
                    )
                except Exception:
                    return None

            # Compute delta in msats if possible
            try:
                if (
                    lnd_info["node_balance_msat"] is not None
                    and lnd_info["external_msat"] is not None
                    and lnd_info["treasury_msat"] is not None
                ):
                    lnd_info["delta_msat"] = lnd_info.get("node_balance_msat", 0) - (
                        lnd_info.get("external_msat", 0) + lnd_info.get("treasury_msat", 0)
                    )
                    # Also compute integer sats using Decimal rounding (half up)
                    lnd_info["delta"] = f"{lnd_info['delta_msat'] / Decimal(1000):,.3f}"
            except Exception:
                lnd_info["delta_msat"] = None
                lnd_info["delta"] = None

            # Convert msat -> sats for external and treasury using Decimal rounding
            try:
                if lnd_info.get("external_msat") is not None:
                    lnd_info["external_sats"] = msat_to_sats_int(lnd_info["external_msat"])
                if lnd_info.get("treasury_msat") is not None:
                    lnd_info["treasury_sats"] = msat_to_sats_int(lnd_info["treasury_msat"])
                if (
                    lnd_info.get("node_balance_msat") is not None
                    and lnd_info.get("node_balance") is None
                ):
                    # Ensure node_balance sats field exists
                    lnd_info["node_balance"] = msat_to_sats_int(lnd_info["node_balance_msat"])
            except Exception:
                lnd_info["external_sats"] = None
                lnd_info["treasury_sats"] = None
                lnd_info["node_balance"] = lnd_info.get("node_balance", None)

            # Formatting helpers: format sats ints with thousands separator
            def fmt_sats_from_int(n):
                try:
                    return f"{int(n):,}"
                except Exception:
                    return "N/A"

            lnd_info["node_balance_fmt"] = (
                fmt_sats_from_int(lnd_info["node_balance"])
                if lnd_info["node_balance"] is not None
                else "N/A"
            )
            lnd_info["external_sats_fmt"] = (
                fmt_sats_from_int(lnd_info["external_sats"])
                if lnd_info["external_sats"] is not None
                else "N/A"
            )
            lnd_info["treasury_sats_fmt"] = (
                fmt_sats_from_int(lnd_info["treasury_sats"])
                if lnd_info["treasury_sats"] is not None
                else "N/A"
            )
            lnd_info["delta_fmt"] = lnd_info["delta"] if lnd_info["delta"] is not None else "N/A"

    except Exception as e:
        logger.warning(
            f"Failed to fetch LND/external balances for admin dashboard: {e}",
            extra={"notification": False},
        )
        # Use defaults in lnd_info
        pass

    server_id = InternalConfig().server_id
    server_balance_check = {"status": "unknown", "icon": "❓"}
    if server_id in hive_balances and "error" not in hive_balances[server_id]:
        try:
            if "server_account_hive_balances" in [name for name, _ in sanity_results.failed]:
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


# End of file
