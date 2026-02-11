import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from logging import Logger
from typing import Any, Callable, Coroutine, List, Tuple

from nectar.amount import Amount
from pydantic import BaseModel

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.balance_sheet import check_balance_sheet_mongodb
from v4vapp_backend_v2.accounting.in_progress_results_class import (
    InProgressResults,
    all_held_msats,
)
from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.hive_extras import account_hive_balances

ICON = "🧪"  # Test Tube


class SanityCheckResult(BaseModel):
    """
    A Pydantic model representing the result of a single sanity check.

    This model captures the identity, outcome, and human-readable details of a
    sanity check. It also provides convenience properties for logging:
    - `log_extra` returns a serialized dict suitable for structured logging.
    - `log_str` returns a concise, human-readable summary string including an
        emoji status and the check details.

    Attributes:
            name (str): The identifier or descriptive name of the sanity check.
            is_valid (bool): True if the check passed, False if it failed.
            details (str): A short message or explanation describing the result.

    Example:
            >>> SanityCheckResult(name="cache_health", is_valid=False, details="Miss rate too high")
            SanityCheckResult(...)
    """

    name: str
    is_valid: bool
    details: str

    @property
    def log_extra(self) -> dict:
        """Generate extra logging information.

        Returns:
            dict: A dictionary with the sanity check result details.
        """
        return {"sanity_check_result": self.model_dump()}

    @property
    def log_str(self) -> str:
        """Generate a log string summarizing the sanity check result.

        Returns:
            str: A formatted string indicating the check name, validity, and details.
        """
        status = "PASSED ✅" if self.is_valid else "FAILED ❌"
        return f"{ICON} Sanity check {self.name} {status}: {self.details}"


class SanityCheckResults(BaseModel):
    check_time: datetime = datetime.now(tz=timezone.utc)
    passed: List[Tuple[str, SanityCheckResult]] = []
    failed: List[Tuple[str, SanityCheckResult]] = []
    results: List[Tuple[str, SanityCheckResult]] = []

    def len(self) -> int:
        """Return the number of failed sanity checks.

        Returns:
            int: The count of entries in self.failed.
        """
        return len(self.failed)

    @property
    def log_str(self) -> str:
        """Generate a log string summarizing the sanity check result.

        Returns:
            str: A formatted string indicating the check name, validity, and details.
        """
        if self.failed:
            answer = "FAILED" + "; ".join(
                f"{name}: {result.details}" for name, result in self.failed
            )
        else:
            answer = "PASSED"
        return answer

    @property
    def log_extra(self) -> dict:
        """Generate extra logging information.

        Returns:
            dict: A dictionary with counts of passed and failed checks.
        """
        return {"sanity_check_results": self.model_dump()}


# MARK: Individual sanity check tests


async def server_account_balances(in_progress: InProgressResults) -> SanityCheckResult:
    """Asynchronously verify that server-related accounts have near-zero balances.

    This coroutine reads the server identifier from InternalConfig().server_id and
    checks the balances of two accounts: the hard-coded "keepsats" account and the
    server account (if configured). For each account it calls await one_account_balance(account)
    and treats a balance as non-zero if abs(balance.msats) > Decimal(2_000) (i.e., greater
    than 2,000 msats, or 2 sats).

    Returns:
        SanityCheckResult: A result object with
            - name: "server_account_balances"
            - is_valid: False and details describing accounts with balances exceeding the tolerance,
                        or True and a pass message naming the checked accounts when all are within tolerance.

    Special cases:
        - If InternalConfig().server_id is None, returns is_valid=False with details indicating
          the hive server account is not configured.

    Exceptions:
        Exceptions from InternalConfig() or one_account_balance(...) may propagate.
    """
    server_id = InternalConfig().server_id
    if server_id is None:
        return SanityCheckResult(
            name="server_account_balances",
            is_valid=False,
            details="Hive server account is not configured.",
        )

    accounts_to_check = ["keepsats", server_id]

    results: List[str] = []

    async def _safe_one_account_balance(account_name: str, in_progress: InProgressResults):
        try:
            return await one_account_balance(account=account_name, in_progress=in_progress)
        except Exception as e:
            logger.error(e, extra={"notification": False})
            return e

    tasks: dict[str, asyncio.Task] = {}
    async with asyncio.TaskGroup() as tg:
        for account in accounts_to_check:
            tasks[account] = tg.create_task(
                _safe_one_account_balance(account_name=account, in_progress=in_progress)
            )

    for account, task in tasks.items():
        res = task.result()
        if isinstance(res, Exception):
            results.append(f"Account '{account}' check failed: {res}")
            continue
        balance = res
        if abs(balance.msats) > Decimal(2_000):  # 2,000 msats = 2 sats tolerance
            results.append(
                f"Account '{account}' has non zero balance: {balance.msats / 1000:,.3f} sats"
            )

    if results:
        return SanityCheckResult(
            name="server_account_balances", is_valid=False, details="; ".join(results)
        )

    return SanityCheckResult(
        name="server_account_balances",
        is_valid=True,
        details=f"Server account balances: {', '.join(accounts_to_check)} sanity check passed.",
    )


async def server_account_hive_balances(in_progress: InProgressResults) -> SanityCheckResult:
    # return SanityCheckResult(
    #     name="server_account_hive_balances", is_valid=True, details="Placeholder implementation."
    # )
    try:
        # Get customer deposits balance
        server_id = InternalConfig().server_id
        customer_deposits_account = AssetAccount(name="Customer Deposits Hive", sub=server_id)

        tasks: dict[str, asyncio.Task] = {}
        async with asyncio.TaskGroup() as tg:
            tasks["deposits_details"] = tg.create_task(
                one_account_balance(account=customer_deposits_account, in_progress=in_progress)
            )
            tasks["balances"] = tg.create_task(
                asyncio.to_thread(account_hive_balances, hive_accname=server_id)
            )

        deposits_details = tasks["deposits_details"].result()
        balances = tasks["balances"].result()

        # Get balances with tolerance
        hive_deposits = deposits_details.balances_net.get(Currency.HIVE, Decimal(0.0))
        hbd_deposits = deposits_details.balances_net.get(Currency.HBD, Decimal(0.0))

        hive_actual = Amount(balances.get("HIVE", 0.0))
        hbd_actual = Amount(balances.get("HBD", 0.0))

        # Check with tolerance
        tolerance = Decimal(0.001)
        hive_delta = hive_deposits - hive_actual.amount_decimal
        hbd_delta = hbd_deposits - hbd_actual.amount_decimal
        hive_match = abs(hive_delta) <= tolerance
        hbd_match = abs(hbd_delta) <= tolerance

        if hive_match and hbd_match:
            return SanityCheckResult(
                name="server_account_hive_balances",
                is_valid=True,
                details=(
                    f"Server Hive balances match: HIVE deposits {hive_deposits:,.3f}, "
                    f"HBD deposits {hbd_deposits:,.3f}."
                ),
            )
        else:
            return SanityCheckResult(
                name="server_account_hive_balances",
                is_valid=False,
                details=(
                    f"Server Hive Mismatch: {hive_delta:,.3f} HIVE, {hbd_delta:,.3f} HBD; "
                    f"balances mismatch: HIVE deposits {hive_deposits:,.3f} vs actual {hive_actual:,.3f}, "
                    f"HBD deposits {hbd_deposits:,.3f} vs actual {hbd_actual:,.3f}."
                ),
            )

    except Exception as e:
        logger.exception(
            f"Failed to check customer deposits balance: {e}",
            extra={"notification": False},
        )
        return SanityCheckResult(
            name="server_account_hive_balances",
            is_valid=False,
            details=f"Failed to check customer deposits balance: {e}",
        )


async def balanced_balance_sheet(in_progress: InProgressResults) -> SanityCheckResult:
    """Asynchronously check whether the balance sheet is balanced and return a SanityCheckResult.

    This coroutine calls check_balance_sheet_mongodb() to obtain a tuple (is_balanced, tolerance)
    where `is_balanced` is a boolean indicating whether the balance sheet balances and `tolerance`
    is an optional numeric value (in millisatoshis, "msats") describing allowable discrepancy.
    The function formats the tolerance for display: if tolerance is None it is rendered as "unknown";
    if numeric, it attempts to format it to one decimal place and falls back to str(tolerance) on failure.

    Returns:
        SanityCheckResult: an object with
            - name: "balanced_balance_sheet"
            - is_valid: the boolean `is_balanced` returned by the underlying check
            - details: a human-readable message stating whether the balance sheet is balanced and
                       including the formatted tolerance in msats.

    Raises:
        Any exceptions raised by check_balance_sheet_mongodb() are propagated. Formatting errors
        when converting the tolerance to float are caught and handled internally.
    """
    is_balanced, tolerance = await check_balance_sheet_mongodb()
    # tolerate a missing/None tolerance value and format numeric tolerances safely
    if tolerance is None:
        tol_text = "unknown"
    else:
        # tolerance may be Decimal/int/float — format safely to one decimal place
        try:
            tol_text = f"{float(tolerance):.1f}"
        except Exception:
            tol_text = str(tolerance)
    if is_balanced:
        balance_line_text = f"The balance sheet is balanced ({tol_text} msats tolerance)."
    else:
        balance_line_text = (
            f"******* The balance sheet is NOT balanced. Tolerance: {tol_text} msats. ********"
        )
    return SanityCheckResult(
        name="balanced_balance_sheet", is_valid=is_balanced, details=balance_line_text
    )


all_sanity_checks: List[Callable[[InProgressResults], Coroutine[Any, Any, SanityCheckResult]]] = [
    server_account_balances,
    balanced_balance_sheet,
    server_account_hive_balances,
]

# MARK: Runner for all sanity checks


# @async_time_decorator
async def run_all_sanity_checks() -> SanityCheckResults:
    """
    Run all registered sanity checks concurrently and return their results as
    a `SanityCheckResults` Pydantic model.

    Each registered check is expected to be an async callable that returns a
    `SanityCheckResult` instance. Checks are scheduled concurrently using an
    asyncio.TaskGroup with a 5.0 second aggregate timeout.

    This call will run `one_account_balance` and other database operations
    multiple times, so it may be resource-intensive.

    Returns:
        SanityCheckResults: Pydantic model containing `passed`, `failed` and
        `results` lists of tuples (check_name, SanityCheckResult).
    """
    # Collect coroutines for checks so we can create TaskGroup tasks from coroutines
    all_held_result = await all_held_msats()
    in_progress = InProgressResults(results=all_held_result)

    try:
        coros: List[Tuple[str, Coroutine[Any, Any, SanityCheckResult]]] = []
        for check in all_sanity_checks:
            check_name = check.__name__
            coros.append((check_name, check(in_progress)))

        # Will hold (check_name, Task) pairs created inside the TaskGroup
        task_list: List[Tuple[str, asyncio.Task]] = []

        async with asyncio.timeout(5.0):  # 5 seconds timeout for all checks
            async with asyncio.TaskGroup() as tg:
                for check_name, coro in coros:
                    task = tg.create_task(coro)
                    task_list.append((check_name, task))

        passed: List[Tuple[str, SanityCheckResult]] = []
        failed: List[Tuple[str, SanityCheckResult]] = []
        all_results: List[Tuple[str, SanityCheckResult]] = []

        for check_name, task in task_list:
            try:
                sanity_result = task.result()
            except Exception as e:
                sanity_result = SanityCheckResult(name=check_name, is_valid=False, details=str(e))
            if sanity_result.is_valid:
                passed.append((check_name, sanity_result))
            else:
                failed.append((check_name, sanity_result))
            # keep the full ordered list as well
            all_results.append((check_name, sanity_result))

        # Optionally filter logging elsewhere; always return the full model
        return SanityCheckResults(passed=passed, failed=failed, results=all_results)
    except Exception as e:
        return SanityCheckResults(
            passed=[],
            failed=[
                (
                    "run_all_sanity_checks",
                    SanityCheckResult(
                        name="run_all_sanity_checks", is_valid=False, details=str(e)
                    ),
                )
            ],
            results=[],
        )


async def log_all_sanity_checks(
    local_logger: Logger,
    log_only_failures: bool = True,
    notification: bool = False,
    append_str: str = "",
) -> SanityCheckResults:
    """
    Run all sanity checks, log their outcomes, and return the Pydantic results model.

    The function calls `run_all_sanity_checks()` to obtain a `SanityCheckResults`
    instance, then logs each check's outcome. Failed checks are logged at WARNING
    level with extra={"notification": True}; passed checks are logged at INFO
    level only when `log_only_failures` is False.

    Returns
    -------
    SanityCheckResults
        The full set of sanity check results (passed, failed, results).
    """
    results_model = await run_all_sanity_checks()
    if append_str:
        append_str = " " + append_str
    for _, sanity_result in results_model.results:
        if not sanity_result.is_valid:
            local_logger.warning(
                f"{sanity_result.log_str}{append_str}",
                extra={"notification": notification, **sanity_result.log_extra},
            )
        else:
            if not log_only_failures:
                local_logger.info(
                    f"{sanity_result.log_str}{append_str}",
                )
    return results_model


if __name__ == "__main__":
    import asyncio

    async def main():
        InternalConfig(config_filename="devhive.config.yaml")
        db_conn = DBConn()
        await db_conn.setup_database()
        await log_all_sanity_checks(
            local_logger=logger, log_only_failures=False, notification=False
        )

    asyncio.run(main())
