import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from logging import Logger
from typing import Any, Callable, Coroutine, List, Tuple

from pydantic import BaseModel

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.balance_sheet import check_balance_sheet_mongodb
from v4vapp_backend_v2.config.setup import InternalConfig, async_time_stats_decorator, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn

ICON = "🧪"  # Test Tube


class SanityCheckResult(BaseModel):
    name: str
    is_valid: bool
    details: str


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

    def log_extra(self) -> dict:
        """Generate extra logging information.

        Returns:
            dict: A dictionary with counts of passed and failed checks.
        """
        return {"sanity_check_results": self.model_dump()}


async def server_account_balances() -> SanityCheckResult:
    server_id = InternalConfig().server_id
    if server_id is None:
        return SanityCheckResult(
            name="server_account_balances",
            is_valid=False,
            details="Hive server account is not configured.",
        )

    accounts_to_check = ["keepsats", server_id]

    results = []
    for account in accounts_to_check:
        balance = await one_account_balance(account)
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


async def balanced_balance_sheet() -> SanityCheckResult:
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


all_sanity_checks: List[Callable[[], Coroutine[Any, Any, SanityCheckResult]]] = [
    server_account_balances,
    balanced_balance_sheet,
]


@async_time_stats_decorator()
async def run_all_sanity_checks() -> SanityCheckResults:
    """
    Run all registered sanity checks concurrently and return their results as
    a `SanityCheckResults` Pydantic model.

    Each registered check is expected to be an async callable that returns a
    `SanityCheckResult` instance. Checks are scheduled concurrently using an
    asyncio.TaskGroup with a 5.0 second aggregate timeout.

    Returns:
        SanityCheckResults: Pydantic model containing `passed`, `failed` and
        `results` lists of tuples (check_name, SanityCheckResult).
    """
    # Collect coroutines for checks so we can create TaskGroup tasks from coroutines
    try:
        coros: List[Tuple[str, Coroutine[Any, Any, SanityCheckResult]]] = []
        for check in all_sanity_checks:
            check_name = check.__name__
            coros.append((check_name, check()))

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
    for check_name, sanity_result in results_model.results:
        is_valid = sanity_result.is_valid
        details = sanity_result.details
        if not is_valid:
            local_logger.warning(
                f"{ICON} Sanity check '{check_name}' failed: {details}{append_str}",
                extra={"notification": notification},
            )
        else:
            if not log_only_failures:
                local_logger.info(
                    f"{ICON}Sanity check '{check_name}' passed: {details}{append_str}"
                )
    return results_model


if __name__ == "__main__":
    import asyncio

    async def main():
        InternalConfig(config_filename="devhive.config.yaml", log_filename="sanity_checks.log")
        db_conn = DBConn()
        await db_conn.setup_database()
        await log_all_sanity_checks(
            local_logger=logger, log_only_failures=False, notification=False
        )

    asyncio.run(main())
