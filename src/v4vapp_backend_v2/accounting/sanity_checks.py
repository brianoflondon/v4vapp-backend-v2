import asyncio
from decimal import Decimal
from logging import Logger
from typing import Any, Callable, Coroutine, List, Tuple

from v4vapp_backend_v2.accounting.account_balances import one_account_balance
from v4vapp_backend_v2.accounting.balance_sheet import check_balance_sheet_mongodb
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn

ICON = "🧪"  # Test Tube


async def server_account_balances() -> Tuple[bool, str]:
    server_id = InternalConfig().server_id
    if server_id is None:
        return False, "Hive server account is not configured."

    accounts_to_check = ["keepsats", server_id]

    results = []
    for account in accounts_to_check:
        balance = await one_account_balance(account)
        if abs(balance.msats) > Decimal(2_000):  # 2,000 msats = 2 sats tolerance
            results.append(
                f"Account '{account}' has non zero balance: {balance.msats / 1000:,.3f} sats"
            )

    if results:
        return False, "; ".join(results)

    return True, f"Server account balances: {', '.join(accounts_to_check)} sanity check passed."


async def balanced_balance_sheet() -> Tuple[bool, str]:
    is_balanced, tolerance = await check_balance_sheet_mongodb()
    # tolerate a missing/None tolerance value and format numeric tolerances safely
    if tolerance is None:
        tol_text = "unknown"
    else:
        tol_text = f"{tolerance:.1f}"
    if is_balanced:
        balance_line_text = f"The balance sheet is balanced ({tol_text} msats tolerance)."
    else:
        balance_line_text = (
            f"******* The balance sheet is NOT balanced. Tolerance: {tol_text} msats. ********"
        )
    return is_balanced, balance_line_text


all_sanity_checks: List[Callable[[], Coroutine[Any, Any, Tuple[bool, str]]]] = [
    server_account_balances,
    balanced_balance_sheet,
]


async def run_all_sanity_checks() -> List[Tuple[str, bool, str]]:
    """
    Run all registered sanity checks concurrently and return their results.

    This coroutine collects coroutines from the global `all_sanity_checks` (each element
    is expected to be an async callable that returns a Tuple[bool, str]) and schedules
    them using an asyncio.TaskGroup. There is a hard overall timeout of 5.0 seconds for
    all checks combined.

    Returns:
        List[Tuple[str, bool, str]]: A list of tuples (check_name, is_valid, details),
            where `check_name` is the check callable's __name__, `is_valid` is True when
            the check completed successfully and reported success, and `details` is the
            message returned by the check or an error description if the check failed.

    Behavior/Errors:
        - Each check is created and scheduled with TaskGroup.create_task().
        - If an individual check raises an exception, it will be recorded as
          is_valid=False and `details` will contain the exception message.
        - If the aggregate 5-second timeout is exceeded, asyncio.TimeoutError is raised
          (no partial results are returned in that case).

    Notes:
        - The order of returned results corresponds to the order of checks in
          `all_sanity_checks`.
        - Checks must be coroutines (async functions); invoking a non-coroutine may
          raise at call time.
    """
    results: List[Tuple[str, bool, str]] = []
    # Collect coroutines for checks so we can create TaskGroup tasks from coroutines (TaskGroup.create_task expects a coroutine)
    coros: List[Tuple[str, Coroutine[Any, Any, Tuple[bool, str]]]] = []
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

    for check_name, task in task_list:
        try:
            is_valid, details = task.result()
        except Exception as e:
            is_valid = False
            details = f"Sanity check '{check_name}' raised an exception: {e}"
        results.append((check_name, is_valid, details))
    return results


async def log_all_sanity_checks(
    local_logger: Logger,
    log_only_failures: bool = True,
    notification: bool = False,
    append_str: str = "",
) -> None:
    """
    Run all sanity checks and log their outcomes.

    This coroutine invokes run_all_sanity_checks(), iterates over the returned
    results and logs a message for each check. Failed checks are logged at WARNING
    level with extra={"notification": True}. Passed checks are logged at INFO
    level only when log_only_failures is False.

    Parameters
    ----------
    local_logger : Logger
        Logger used to emit messages for each sanity check.
    log_only_failures : bool, optional
        If True (default), only failed checks are logged. If False, both passed
        and failed checks are logged.

    Returns
    -------
    None
        The function performs logging as a side effect and does not return a value.

    Notes
    -----
    - Expects run_all_sanity_checks() to return an iterable of tuples
      (check_name: str, is_valid: bool, details: Any).
    - Any exceptions raised by run_all_sanity_checks() will propagate to the caller.
    """
    results = await run_all_sanity_checks()
    if append_str:
        append_str = " " + append_str
    for check_name, is_valid, details in results:
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
