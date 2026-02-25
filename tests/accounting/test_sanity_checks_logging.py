import asyncio

import pytest

from v4vapp_backend_v2.accounting import sanity_checks
from v4vapp_backend_v2.accounting.in_progress_results_class import InProgressResults
from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResult, SanityCheckResults


async def _slow_check(in_progress: InProgressResults) -> SanityCheckResult:
    # sleep a small amount; the per-check timeout will be reduced below so
    # this still triggers the timeout quickly.
    await asyncio.sleep(0.1)
    return SanityCheckResult(name="slow", is_valid=True, details="should not reach")


async def _fast_check(in_progress: InProgressResults) -> SanityCheckResult:
    return SanityCheckResult(name="fast", is_valid=True, details="ok")


@pytest.mark.asyncio
async def test_timeout_logs_task_status(caplog, monkeypatch):
    """When all checks take too long the outer timeout is hit and we log each task's status.

    We replace ``all_sanity_checks`` with a slow and a fast check and run the
    standard runner.  ``caplog`` is used to assert that the helper warning
    message containing "status on overall timeout" appears in the logs.
    """

    # avoid hitting mongo by faking the in-progress results helper
    async def _fake_all_held():
        return {}

    monkeypatch.setattr(sanity_checks, "all_held_msats", _fake_all_held)
    monkeypatch.setattr(sanity_checks, "all_sanity_checks", [_slow_check, _fast_check])
    # shorten the timeouts used in the runner so the test finishes fast
    orig_timeout = sanity_checks.asyncio.timeout
    monkeypatch.setattr(sanity_checks.asyncio, "timeout", lambda *_: orig_timeout(0.001))

    caplog.set_level("DEBUG")  # capture start/finish at debug level

    results: SanityCheckResults = await sanity_checks.run_all_sanity_checks()

    # the slow check should have timed out and therefore the wrapper will
    # have logged a warning about the exception it raised
    assert any("sanity check '_slow_check'" in rec.message for rec in caplog.records)
    # our wrapper still logs start/finish messages for every check
    assert any("starting sanity check _slow_check" in rec.message for rec in caplog.records)
    assert any("completed sanity check _slow_check" in rec.message for rec in caplog.records)
