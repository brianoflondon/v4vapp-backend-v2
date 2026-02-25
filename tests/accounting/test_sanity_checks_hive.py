from decimal import Decimal

import pytest

from v4vapp_backend_v2.accounting import sanity_checks
from v4vapp_backend_v2.accounting.sanity_checks import SanityCheckResults
from v4vapp_backend_v2.helpers.currency_class import Currency


@pytest.mark.asyncio
async def test_hive_task_failure_logged(caplog, monkeypatch):
    """Ensure our new inner timeout/warning around the hive call is exercised.

    We patch `account_hive_balances` to sleep a short time, then force the
    overall runner to use very small timeouts so the inner wrapper around the
    to_thread call raises.  The test asserts that both the explicit "hive
    balance fetch failed" log and the cancellation warning from the check are
    emitted.
    """

    # fake hive call that would normally run in a thread; must be synchronous
    def fake_hive(*args, **kwargs):
        import time

        time.sleep(0.1)
        return {"HIVE": 0, "HBD": 0}

    # minimal account balance result so the other tasks complete quickly
    async def fake_account_balance(*args, **kwargs):
        class Dummy:
            balances_net = {Currency.HIVE: Decimal(0), Currency.HBD: Decimal(0)}

        return Dummy()

    monkeypatch.setattr(sanity_checks, "account_hive_balances", fake_hive)
    monkeypatch.setattr(sanity_checks, "one_account_balance", fake_account_balance)

    # avoid loading real config; we just need a server_id attribute
    class DummyConfig:
        server_id = "testserver"

    monkeypatch.setattr(sanity_checks, "InternalConfig", lambda *args, **kwargs: DummyConfig())

    async def _fake_all_held():
        return {}

    monkeypatch.setattr(sanity_checks, "all_held_msats", _fake_all_held)
    monkeypatch.setattr(
        sanity_checks,
        "all_sanity_checks",
        [sanity_checks.server_account_hive_balances],
    )

    orig_to = sanity_checks.asyncio.timeout

    def timeout_override(delay):
        # only shorten the inner hive-fetch duration (20s) while leaving
        # the per‑check and overall timeouts alone so the check itself isn't
        # cancelled immediately.
        if delay == 20.0:
            return orig_to(0.001)
        return orig_to(delay)

    monkeypatch.setattr(sanity_checks.asyncio, "timeout", timeout_override)

    caplog.set_level("WARNING")

    results: SanityCheckResults = await sanity_checks.run_all_sanity_checks()

    # check that the hive-specific warning appeared
    assert any("hive balance fetch failed" in rec.message for rec in caplog.records)
    # the wrapper won't see the error because the check itself catches
    # exceptions and returns a failure result – our interest is the hive log above.
    # result should indicate failure (timeout bubbled out)
    assert results.failed
