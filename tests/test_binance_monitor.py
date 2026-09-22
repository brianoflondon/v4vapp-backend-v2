import asyncio
import time
from decimal import Decimal

import pytest

import binance_monitor
from status.status_api import StatusAPIException


@pytest.fixture
def reset_monitor_state():
    previous_success = binance_monitor._last_success_monotonic
    previous_started = binance_monitor._monitor_started_monotonic
    binance_monitor._last_success_monotonic = None
    binance_monitor._monitor_started_monotonic = None
    yield
    binance_monitor._last_success_monotonic = previous_success
    binance_monitor._monitor_started_monotonic = previous_started


async def _hold_balance_task():
    await asyncio.sleep(30)


class _BalanceTask:
    def __init__(self):
        self.task = asyncio.create_task(
            _hold_balance_task(), name=binance_monitor.BALANCE_TASK_NAME
        )

    async def stop(self):
        self.task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await self.task


@pytest.mark.asyncio
async def test_health_check_starting_is_ok(reset_monitor_state):
    holder = _BalanceTask()
    try:
        binance_monitor._monitor_started_monotonic = time.monotonic()
        result = await binance_monitor.health_check()
        assert result["status"] == "OK"
        assert result["last_success_age_s"] is None
    finally:
        await holder.stop()


@pytest.mark.asyncio
async def test_health_check_fails_when_poll_is_stale(reset_monitor_state):
    holder = _BalanceTask()
    try:
        binance_monitor._monitor_started_monotonic = time.monotonic() - 1000
        binance_monitor._last_success_monotonic = time.monotonic() - (
            binance_monitor.HEALTH_MAX_STALE_S + 5
        )
        with pytest.raises(StatusAPIException, match="last successful poll"):
            await binance_monitor.health_check()
    finally:
        await holder.stop()


@pytest.mark.asyncio
async def test_health_check_fails_when_balance_task_is_gone(reset_monitor_state):
    binance_monitor._last_success_monotonic = time.monotonic()
    with pytest.raises(StatusAPIException, match="is not running"):
        await binance_monitor.health_check()


@pytest.mark.asyncio
async def test_poll_logs_start_and_end(monkeypatch, caplog):
    balances = {"HIVE": Decimal(10), "SATS": Decimal(1000)}

    def fetch():
        return balances, Decimal("0.00011")

    monkeypatch.setattr(binance_monitor, "fetch_balances_and_price", fetch)
    monkeypatch.setattr(
        binance_monitor,
        "generate_message",
        lambda saved, current, price: (current, Decimal(1), "note", "log line"),
    )

    with caplog.at_level("INFO"):
        result = await binance_monitor.poll_binance_balances({})

    assert result[0] == balances
    assert "Binance poll started" in caplog.text
    assert "Binance poll ended" in caplog.text
    assert "(ok)" in caplog.text


@pytest.mark.asyncio
async def test_poll_timeout_is_logged(monkeypatch, caplog):
    def fetch_slow():
        time.sleep(0.4)

    monkeypatch.setattr(binance_monitor, "BINANCE_POLL_TIMEOUT_S", 0.05)
    monkeypatch.setattr(binance_monitor, "fetch_balances_and_price", fetch_slow)

    with caplog.at_level("INFO"):
        result = await binance_monitor.poll_binance_balances({})

    assert result is None
    assert "Binance poll started" in caplog.text
    assert "Binance poll failed" in caplog.text
    assert "TimeoutError" in caplog.text
    assert "Binance poll ended" in caplog.text


def test_fetch_rejects_overlapping_poll(reset_monitor_state):
    assert binance_monitor._fetch_lock.acquire(blocking=False)
    try:
        with pytest.raises(binance_monitor.BinancePollInProgress):
            binance_monitor.fetch_balances_and_price()
    finally:
        binance_monitor._fetch_lock.release()


def test_balance_task_exit_is_logged(caplog):
    async def boom():
        raise RuntimeError("balance loop crashed")

    async def run():
        task = asyncio.create_task(boom(), name=binance_monitor.BALANCE_TASK_NAME)
        task.add_done_callback(binance_monitor._log_balance_task_done)
        with pytest.raises(RuntimeError):
            await task

    with caplog.at_level("ERROR"):
        asyncio.run(run())

    assert "exited: RuntimeError: balance loop crashed" in caplog.text
