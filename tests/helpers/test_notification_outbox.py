"""Telegram outbox: retry later, dedupe on a stable key, digest an outage."""

import logging
from datetime import UTC, datetime, timedelta

import pytest
from telegram.error import TimedOut

from v4vapp_backend_v2.helpers.notification_outbox import (
    Outbox,
    backoff_seconds,
    drain_once,
    format_outbox_digest,
    identity_from_record,
    redis_client,
    reset_for_tests,
    start_notification_outbox,
)

NOW = datetime(2026, 11, 16, 5, 0, tzinfo=UTC)
INVOICE = "ab" * 32


@pytest.fixture(autouse=True)
def _clear_outbox():
    reset_for_tests()
    yield
    reset_for_tests()


def _record(message: str, **extra):
    record = logging.LogRecord(
        name=extra.pop("name", "lnd_monitor_v2"),
        level=logging.INFO,
        pathname="lnd_monitor_v2.py",
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def test_digest_line_matches_the_outage_summary():
    text = format_outbox_digest({"invoice": 28, "binance": 12, "other": 7})
    assert text == ("47 notifications queued during the outage: 28 invoice, 12 Binance, 7 other")


def test_digest_line_for_one_notification():
    assert (
        format_outbox_digest({"invoice": 1})
        == "1 notification queued during the outage: 1 invoice"
    )


def test_identity_uses_invoice_hash_and_state_not_the_tail():
    record = _record("ignored", invoice={"r_hash": INVOICE, "state": "SETTLED"})
    early, category = identity_from_record(f"Invoice settled {INVOICE} at 04:37:01", record)
    later, _ = identity_from_record(f"Invoice settled {INVOICE} at 06:45:11", record)
    assert early == later == f"invoice:{INVOICE}:SETTLED"
    assert category == "invoice"


def test_identity_keeps_open_and_settled_apart():
    opened = _record("opened", invoice={"r_hash": INVOICE, "state": "OPEN"})
    settled = _record("settled", invoice={"r_hash": INVOICE, "state": "SETTLED"})
    open_key, _ = identity_from_record("Invoice opened", opened)
    settled_key, _ = identity_from_record("Invoice settled", settled)
    assert open_key != settled_key


def test_identity_uses_error_code_and_marks_binance():
    record = _record(
        "Giving up get_balances after 3 tries",
        name="binance_monitor",
        error_code="binance_api_error",
    )
    key, category = identity_from_record(record.getMessage(), record)
    assert key == "error:binance_api_error"
    assert category == "binance"


def test_backoff_caps_at_five_minutes(monkeypatch):
    monkeypatch.setattr(
        "v4vapp_backend_v2.helpers.notification_outbox.random.random",
        lambda: 0,
    )
    assert backoff_seconds(1) == 2
    assert backoff_seconds(8) == 256
    assert backoff_seconds(9) == 300


def test_drainer_stays_off_under_pytest():
    assert start_notification_outbox() is None


def _enqueue(box: Outbox, *, text: str, category: str, created_at: datetime, hold: bool = False):
    return box.enqueue(
        text=text,
        chat_id=12345,
        bot_name="testbot",
        kwargs={},
        dedupe_key=f"text:{text}",
        category=category,
        hold_for_digest=hold,
        created_at=created_at,
        next_attempt_at=created_at,
    )


class _Recorder:
    def __init__(self, exc: Exception | None = None):
        self.exc = exc
        self.texts: list[str] = []

    async def __call__(self, item):
        if self.exc is not None:
            raise self.exc
        self.texts.append(item["text"])


@pytest.mark.asyncio
async def test_drainer_holds_everything_while_egress_is_down():
    box = Outbox(redis_client())
    _enqueue(box, text="one", category="invoice", created_at=NOW)
    deliver = _Recorder()
    cleared = await drain_once(box, deliver=deliver, now=NOW, egress_down=True)
    assert cleared == 0
    assert deliver.texts == []
    assert all(item["hold_for_digest"] for item in box.all_items())


@pytest.mark.asyncio
async def test_recovery_sends_one_digest_instead_of_replaying(caplog):
    box = Outbox(redis_client())
    _enqueue(box, text="inv", category="invoice", created_at=NOW, hold=True)
    _enqueue(box, text="bin", category="binance", created_at=NOW, hold=True)
    _enqueue(box, text="etc", category="other", created_at=NOW, hold=True)
    deliver = _Recorder()
    with caplog.at_level(logging.INFO, logger="backend"):
        cleared = await drain_once(box, deliver=deliver, now=NOW, egress_down=False)
    assert cleared == 3
    assert deliver.texts == [
        "3 notifications queued during the outage: 1 invoice, 1 Binance, 1 other"
    ]
    assert box.all_items() == []
    assert "outbox drained 3 messages" in caplog.text


@pytest.mark.asyncio
async def test_individual_retry_sends_when_the_path_is_up(caplog):
    box = Outbox(redis_client())
    _enqueue(box, text="hello gad", category="other", created_at=NOW)
    deliver = _Recorder()
    with caplog.at_level(logging.INFO, logger="backend"):
        cleared = await drain_once(box, deliver=deliver, now=NOW, egress_down=False)
    assert cleared == 1
    assert deliver.texts == ["hello gad"]
    assert box.all_items() == []
    assert "outbox drained 1 messages" in caplog.text


@pytest.mark.asyncio
async def test_one_live_retry_per_pass():
    box = Outbox(redis_client())
    _enqueue(box, text="first", category="other", created_at=NOW)
    _enqueue(box, text="second", category="other", created_at=NOW)
    deliver = _Recorder()
    cleared = await drain_once(box, deliver=deliver, now=NOW, egress_down=False)
    assert cleared == 1
    assert deliver.texts == ["first"]
    assert [item["text"] for item in box.all_items()] == ["second"]


@pytest.mark.asyncio
async def test_failed_retry_waits_and_stays_queued():
    box = Outbox(redis_client())
    _enqueue(box, text="still down", category="other", created_at=NOW)
    cleared = await drain_once(
        box,
        deliver=_Recorder(TimedOut("Timed out")),
        now=NOW,
        egress_down=False,
    )
    assert cleared == 0
    stored = box.all_items()
    assert len(stored) == 1
    assert stored[0]["attempts"] == 1
    due = datetime.fromisoformat(stored[0]["next_attempt_at"])
    assert due > NOW


@pytest.mark.asyncio
async def test_stale_outbox_logs_one_error(caplog):
    box = Outbox(redis_client())
    created = NOW - timedelta(minutes=31)
    _enqueue(box, text="stuck", category="other", created_at=created)
    deliver = _Recorder(TimedOut("Timed out"))
    with caplog.at_level(logging.ERROR, logger="backend"):
        await drain_once(box, deliver=deliver, now=NOW, egress_down=False)
    errors = [record for record in caplog.records if "older than 30 min" in record.message]
    assert len(errors) == 1
    assert errors[0].levelno == logging.ERROR
    caplog.clear()
    await drain_once(box, deliver=deliver, now=NOW, egress_down=False)
    assert not any("older than 30 min" in record.message for record in caplog.records)
    assert len(box.all_items()) == 1


@pytest.mark.asyncio
async def test_items_older_than_six_hours_are_dropped(caplog):
    box = Outbox(redis_client())
    created = NOW - timedelta(hours=6, seconds=1)
    _enqueue(box, text="ancient", category="other", created_at=created)
    with caplog.at_level(logging.WARNING, logger="backend"):
        cleared = await drain_once(box, deliver=_Recorder(), now=NOW, egress_down=False)
    assert cleared == 0
    assert box.all_items() == []
    assert "older than 6h" in caplog.text
