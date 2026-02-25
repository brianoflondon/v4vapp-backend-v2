import logging
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent
from v4vapp_backend_v2.process.process_tracked_events import process_tracked_event


@pytest.mark.asyncio
async def test_forward_event_logs_once(caplog, monkeypatch):
    """Ensure that processing a single forward event only emits one INFO log
    record from **process_tracked_events**.

    This guards against the historical bug where the branch for
    ``TrackedForwardEvent`` contained its own ``logger.info`` in addition to
    the generic summary at the end of ``process_tracked_event``.  The
    duplicate logs were noisy in the db-monitor output even though only one
    ledger entry was created.
    """

    caplog.set_level(logging.INFO)

    # minimal forward event, other attributes may be left unset
    event = TrackedForwardEvent(
        group_id="g1",
        htlc_id=1,
        forward_success=True,
        message_type="FORWARD",
        amount=Decimal("4548"),
        fee=Decimal("0.031"),
        fee_percent=Decimal("0"),
        fee_ppm=7,
        from_channel="e960fd8385a1603003727",
        to_channel="Bitrefill",
        timestamp=datetime.now(timezone.utc),
    )

    # avoid any database interactions or other side effects
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.LedgerEntry.load",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.load_tracked_object",
        AsyncMock(return_value=None),
    )

    # lock_str grabs a redis connection; on CI the event loop may differ from
    # redis' connection loop, leading to "Future attached to a different loop"
    # errors.  We don't care about locking in this unit test, so stub it out.
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def _dummy_locked(self, timeout=None, blocking_timeout=None, request_details=None):
        yield

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.lock_str_class.LockStr.locked",
        _dummy_locked,
    )
    # process_tracked_events imported LockStr at module import time, so
    # patch that reference as well.
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.LockStr.locked",
        _dummy_locked,
    )

    class DummySanity:
        log_extra = {}

    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.run_all_sanity_checks",
        AsyncMock(return_value=DummySanity()),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.tracked_forward_models.TrackedForwardEvent.save",
        AsyncMock(),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_tracked_events.process_forward",
        AsyncMock(return_value=[]),
    )

    await process_tracked_event(event)

    # look for the specific success summary message that is emitted when an
    # event is processed.  Previously the same string was logged twice; after
    # the fix we should see a single instance.
    matches = [r for r in caplog.records if "FORWARD HTLC" in r.getMessage()]
    assert len(matches) == 1, (
        f"expected exactly one forward summary log, got {len(matches)}: {matches}"
    )
