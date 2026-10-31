"""Pending rows claimed by a live Hive send must not be resent on resume."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from v4vapp_backend_v2.hive.hive_extras import CustomJsonSendError, send_custom_json
from v4vapp_backend_v2.hive_models.pending_transaction_class import PendingCustomJson
from v4vapp_backend_v2.process.process_pending_hive import (
    PENDING_IN_FLIGHT_GRACE,
    pending_resend_blocked,
    resend_pending_custom_jsons,
)


def _pending(**overrides) -> PendingCustomJson:
    fields = {
        "unique_key": "v4vapp_v4vapp_transfer_test",
        "cj_id": "v4vapp_transfer",
        "send_account": "v4vapp",
        "json_data": {
            "from_account": "v4vapp",
            "to_account": "podping",
            "msats": 109000,
        },
        "active": True,
    }
    fields.update(overrides)
    return PendingCustomJson(**fields)


def test_fresh_in_flight_row_is_not_resent():
    now = datetime(2026, 10, 31, 6, 29, 51, tzinfo=UTC)
    pending = _pending(in_flight=True, in_flight_since=now)
    assert pending_resend_blocked(pending, now=now + timedelta(seconds=1)) is True


def test_abandoned_in_flight_row_can_be_resent():
    now = datetime(2026, 10, 31, 6, 29, 51, tzinfo=UTC)
    pending = _pending(
        in_flight=True,
        in_flight_since=now - PENDING_IN_FLIGHT_GRACE - timedelta(seconds=1),
    )
    assert pending_resend_blocked(pending, now=now) is False


def test_row_that_is_not_in_flight_can_be_resent():
    pending = _pending(in_flight=False, in_flight_since=None)
    assert pending_resend_blocked(pending) is False


def test_in_flight_row_without_a_timestamp_stays_blocked():
    pending = _pending(in_flight=True, in_flight_since=None)
    assert pending_resend_blocked(pending) is True


def test_naive_in_flight_timestamp_is_treated_as_utc():
    now = datetime(2026, 10, 31, 6, 29, 51, tzinfo=UTC)
    pending = _pending(in_flight=True, in_flight_since=now.replace(tzinfo=None))
    assert pending_resend_blocked(pending, now=now + timedelta(seconds=1)) is True


async def test_resend_does_not_broadcast_a_fresh_in_flight_custom_json(monkeypatch):
    fresh = _pending(in_flight=True, in_flight_since=datetime.now(tz=UTC))
    monkeypatch.setattr(
        PendingCustomJson,
        "list_all",
        AsyncMock(return_value=[fresh]),
    )
    send = AsyncMock()
    get_client = AsyncMock()
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_pending_hive.send_custom_json",
        send,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_pending_hive.get_verified_hive_client",
        get_client,
    )

    await resend_pending_custom_jsons()

    send.assert_not_awaited()
    get_client.assert_not_awaited()


async def test_resend_broadcasts_an_abandoned_in_flight_custom_json(monkeypatch):
    stale = SimpleNamespace(
        unique_key="v4vapp_v4vapp_transfer_abandoned",
        cj_id="v4vapp_transfer",
        send_account="v4vapp",
        json_data={"from_account": "v4vapp", "to_account": "podping", "msats": 109000},
        active=True,
        nobroadcast=False,
        resend_attempt=0,
        in_flight=True,
        in_flight_since=datetime.now(tz=UTC) - PENDING_IN_FLIGHT_GRACE - timedelta(seconds=1),
        delete=AsyncMock(),
    )
    monkeypatch.setattr(PendingCustomJson, "list_all", AsyncMock(return_value=[stale]))
    send = AsyncMock(return_value={"trx_id": "9a4ec5335985885c6c0c4cb94400856761681340"})
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_pending_hive.send_custom_json",
        send,
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.process.process_pending_hive.get_verified_hive_client",
        AsyncMock(return_value=(MagicMock(), None)),
    )

    await resend_pending_custom_jsons()

    send.assert_awaited()
    assert send.await_args.kwargs["resend_attempt"] == 1
    stale.delete.assert_awaited()


async def test_send_custom_json_inserts_an_in_flight_pending_row(monkeypatch):
    created: dict = {}

    class FakePending:
        def __init__(self, **kwargs):
            created.update(kwargs)
            self.id = "pending-id"

        async def save(self):
            return self

        async def delete(self):
            created["deleted"] = True

        async def set_in_flight(self, in_flight: bool):
            created["released"] = in_flight

    monkeypatch.setattr(
        "v4vapp_backend_v2.hive.hive_extras.PendingCustomJson",
        FakePending,
    )
    hive = MagicMock()
    hive.nobroadcast = False
    hive.custom_json.return_value = {"trx_id": "6a5695bc8af12c7496862cdfaa1118ec915d83a9"}

    trx = await send_custom_json(
        json_data={"from_account": "v4vapp", "to_account": "podping", "msats": 109000},
        send_account="v4vapp",
        hive_client=hive,
        id="v4vapp_transfer",
        active=True,
    )

    assert trx["trx_id"] == "6a5695bc8af12c7496862cdfaa1118ec915d83a9"
    assert created["in_flight"] is True
    assert created["in_flight_since"] is not None
    assert created["deleted"] is True
    assert "released" not in created


async def test_failed_custom_json_send_releases_the_in_flight_claim(monkeypatch):
    created: dict = {}

    class FakePending:
        def __init__(self, **kwargs):
            created.update(kwargs)
            self.id = "pending-id"

        async def save(self):
            return self

        async def delete(self):
            created["deleted"] = True

        async def set_in_flight(self, in_flight: bool):
            created["released"] = in_flight

    monkeypatch.setattr(
        "v4vapp_backend_v2.hive.hive_extras.PendingCustomJson",
        FakePending,
    )
    hive = MagicMock()
    hive.nobroadcast = False
    hive.custom_json.side_effect = RuntimeError("node down")

    with pytest.raises(CustomJsonSendError):
        await send_custom_json(
            json_data={"from_account": "v4vapp", "to_account": "podping", "msats": 109000},
            send_account="v4vapp",
            hive_client=hive,
            id="v4vapp_transfer",
            active=True,
        )

    assert created["in_flight"] is True
    assert created["released"] is False
    assert "deleted" not in created
