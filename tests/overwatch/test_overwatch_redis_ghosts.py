"""Stale Overwatch copies left behind when cust_id is filled in.

An invoice is persisted with an empty customer id. A later event learns
the customer and the Redis key changes. The old field must be deleted,
and a restart that still finds one must drop it when a completed sibling
already covers its events. A narrower loopback sibling must not block that.
"""

from datetime import datetime, timezone

import pytest
from pydantic import BaseModel

from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.process.overwatch_flows import (
    EXTERNAL_TO_HIVE_FLOW,
    EXTERNAL_TO_KEEPSATS_FLOW,
    EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW,
)
from v4vapp_backend_v2.process.process_overwatch import (
    _REDIS_ACTIVE_KEY,
    _REDIS_COMPLETED_PREFIX,
    FlowEvent,
    FlowInstance,
    FlowStatus,
    Overwatch,
)

GROUP = "6K228qJsVqNoYCE3nKVYfCIzLEvYZ2uQ+t5vPRRKL5E="
SHORT = "6K228qJsVq"


class _Note(BaseModel):
    """Serializable stand-in for the Hive op that carries the customer id."""

    cust_id: str


OLD = datetime(2026, 10, 13, 13, 21, tzinfo=timezone.utc)
RESTART = datetime(2026, 10, 14, 8, 17, 53, tzinfo=timezone.utc)


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.strings: dict[str, str] = {}

    async def hset(self, name: str, key: str, value: str) -> None:
        self.hashes.setdefault(name, {})[key] = value

    async def hdel(self, name: str, key: str) -> None:
        self.hashes.get(name, {}).pop(key, None)

    async def hgetall(self, name: str) -> dict[str, str]:
        return dict(self.hashes.get(name, {}))

    async def set(self, name: str, value: str, ex: int | None = None) -> None:
        self.strings[name] = value

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.strings.pop(key, None)
            self.hashes.pop(key, None)


def _invoice() -> FlowEvent:
    return FlowEvent(
        event_type="op",
        timestamp=OLD,
        group_id=GROUP,
        short_id=SHORT,
        op_type="invoice",
    )


def _deposit() -> FlowEvent:
    return FlowEvent(
        event_type="ledger",
        timestamp=OLD,
        group_id=GROUP,
        short_id=SHORT,
        ledger_type=LedgerType.DEPOSIT_LIGHTNING,
    )


def _completed(definition, events: list[FlowEvent]) -> FlowInstance:
    return FlowInstance(
        flow_definition=definition,
        trigger_group_id=GROUP,
        trigger_short_id=SHORT,
        cust_id="privex",
        status=FlowStatus.COMPLETED,
        started_at=OLD,
        completed_at=OLD,
        events=events,
    )


def _ghost(definition) -> FlowInstance:
    return FlowInstance(
        flow_definition=definition,
        trigger_group_id=GROUP,
        trigger_short_id=SHORT,
        cust_id="",
        status=FlowStatus.IN_PROGRESS,
        started_at=OLD,
        events=[_invoice(), _deposit()],
    )


def _register() -> Overwatch:
    Overwatch.reset()
    ow = Overwatch()
    Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_HIVE_FLOW)
    Overwatch.register_flow(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW)
    Overwatch._loaded_from_redis = True
    return ow


def _block_redis(monkeypatch) -> None:
    """Stall cleanup deletes Redis fields. Tests must not touch a live server."""
    monkeypatch.setattr(Overwatch, "_redis", lambda self: None)


@pytest.mark.asyncio
async def test_restart_drops_empty_cust_snapshot_when_broader_sibling_completed(monkeypatch):
    """The 2/4 and 2/6 ghosts from 6K228qJsVq must not be marked stalled."""
    _block_redis(monkeypatch)
    ow = _register()
    ow.flow_instances.extend(
        [
            _completed(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW, [_invoice()]),
            _completed(EXTERNAL_TO_KEEPSATS_FLOW, [_invoice(), _deposit()]),
            _completed(EXTERNAL_TO_HIVE_FLOW, [_invoice(), _deposit()]),
            _ghost(EXTERNAL_TO_KEEPSATS_FLOW),
            _ghost(EXTERNAL_TO_HIVE_FLOW),
        ]
    )

    stalled = await ow.check_stalls(now=RESTART)

    assert stalled == []
    assert [f.cust_id for f in ow.flow_instances] == ["privex", "privex", "privex"]
    assert {f.status for f in ow.flow_instances} == {FlowStatus.COMPLETED}


@pytest.mark.asyncio
async def test_narrow_sibling_alone_does_not_drop_a_real_deposit(monkeypatch):
    """A loopback completion does not explain a lightning deposit."""
    _block_redis(monkeypatch)
    ow = _register()
    ghost = _ghost(EXTERNAL_TO_KEEPSATS_FLOW)
    ow.flow_instances.extend(
        [
            _completed(EXTERNAL_TO_KEEPSATS_LOOPBACK_FLOW, [_invoice()]),
            ghost,
        ]
    )

    stalled = await ow.check_stalls(now=RESTART)

    assert stalled == [ghost]
    assert ghost.status == FlowStatus.STALLED
    assert ghost in ow.flow_instances


@pytest.mark.asyncio
async def test_cust_id_change_removes_the_previous_redis_field(monkeypatch):
    """Filling in cust_id must not leave the empty-customer active field."""
    fake = _FakeRedis()
    monkeypatch.setattr(Overwatch, "_redis", lambda self: fake)
    ow = _register()
    try:
        await ow._try_create_flow(
            _invoice(),
            type(
                "FakeOp",
                (),
                {
                    "group_id": GROUP,
                    "short_id": SHORT,
                    "op_type": "invoice",
                    "from_account": "",
                },
            )(),
        )
        empty_key = f":{GROUP}:external_to_keepsats"
        assert empty_key in fake.hashes[_REDIS_ACTIVE_KEY]

        await ow._dispatch(
            FlowEvent(
                event_type="op",
                timestamp=OLD,
                group_id="gid_notif",
                short_id="3800_61dc3b_1",
                op_type="custom_json",
                op=_Note(cust_id="privex"),
            )
        )

        active = fake.hashes[_REDIS_ACTIVE_KEY]
        assert empty_key not in active
        assert f"privex:{GROUP}:external_to_keepsats" in active
        assert not any(
            key.removeprefix(_REDIS_COMPLETED_PREFIX).startswith(":") for key in fake.strings
        )
    finally:
        Overwatch.reset()
