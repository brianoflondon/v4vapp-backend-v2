"""Tests for stream_magi.py — covers handshake, streaming, reconnection."""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest
import websockets.exceptions
from mongomock_motor import AsyncMongoMockClient

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.magi.magi_classes import DB_MAGI_BTC_COLLECTION, MagiBTCTransferEvent
from v4vapp_backend_v2.magi.stream_magi import (
    _connect_and_stream,
    _http_to_ws,
    stream_magi_transfer_events,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_EVENT = {
    "from_addr": "hive:alice",
    "to_addr": "hive:bob",
    "amount": "5000",
    "indexer_block_height": 1000,
    "indexer_tx_hash": "abc123",
    "indexer_ts": "2026-04-22T10:00:00",
    "indexer_id": 42,
}

SAMPLE_EVENT_2 = {**SAMPLE_EVENT, "indexer_id": 43, "amount": "1000"}


def _next_msg(events: list[dict]) -> str:
    return json.dumps({
        "type": "next",
        "payload": {"data": {"btc_mapping_transfer_events_stream": events}},
    })


def _complete_msg() -> str:
    return json.dumps({"type": "complete"})


def _ping_msg() -> str:
    return json.dumps({"type": "ping", "payload": {"message": "keepalive"}})


def _ack_msg() -> str:
    return json.dumps({"type": "connection_ack"})


def _error_msg(message: str) -> str:
    return json.dumps({"type": "error", "payload": [{"message": message}]})


class MockWebSocket:
    """A minimal async-iterable WebSocket mock."""

    def __init__(self, recv_side_effect: list[str], stream_messages: list[str]):
        self._recv = AsyncMock(side_effect=recv_side_effect)
        self._stream_messages = iter(stream_messages)
        self.send = AsyncMock()

    async def recv(self) -> str:
        return await self._recv()

    def __aiter__(self):
        return self

    async def __anext__(self) -> str:
        try:
            return next(self._stream_messages)
        except StopIteration:
            raise StopAsyncIteration

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------


def test_http_to_ws_https():
    assert _http_to_ws("https://example.com/graphql") == "wss://example.com/graphql"


def test_http_to_ws_http():
    assert _http_to_ws("http://localhost:8081/v1/graphql") == "ws://localhost:8081/v1/graphql"


def test_http_to_ws_already_ws():
    assert _http_to_ws("ws://example.com/graphql") == "ws://example.com/graphql"


def test_transfer_event_model():
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    assert event.indexer_id == 42
    assert event.amount == Decimal("5000")
    assert "alice" in event.log_str
    assert "bob" in event.log_str


# ---------------------------------------------------------------------------
# Async tests: _connect_and_stream
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_and_stream_simple():
    """Normal flow: ack → one batch of events → complete."""
    ws = MockWebSocket(
        recv_side_effect=[_ack_msg()],
        stream_messages=[_next_msg([SAMPLE_EVENT, SAMPLE_EVENT_2]), _complete_msg()],
    )

    with patch("v4vapp_backend_v2.magi.stream_magi.ws_connect", return_value=ws):
        events = [e async for e in _connect_and_stream("wss://example.com/graphql", 0)]

    assert len(events) == 2
    assert events[0].indexer_id == 42
    assert events[1].indexer_id == 43


@pytest.mark.asyncio
async def test_connect_and_stream_ping_before_ack():
    """Server sends ping before connection_ack — must handle gracefully."""
    ws = MockWebSocket(
        recv_side_effect=[_ping_msg(), _ack_msg()],
        stream_messages=[_next_msg([SAMPLE_EVENT]), _complete_msg()],
    )

    with patch("v4vapp_backend_v2.magi.stream_magi.ws_connect", return_value=ws):
        events = [e async for e in _connect_and_stream("wss://example.com/graphql", 0)]

    assert len(events) == 1
    sent_messages = [json.loads(call.args[0]) for call in ws.send.call_args_list]
    assert any(m["type"] == "pong" for m in sent_messages)


@pytest.mark.asyncio
async def test_connect_and_stream_graphql_error():
    """GraphQL error message should raise RuntimeError."""
    ws = MockWebSocket(
        recv_side_effect=[_ack_msg()],
        stream_messages=[_error_msg("permission denied")],
    )

    with patch("v4vapp_backend_v2.magi.stream_magi.ws_connect", return_value=ws):
        with pytest.raises(RuntimeError, match="GraphQL error"):
            async for _ in _connect_and_stream("wss://example.com/graphql", 0):
                pass


@pytest.mark.asyncio
async def test_connect_and_stream_ping_during_stream():
    """Ping received during streaming (not during handshake) is handled."""
    ws = MockWebSocket(
        recv_side_effect=[_ack_msg()],
        stream_messages=[_ping_msg(), _next_msg([SAMPLE_EVENT]), _complete_msg()],
    )

    with patch("v4vapp_backend_v2.magi.stream_magi.ws_connect", return_value=ws):
        events = [e async for e in _connect_and_stream("wss://example.com/graphql", 0)]

    assert len(events) == 1
    sent = [json.loads(c.args[0]) for c in ws.send.call_args_list]
    assert any(m["type"] == "pong" for m in sent)


# ---------------------------------------------------------------------------
# Async tests: stream_magi_transfer_events (reconnection logic)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_reconnects_on_disconnect(mocker):
    """ConnectionClosed triggers a reconnect that resumes from last indexer_id."""
    call_count = 0

    async def mock_connect_and_stream(ws_url: str, from_indexer_id: int):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            yield MagiBTCTransferEvent(**SAMPLE_EVENT)
            raise websockets.exceptions.ConnectionClosed(None, None)
        else:
            assert from_indexer_id == 42, "should resume from last seen indexer_id"
            yield MagiBTCTransferEvent(**SAMPLE_EVENT_2)
            return

    mocker.patch(
        "v4vapp_backend_v2.magi.stream_magi._connect_and_stream",
        side_effect=mock_connect_and_stream,
    )
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    events = []
    async for event in stream_magi_transfer_events(
        endpoint="https://example.com/graphql",
        from_indexer_id=0,
        max_reconnect_attempts=3,
    ):
        events.append(event)

    assert len(events) == 2
    assert events[0].indexer_id == 42
    assert events[1].indexer_id == 43
    assert call_count == 2


@pytest.mark.asyncio
async def test_stream_raises_after_max_reconnects(mocker):
    """Exceeding max_reconnect_attempts re-raises the last exception."""

    async def mock_connect_and_stream(ws_url: str, from_indexer_id: int):
        raise websockets.exceptions.ConnectionClosed(None, None)
        yield  # make it an async generator

    mocker.patch(
        "v4vapp_backend_v2.magi.stream_magi._connect_and_stream",
        side_effect=mock_connect_and_stream,
    )
    mocker.patch("asyncio.sleep", new_callable=AsyncMock)

    with pytest.raises(websockets.exceptions.ConnectionClosed):
        async for _ in stream_magi_transfer_events(
            endpoint="https://example.com/graphql",
            from_indexer_id=0,
            max_reconnect_attempts=2,
        ):
            pass


@pytest.mark.asyncio
async def test_stream_uses_default_endpoint(mocker):
    """When no endpoint is given, uses MAGI_ENDPOINTS[0]."""
    seen_urls: list[str] = []

    async def mock_connect_and_stream(ws_url: str, from_indexer_id: int):
        seen_urls.append(ws_url)
        return
        yield  # async generator

    mocker.patch(
        "v4vapp_backend_v2.magi.stream_magi._connect_and_stream",
        side_effect=mock_connect_and_stream,
    )

    async for _ in stream_magi_transfer_events():
        pass

    assert len(seen_urls) == 1
    assert seen_urls[0].startswith("ws")


SAMPLE_EVENT_OTHER = {
    "from_addr": "hive:carol",
    "to_addr": "hive:dave",
    "amount": "250",
    "indexer_block_height": 1001,
    "indexer_tx_hash": "def456",
    "indexer_ts": "2026-04-22T11:00:00",
    "indexer_id": 44,
}


@pytest.mark.asyncio
async def test_stream_watch_accounts_filters_events(mocker):
    """Only events involving a watched account are yielded; others are dropped."""

    async def mock_connect_and_stream(ws_url: str, from_indexer_id: int):
        # SAMPLE_EVENT: alice -> bob  (bob is watched)
        yield MagiBTCTransferEvent(**SAMPLE_EVENT)
        # SAMPLE_EVENT_OTHER: carol -> dave  (neither watched)
        yield MagiBTCTransferEvent(**SAMPLE_EVENT_OTHER)
        return

    mocker.patch(
        "v4vapp_backend_v2.magi.stream_magi._connect_and_stream",
        side_effect=mock_connect_and_stream,
    )

    events = []
    async for event in stream_magi_transfer_events(
        endpoint="https://example.com/graphql",
        from_indexer_id=0,
        watch_accounts=["hive:bob"],
    ):
        events.append(event)

    assert len(events) == 1
    assert events[0].indexer_id == 42  # only alice->bob passed through


@pytest.mark.asyncio
async def test_stream_watch_accounts_matches_from_addr(mocker):
    """A watched account in from_addr also matches."""

    async def mock_connect_and_stream(ws_url: str, from_indexer_id: int):
        yield MagiBTCTransferEvent(**SAMPLE_EVENT)  # alice -> bob
        yield MagiBTCTransferEvent(**SAMPLE_EVENT_2)  # alice -> bob (different id)
        return

    mocker.patch(
        "v4vapp_backend_v2.magi.stream_magi._connect_and_stream",
        side_effect=mock_connect_and_stream,
    )

    events = []
    async for event in stream_magi_transfer_events(
        endpoint="https://example.com/graphql",
        from_indexer_id=0,
        watch_accounts=["hive:alice"],  # sender, not receiver
    ):
        events.append(event)

    assert len(events) == 2  # both events have alice as sender


# ---------------------------------------------------------------------------
# MagiBTCTransferEvent TrackedBaseModel integration
# ---------------------------------------------------------------------------


def test_transfer_event_collection_name():
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    assert event.collection_name == DB_MAGI_BTC_COLLECTION


def test_transfer_event_op_in_trx_no_suffix():
    """indexer_tx_hash with no suffix → op_in_trx = 1."""
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)  # hash = "abc123"
    assert event.op_in_trx == 1


def test_transfer_event_op_in_trx_suffix_zero():
    """Suffix -0 (first of several ops in same tx) → op_in_trx = 1."""
    event = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_tx_hash": "abc123-0"})
    assert event.op_in_trx == 1


def test_transfer_event_op_in_trx_suffix_one():
    """Suffix -1 → op_in_trx = 2."""
    event = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_tx_hash": "abc123-1"})
    assert event.op_in_trx == 2


def test_transfer_event_group_id():
    """group_id follows the OpBase pattern: block_height_txhash_op_in_trx_real."""
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)  # hash = "abc123", height = 1000
    expected = f"{SAMPLE_EVENT['indexer_block_height']}_abc123_1_real"
    assert event.group_id == expected
    assert event.group_id_p == expected


def test_transfer_event_group_id_with_suffix():
    """group_id strips -N from tx hash and reflects correct op_in_trx."""
    event = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_tx_hash": "abc123-1"})
    expected = f"{SAMPLE_EVENT['indexer_block_height']}_abc123_2_real"
    assert event.group_id == expected
    assert event.group_id_p == expected


def test_transfer_event_group_id_query():
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    assert event.group_id_query == {"indexer_id": SAMPLE_EVENT["indexer_id"]}


def test_transfer_event_tracked_base_fields():
    """TrackedBaseModel extra fields are present with None defaults."""
    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    assert event.replies is None
    assert event.conv is None
    assert event.fee_conv is None


def test_transfer_event_link_strips_suffix():
    """link property strips the trailing -<n> from indexer_tx_hash."""
    event = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_tx_hash": "abc123-1"})
    assert event.link == "https://hivehub.dev/tx/abc123"


def test_transfer_event_link_no_suffix():
    """link property works when no suffix is present."""
    event = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "indexer_tx_hash": "abc123"})
    assert event.link == "https://hivehub.dev/tx/abc123"


# ---------------------------------------------------------------------------
# MagiBTCTransferEvent.save() — uses mongomock
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_transfer_event_save_and_retrieve(mocker):
    """save() upserts the event into magi_btc; the document can be retrieved."""
    mock_client = AsyncMongoMockClient()
    mock_db = mock_client["test_db"]
    mocker.patch.object(InternalConfig, "db", mock_db)

    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    await event.save()

    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({
        "indexer_id": SAMPLE_EVENT["indexer_id"]
    })
    assert doc is not None
    assert doc["indexer_id"] == SAMPLE_EVENT["indexer_id"]
    # amount is stored as float (convert_decimals_for_mongodb converts Decimal → float)
    assert float(doc["amount"]) == float(SAMPLE_EVENT["amount"])


@pytest.mark.asyncio
async def test_transfer_event_save_upsert(mocker):
    """Second save with updated amount overwrites the existing document."""
    mock_client = AsyncMongoMockClient()
    mock_db = mock_client["test_db"]
    mocker.patch.object(InternalConfig, "db", mock_db)

    event = MagiBTCTransferEvent(**SAMPLE_EVENT)
    await event.save()

    updated = MagiBTCTransferEvent(**{**SAMPLE_EVENT, "amount": "9999"})
    await updated.save()

    # Only one document should exist
    count = await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({
        "indexer_id": SAMPLE_EVENT["indexer_id"]
    })
    assert count == 1

    doc = await mock_db[DB_MAGI_BTC_COLLECTION].find_one({
        "indexer_id": SAMPLE_EVENT["indexer_id"]
    })
    assert float(doc["amount"]) == 9999.0


@pytest.mark.asyncio
async def test_transfer_event_save_multiple_ids(mocker):
    """Different indexer_ids produce separate documents."""
    mock_client = AsyncMongoMockClient()
    mock_db = mock_client["test_db"]
    mocker.patch.object(InternalConfig, "db", mock_db)

    for raw in [SAMPLE_EVENT, SAMPLE_EVENT_2]:
        await MagiBTCTransferEvent(**raw).save()

    count = await mock_db[DB_MAGI_BTC_COLLECTION].count_documents({})
    assert count == 2
