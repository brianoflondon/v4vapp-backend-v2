import asyncio
import json
from collections.abc import AsyncGenerator
from typing import List, cast

import websockets
import websockets.exceptions
from websockets.asyncio.client import connect as ws_connect
from websockets.typing import Subprotocol

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.magi.magi_balances import ICON, MAGI_ENDPOINTS
from v4vapp_backend_v2.magi.magi_classes import MagiBTCTransferEvent

TRANSFER_STREAM_QUERY = """subscription {
  btc_mapping_transfer_events_stream(
    batch_size: 10,
    cursor: {initial_value: {indexer_id: %d}, ordering: ASC}
  ) {
    from_addr
    to_addr
    amount
    indexer_block_height
    indexer_tx_hash
    indexer_ts
    indexer_id
  }
}"""

MAX_RECONNECT_ATTEMPTS = 10
BASE_RECONNECT_DELAY = 1.0  # seconds
MAX_RECONNECT_DELAY = 60.0  # seconds


def _http_to_ws(url: str) -> str:
    return url.replace("https://", "wss://", 1).replace("http://", "ws://", 1)


async def _connect_and_stream(
    ws_url: str,
    from_indexer_id: int,
) -> AsyncGenerator[MagiBTCTransferEvent, None]:
    """Single connection attempt: connect, handshake, subscribe, and yield events."""
    query = TRANSFER_STREAM_QUERY % from_indexer_id
    async with ws_connect(
        ws_url,
        subprotocols=cast(list[Subprotocol], ["graphql-transport-ws"]),
    ) as ws:
        # 1. Init — loop until we get connection_ack, responding to pings
        await ws.send(json.dumps({"type": "connection_init", "payload": {}}))
        while True:
            ack = json.loads(await ws.recv())
            ack_type = ack.get("type")
            if ack_type == "connection_ack":
                break
            elif ack_type == "ping":
                await ws.send(json.dumps({"type": "pong"}))
            else:
                raise RuntimeError(f"Expected connection_ack, got: {ack}")

        # 2. Subscribe
        await ws.send(
            json.dumps({
                "id": "1",
                "type": "subscribe",
                "payload": {"query": query},
            })
        )

        # 3. Yield events
        async for raw in ws:
            msg = json.loads(raw)
            msg_type = msg.get("type")

            if msg_type == "next":
                events = (
                    msg
                    .get("payload", {})
                    .get("data", {})
                    .get("btc_mapping_transfer_events_stream", [])
                )
                for event in events:
                    transfer = MagiBTCTransferEvent(**event)
                    logger.info(transfer.log_str)
                    yield transfer

            elif msg_type == "complete":
                logger.info(f"{ICON} MAGI transfer stream completed")
                return

            elif msg_type == "error":
                raise RuntimeError(f"GraphQL error: {msg.get('payload')}")

            elif msg_type == "ping":
                await ws.send(json.dumps({"type": "pong"}))


async def stream_magi_transfer_events(
    endpoint: str | None = None,
    from_indexer_id: int = 0,
    max_reconnect_attempts: int = MAX_RECONNECT_ATTEMPTS,
    watch_accounts: List[str] | None = None,
) -> AsyncGenerator[MagiBTCTransferEvent, None]:
    """Stream BTC transfer events from a MAGI GraphQL WebSocket endpoint.

    Automatically reconnects on disconnection, resuming from the last seen
    indexer_id. Raises after max_reconnect_attempts consecutive failures.

    Set from_indexer_id to the latest known indexer_id to only receive new events.
    If endpoint is None, uses the first endpoint from MAGI_ENDPOINTS.
    """
    if endpoint is None:
        endpoint = MAGI_ENDPOINTS[0]

    ws_url = _http_to_ws(endpoint)
    cursor = from_indexer_id
    consecutive_failures = 0

    while True:
        logger.info(
            f"{ICON} Connecting to MAGI transfer stream at {ws_url} "
            f"from indexer_id={cursor} (attempt {consecutive_failures + 1})"
        )
        try:
            async for transfer in _connect_and_stream(ws_url, cursor):
                consecutive_failures = 0  # reset on successful data
                cursor = transfer.indexer_id  # advance cursor so reconnect resumes here
                if watch_accounts:
                    if (
                        AccName(transfer.to_addr).magi_prefix in watch_accounts
                        or AccName(transfer.from_addr).magi_prefix in watch_accounts
                    ):
                        yield transfer
                else:
                    yield transfer
            # clean 'complete' — stop iterating
            return

        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
        ) as exc:
            consecutive_failures += 1
            if consecutive_failures > max_reconnect_attempts:
                logger.error(
                    f"{ICON} MAGI stream: exceeded {max_reconnect_attempts} reconnect attempts",
                    extra={"error": str(exc)},
                )
                raise
            delay = min(
                BASE_RECONNECT_DELAY * (2 ** (consecutive_failures - 1)), MAX_RECONNECT_DELAY
            )
            logger.warning(
                f"{ICON} MAGI stream disconnected ({exc}); "
                f"reconnecting in {delay:.1f}s (attempt {consecutive_failures}/{max_reconnect_attempts})"
            )
            await asyncio.sleep(delay)


async def main_test():
    endpoint = MAGI_ENDPOINTS[0]  # https://magi-api.v4v.app/hasura/v1/graphql
    print(f"Streaming transfer events from {endpoint}")
    count = 0
    try:
        async for event in stream_magi_transfer_events(
            endpoint=endpoint, from_indexer_id=0, watch_accounts=["hive:devser.v4vapp"]
        ):
            print(event.log_str)

    except KeyboardInterrupt:
        print(f"\nStopped. {count} matching events seen.")


if __name__ == "__main__":
    asyncio.run(main_test())
