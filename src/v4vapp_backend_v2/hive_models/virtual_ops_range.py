"""Catch-up virtual operations via ``account_history_api.enum_virtual_ops``.

Nectar's ``Blockchain.stream`` loads virtual ops with one ``get_ops_in_block``
per block, and each of those calls also reads the head block. On a public node
that is about two round trips per block (about 9s for 20 blocks).

``enum_virtual_ops`` returns a block range in one call. hive-nectar already
routes that method name to ``account_history_api`` but does not use it while
streaming. This module calls it directly and normalizes each op to the same
dict ``Blockchain.stream`` yields, so the rest of the monitor is unchanged.

The call is not filtered by op type. ``op_in_trx`` (and therefore ``group_id``)
counts every virtual op that shares a transaction id, including types this app
does not store. A bitmask would drop those and give catch-up a different id
from the per-block stream. The bitmask helper remains only as a gate: an
unknown op name keeps this fetcher off. A window is used only after its first
block matches ``get_ops_in_block``. Callers keep the per-block stream near head
and after this fetcher disables itself.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from nectar.blockchain import Blockchain
from nectar.utils import parse_time

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive_models.real_virtual_ops import HIVE_REAL_OPS, HIVE_VIRTUAL_OPS

ICON = "🔗"

# fill_convert_request_operation is protocol id 57 and enum_vops_filter bit 0.
FIRST_VIRTUAL_OP_ID = 57
# JSON numbers are exact only up to 2^53. Higher filter bits cannot be sent safely.
JSON_SAFE_FILTER_BITS = 53

VIRTUAL_WINDOW_BLOCKS = 200
VIRTUAL_ENUM_LIMIT = 10_000
VIRTUAL_ENUM_MAX_PAGES = 20
# How many leading blocks to compare with get_ops_in_block before trusting a window.
VERIFY_BLOCKS = 3


@dataclass
class VirtualOpWindow:
    begin: int
    end: int
    by_block: dict[int, list[dict[str, Any]]] = field(default_factory=dict)


def virtual_op_filter_mask(op_names: list[str]) -> int | None:
    """Bitmask for ``enum_virtual_ops``, or ``None`` when it would be unsafe.

    ``0`` means the caller asked for no known virtual ops. The fetcher then
    stays off and the per-block stream runs as before. ``None`` means some
    name is neither a known real op nor a known virtual op, or its bit cannot
    be sent as a JSON number.
    """
    mask = 0
    for name in op_names:
        if name in HIVE_REAL_OPS:
            continue
        if name not in HIVE_VIRTUAL_OPS:
            return None
        bit = HIVE_VIRTUAL_OPS[name] - FIRST_VIRTUAL_OP_ID
        if bit < 0 or bit >= JSON_SAFE_FILTER_BITS:
            return None
        mask |= 1 << bit
    return mask


def normalize_virtual_op(raw: dict[str, Any], op_names: set[str] | None) -> dict[str, Any] | None:
    """Flatten one ``enum_virtual_ops`` / ``get_ops_in_block`` wrapper.

    The shape matches ``Blockchain.stream`` (non-raw): operation fields plus
    ``type``, ``_id``, ``timestamp``, ``block_num``, ``trx_num``, and ``trx_id``.
    The chain's ``op_in_trx`` is left off. The stream assigns that later by
    counting virtual ops, and for a real transaction the chain value also
    counts the signed ops. ``op_names`` drops types that are not in the set.
    ``None`` keeps every well-formed op, which is what the catch-up counter
    has to see.
    """
    if not isinstance(raw, dict):
        return None
    wrapped = raw.get("op")
    if not isinstance(wrapped, dict):
        return None
    op_type = wrapped.get("type")
    value = wrapped.get("value")
    if not isinstance(op_type, str) or not isinstance(value, dict):
        return None
    op_type = op_type.removesuffix("_operation")
    if op_names is not None and op_type not in op_names:
        return None
    block_num = raw.get("block", raw.get("block_num"))
    if not isinstance(block_num, int):
        return None
    timestamp = raw.get("timestamp")
    if isinstance(timestamp, str):
        try:
            timestamp = parse_time(timestamp)
        except (TypeError, ValueError):
            pass
    event: dict[str, Any] = {"type": op_type}
    event.update(value.copy())
    event.update(
        {
            "_id": Blockchain.hash_op(wrapped),
            "timestamp": timestamp,
            "block_num": block_num,
            "trx_num": raw.get("trx_in_block", 0),
            "trx_id": raw.get("trx_id", "") or "",
        }
    )
    return event


def virtual_op_identity(event: dict[str, Any]) -> str:
    """Stable text for comparing a fast-path op with ``get_ops_in_block``."""
    payload = {
        key: value
        for key, value in event.items()
        if key not in {"_id", "timestamp", "block_num", "trx_num", "trx_id", "type"}
    }
    timestamp = event.get("timestamp")
    if isinstance(timestamp, datetime):
        timestamp = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    body = {
        "type": event.get("type"),
        "trx_id": event.get("trx_id"),
        "block_num": event.get("block_num"),
        "trx_num": event.get("trx_num"),
        "_id": event.get("_id"),
        "timestamp": timestamp,
        "payload": payload,
    }
    return json.dumps(body, sort_keys=True, default=str)


class VirtualOpRangeFetcher:
    """Fetch and cache every virtual op for blocks far behind head.

    ``events_for`` returns ``None`` when the caller must use the per-block
    stream. A returned ``(events, next_block)`` pair covers ``[begin, next_block)``
    only. The caller repeats until the requested range is done so one timeout
    cannot span an unbounded gap.
    """

    def __init__(self, op_names: list[str]) -> None:
        self.op_names = set(op_names)
        self.mask = virtual_op_filter_mask(op_names)
        self.window: VirtualOpWindow | None = None
        self.disabled = False
        self.failures = 0
        self._disable_logged = False
        self._announced = False

    def disable(self, reason: str) -> None:
        self.disabled = True
        self.window = None
        if self._disable_logged:
            return
        self._disable_logged = True
        logger.warning(
            f"{ICON} enum_virtual_ops catch-up disabled ({reason}); "
            "per-block virtual ops for the rest of this stream",
            extra={"notification": False},
        )

    def note_failure(self, reason: str) -> None:
        self.failures += 1
        logger.warning(
            f"{ICON} enum_virtual_ops failed ({reason}); failure {self.failures}/2",
            extra={"notification": False},
        )
        if self.failures >= 2:
            self.disable(reason)

    def events_for(
        self,
        rpc: Any,
        begin: int,
        end: int,
        head: int,
        near_head_blocks: int,
    ) -> tuple[list[dict[str, Any]], int] | None:
        """Ops in ``[begin, served_end)``, or ``None`` to use the slow path."""
        try:
            return self._events_for(rpc, begin, end, head, near_head_blocks)
        except Exception as exc:
            # Any RPC or shape error falls back to the per-block stream.
            self.note_failure(f"{type(exc).__name__}: {exc}")
            return None

    def _events_for(
        self,
        rpc: Any,
        begin: int,
        end: int,
        head: int,
        near_head_blocks: int,
    ) -> tuple[list[dict[str, Any]], int] | None:
        if self.disabled or not self.mask or begin >= end:
            return None
        safe_end = head - near_head_blocks
        if begin >= safe_end or end > safe_end:
            return None

        if self.window is not None and self.window.begin <= begin < self.window.end:
            served_end = min(end, self.window.end)
            return self._collect(begin, served_end), served_end

        window_end = min(begin + VIRTUAL_WINDOW_BLOCKS, safe_end)
        if window_end <= begin:
            return None
        window = self._fetch_window(rpc, begin, window_end)
        if not self._verify_window(rpc, window):
            self.disable(f"verify mismatch at block {window.begin:,}")
            return None
        self.failures = 0
        self.window = window
        if not self._announced:
            self._announced = True
            logger.info(
                f"{ICON} Catch-up virtual ops via enum_virtual_ops "
                f"blocks {window.begin:,}–{window.end - 1:,} "
                "(every virtual op; tracked types are kept after op_in_trx) "
                "instead of per-block get_ops_in_block",
                extra={"notification": False},
            )
        served_end = min(end, window.end)
        return self._collect(begin, served_end), served_end

    def _collect(self, begin: int, end: int) -> list[dict[str, Any]]:
        if self.window is None:
            return []
        events: list[dict[str, Any]] = []
        for block_num in range(begin, end):
            events.extend(self.window.by_block.get(block_num, []))
        return events

    def _fetch_window(self, rpc: Any, begin: int, end: int) -> VirtualOpWindow:
        by_block: dict[int, list[dict[str, Any]]] = {}
        cursor = begin
        operation_begin = 0
        for _page in range(VIRTUAL_ENUM_MAX_PAGES):
            # No ``filter``. The bitmask would omit untracked virtual ops, and
            # op_in_trx counts those. group_id would then differ from the
            # per-block stream. self.mask only decides whether this path is safe
            # to turn on.
            result = rpc.enum_virtual_ops(
                {
                    "block_range_begin": cursor,
                    "block_range_end": end,
                    "operation_begin": operation_begin,
                    "limit": VIRTUAL_ENUM_LIMIT,
                    "include_reversible": True,
                    "group_by_block": False,
                }
            )
            if not isinstance(result, dict):
                raise TypeError(f"enum_virtual_ops returned {type(result).__name__}")
            raw_ops = result.get("ops") or []
            if not isinstance(raw_ops, list):
                raise TypeError("enum_virtual_ops ops is not a list")
            for raw in raw_ops:
                event = normalize_virtual_op(raw, None)
                if event is None:
                    continue
                by_block.setdefault(event["block_num"], []).append(event)
            next_begin = int(result.get("next_block_range_begin", end))
            next_operation = int(result.get("next_operation_begin") or 0)
            if next_begin >= end:
                return VirtualOpWindow(begin=begin, end=end, by_block=by_block)
            if next_begin == cursor and next_operation == operation_begin:
                raise RuntimeError(f"enum_virtual_ops made no progress at block {cursor:,}")
            if not raw_ops and next_operation == 0:
                raise RuntimeError(f"enum_virtual_ops stopped early at block {cursor:,}")
            cursor = next_begin
            operation_begin = next_operation
        raise RuntimeError(
            f"enum_virtual_ops pagination exceeded {VIRTUAL_ENUM_MAX_PAGES} pages "
            f"for blocks {begin:,}–{end - 1:,}"
        )

    def _verify_window(self, rpc: Any, window: VirtualOpWindow) -> bool:
        """True when the full virtual-op range matches ``get_ops_in_block`` on a sample."""
        # One matching block is enough. Scan a few in case the first is empty.
        for block_num in range(window.begin, min(window.end, window.begin + VERIFY_BLOCKS)):
            slow = self._slow_block_events(rpc, block_num)
            fast = window.by_block.get(block_num, [])
            if not slow and not fast:
                continue
            if [virtual_op_identity(op) for op in slow] != [
                virtual_op_identity(op) for op in fast
            ]:
                logger.warning(
                    f"{ICON} enum_virtual_ops mismatch at block {block_num:,}: "
                    f"get_ops_in_block={len(slow)} enum={len(fast)}",
                    extra={"notification": False},
                )
                return False
            return True
        logger.warning(
            f"{ICON} enum_virtual_ops returned no tracked ops in the first "
            f"{VERIFY_BLOCKS} blocks from {window.begin:,}",
            extra={"notification": False},
        )
        return False

    def _slow_block_events(self, rpc: Any, block_num: int) -> list[dict[str, Any]]:
        result = rpc.get_ops_in_block({"block_num": block_num, "only_virtual": True})
        if isinstance(result, dict):
            raw_ops = result.get("ops") or []
        elif isinstance(result, list):
            raw_ops = result
        else:
            raise TypeError(f"get_ops_in_block returned {type(result).__name__}")
        events: list[dict[str, Any]] = []
        for raw in raw_ops:
            event = normalize_virtual_op(raw, None)
            if event is not None:
                events.append(event)
        return events
