"""Compare catch-up virtual-op dicts with hive-nectar's Blockchain.stream.

Catch-up (more than 50 blocks behind head) no longer asks hive-nectar to
flatten virtual operations. ``normalize_virtual_op`` in
``src/v4vapp_backend_v2/hive_models/virtual_ops_range.py`` does that, then
``op_any_or_base`` builds the same models as the live path. Real operations,
and virtual operations within 50 blocks of head, still come from
``Blockchain.stream``.

This script checks that the hand-written flattener matches what nectar
emits. It talks to a live Hive node. It is not part of pytest.

What is compared
----------------
For each block in a historical range, nectar is asked for every virtual
operation (``opNames=[]``), not only the five types the monitor stores.

1. ``account_history_api.get_ops_in_block`` is the raw wrapper nectar's
   virtual stream reads. Every op in that response is passed through
   ``normalize_virtual_op``, including types that are not in ``OP_TRACKED``
   and types that are not listed in ``HIVE_VIRTUAL_OPS``. That list is
   compared with ``Blockchain.stream(..., only_virtual_ops=True)``.

2. ``account_history_api.enum_virtual_ops`` is called with no bitmask, so
   the node returns every virtual op in the range. Those are normalized the
   same way and compared with the same nectar stream. The call is paginated.

A match is the same operations, in the same order, with the same values for
``type``, the operation fields (amounts stay ``{amount, precision, nai}``
dicts), ``_id`` (nectar's ``Blockchain.hash_op``), ``timestamp`` (nectar's
``parse_time``), ``block_num``, ``trx_num`` (from ``trx_in_block``), and
``trx_id``.

Neither side copies the chain's ``op_in_trx``. ``OpInTrxCounter`` assigns
that after the model is built, on both paths.

Usage
-----
From the repository root:

    uv run python scripts/compare_virtual_op_normalize.py
    uv run python scripts/compare_virtual_op_normalize.py --blocks 10
    uv run python scripts/compare_virtual_op_normalize.py --start 111284000 --blocks 20
    uv run python scripts/compare_virtual_op_normalize.py --node https://api.hive.blog

``--start`` defaults to ``head - --blocks - 200`` so the sample is not in
the reversible window. Exit code 0 when every op matches. Exit code 1
after the first difference, which is printed with the keys that differ.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

from nectar.blockchain import Blockchain
from nectar.hive import Hive

from v4vapp_backend_v2.hive_models.virtual_ops_range import normalize_virtual_op

ENUM_PAGE_LIMIT = 10_000
ENUM_MAX_PAGES = 50


def _short(value: Any) -> str:
    text = repr(value)
    if len(text) > 180:
        return text[:177] + "..."
    return text


def _diff_lines(nectar_op: dict[str, Any], our_op: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for key in sorted(set(nectar_op) | set(our_op)):
        left = nectar_op.get(key)
        right = our_op.get(key)
        if left != right:
            lines.append(f"    {key}: nectar={_short(left)}")
            lines.append(f"    {key}: ours  ={_short(right)}")
    return lines


def _raw_op_type(raw: Any) -> str | None:
    if not isinstance(raw, dict):
        return None
    wrapped = raw.get("op")
    if not isinstance(wrapped, dict):
        return None
    op_type = wrapped.get("type")
    if not isinstance(op_type, str):
        return None
    return op_type.removesuffix("_operation")


def _normalize_all(raw_ops: list[Any]) -> list[dict[str, Any]]:
    """Flatten every well-formed op, not only types this app stores."""
    names = {op_type for raw in raw_ops if (op_type := _raw_op_type(raw))}
    events: list[dict[str, Any]] = []
    for raw in raw_ops:
        event = normalize_virtual_op(raw, names)
        if event is not None:
            events.append(event)
    return events


def _stream_block(chain: Blockchain, block_num: int) -> list[dict[str, Any]]:
    # An empty opNames list makes Blockchain.stream yield every virtual op.
    return list(
        chain.stream(
            opNames=[],
            start=block_num,
            stop=block_num,
            only_virtual_ops=True,
            max_batch_size=None,
            threading=False,
            raw_ops=False,
        )
    )


def _enum_all_virtual_ops(hive: Hive, begin: int, end: int) -> list[Any]:
    """Every virtual op in ``[begin, end)``. No operation bitmask."""
    cursor = begin
    operation_begin = 0
    raw_ops: list[Any] = []
    for _page in range(ENUM_MAX_PAGES):
        result = hive.rpc.enum_virtual_ops(
            {
                "block_range_begin": cursor,
                "block_range_end": end,
                "operation_begin": operation_begin,
                "limit": ENUM_PAGE_LIMIT,
                "include_reversible": True,
                "group_by_block": False,
            }
        )
        if not isinstance(result, dict):
            raise TypeError(f"enum_virtual_ops returned {type(result).__name__}")
        page = result.get("ops") or []
        if not isinstance(page, list):
            raise TypeError("enum_virtual_ops ops is not a list")
        raw_ops.extend(page)
        next_begin = int(result.get("next_block_range_begin", end))
        next_operation = int(result.get("next_operation_begin") or 0)
        if next_begin >= end:
            return raw_ops
        if next_begin == cursor and next_operation == operation_begin:
            raise RuntimeError(f"enum_virtual_ops made no progress at block {cursor:,}")
        if not page and next_operation == 0:
            raise RuntimeError(f"enum_virtual_ops stopped early at block {cursor:,}")
        cursor = next_begin
        operation_begin = next_operation
    raise RuntimeError(
        f"enum_virtual_ops pagination exceeded {ENUM_MAX_PAGES} pages "
        f"for blocks {begin:,}–{end - 1:,}"
    )


def _report_mismatch(
    label: str,
    block_num: int,
    nectar_ops: list[dict[str, Any]],
    our_ops: list[dict[str, Any]],
) -> bool:
    """Print the first difference. Return True when the lists differ."""
    if len(nectar_ops) != len(our_ops):
        print(
            f"{label} block {block_num:,}: count nectar={len(nectar_ops)} ours={len(our_ops)}",
            file=sys.stderr,
        )
        return True
    for index, (nectar_op, our_op) in enumerate(zip(nectar_ops, our_ops)):
        lines = _diff_lines(nectar_op, our_op)
        if not lines:
            continue
        op_type = our_op.get("type") or nectar_op.get("type")
        print(
            f"{label} block {block_num:,} op {index} {op_type} differs:",
            file=sys.stderr,
        )
        print("\n".join(lines), file=sys.stderr)
        return True
    return False


def compare(node: str, start: int | None, blocks: int) -> int:
    hive = Hive(
        node=[node],
        timeout=20,
        num_retries=3,
        num_retries_call=2,
        monitor_interval=0,
    )
    chain = Blockchain(blockchain_instance=hive)
    head = int(hive.get_dynamic_global_properties()["head_block_number"])
    begin = start if start is not None else head - blocks - 200
    end = begin + blocks
    if begin < 1 or end <= begin:
        print(f"Bad range {begin}..{end} (head {head:,}).", file=sys.stderr)
        return 1

    print(
        f"Node {node}\n"
        f"Head {head:,}\n"
        f"Blocks {begin:,}–{end - 1:,} ({blocks} blocks)\n"
        "Comparing every virtual op, not only the types the monitor stores."
    )

    try:
        enum_raw = _enum_all_virtual_ops(hive, begin, end)
    except (TypeError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    enum_by_block: dict[int, list[dict[str, Any]]] = {}
    for event in _normalize_all(enum_raw):
        enum_by_block.setdefault(event["block_num"], []).append(event)

    nectar_ops_total = 0
    type_counts: dict[str, int] = {}
    for block_num in range(begin, end):
        nectar_ops = _stream_block(chain, block_num)
        raw = hive.rpc.get_ops_in_block({"block_num": block_num, "only_virtual": True})
        raw_ops = raw.get("ops") or [] if isinstance(raw, dict) else []
        ours = _normalize_all(raw_ops)
        nectar_ops_total += len(nectar_ops)
        for op in nectar_ops:
            op_type = str(op.get("type"))
            type_counts[op_type] = type_counts.get(op_type, 0) + 1
        if _report_mismatch("get_ops_in_block", block_num, nectar_ops, ours):
            print(
                f"Stopped after {nectar_ops_total:,} nectar ops. "
                "normalize_virtual_op does not match Blockchain.stream.",
                file=sys.stderr,
            )
            return 1

        ours_enum = enum_by_block.get(block_num, [])
        if _report_mismatch("enum_virtual_ops", block_num, nectar_ops, ours_enum):
            print(
                "Stopped. enum_virtual_ops + normalize_virtual_op does not match "
                "Blockchain.stream.",
                file=sys.stderr,
            )
            return 1
        print(f"  {block_num:,}  virtual {len(nectar_ops):>4}")

    type_list = ", ".join(f"{name} {count}" for name, count in sorted(type_counts.items()))
    print(
        f"Match. {nectar_ops_total:,} virtual ops across {blocks} blocks "
        f"are identical to Blockchain.stream.\n"
        f"Types: {type_list}"
    )
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--node",
        default="https://api.hive.blog",
        help="Hive RPC node (default: https://api.hive.blog)",
    )
    parser.add_argument(
        "--blocks",
        type=int,
        default=20,
        help="How many historical blocks to compare (default: 20)",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=None,
        help="First block. Default is head - blocks - 200.",
    )
    args = parser.parse_args()
    if args.blocks < 1:
        print("--blocks must be at least 1", file=sys.stderr)
        sys.exit(1)
    sys.exit(compare(args.node, args.start, args.blocks))


if __name__ == "__main__":
    main()
