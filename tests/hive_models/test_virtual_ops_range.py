"""Unit tests for the catch-up enum_virtual_ops path. No live Hive calls."""

from v4vapp_backend_v2.hive_models.op_base_extras import OP_TRACKED
from v4vapp_backend_v2.hive_models.virtual_ops_range import (
    VirtualOpRangeFetcher,
    normalize_virtual_op,
    virtual_op_filter_mask,
    virtual_op_identity,
)

# Hive enum_vops_filter bits. These are chain constants, not derived from our map,
# so a wrong protocol id in HIVE_VIRTUAL_OPS fails the mask test.
_FILL_ORDER = 0x80
_PRODUCER_REWARD = 0x4000
_FILL_RECURRENT_TRANSFER = 0x2_0000_0000
_LIMIT_ORDER_CANCELLED = 0x8_0000_0000
_PRODUCER_MISSED = 0x10_0000_0000
_TRACKED_VIRTUAL_MASK = (
    _FILL_ORDER
    | _PRODUCER_REWARD
    | _FILL_RECURRENT_TRANSFER
    | _LIMIT_ORDER_CANCELLED
    | _PRODUCER_MISSED
)


def _raw_reward(block_num: int, producer: str = "alice") -> dict:
    return {
        "trx_id": "0000000000000000000000000000000000000000",
        "block": block_num,
        "trx_in_block": 4294967295,
        "op_in_trx": 1,
        "virtual_op": True,
        "timestamp": "2026-08-01T12:00:00",
        "op": {
            "type": "producer_reward_operation",
            "value": {
                "producer": producer,
                "vesting_shares": {
                    "amount": "1000",
                    "precision": 6,
                    "nai": "@@000000037",
                },
            },
        },
    }


class _Rpc:
    def __init__(self, ops_by_block: dict[int, list[dict]], *, pages: list[dict] | None = None):
        self.ops_by_block = ops_by_block
        self.pages = pages
        self.enum_calls: list[dict] = []
        self.block_calls: list[int] = []

    def enum_virtual_ops(self, params: dict) -> dict:
        self.enum_calls.append(params)
        if self.pages is not None:
            return self.pages.pop(0)
        begin = params["block_range_begin"]
        end = params["block_range_end"]
        ops = []
        for block_num in range(begin, end):
            ops.extend(self.ops_by_block.get(block_num, []))
        return {
            "ops": ops,
            "next_block_range_begin": end,
            "next_operation_begin": 0,
        }

    def get_ops_in_block(self, params: dict) -> dict:
        block_num = params["block_num"]
        self.block_calls.append(block_num)
        return {"ops": list(self.ops_by_block.get(block_num, []))}


def test_tracked_virtual_mask_matches_hive_enum_vops_filter():
    assert virtual_op_filter_mask(OP_TRACKED) == _TRACKED_VIRTUAL_MASK
    assert virtual_op_filter_mask(["transfer", "custom_json"]) == 0
    assert virtual_op_filter_mask(["not_a_hive_op"]) is None


def test_normalize_strips_operation_suffix_and_keeps_stream_fields():
    event = normalize_virtual_op(_raw_reward(111), {"producer_reward"})
    assert event is not None
    assert event["type"] == "producer_reward"
    assert event["block_num"] == 111
    assert event["trx_num"] == 4294967295
    assert event["producer"] == "alice"
    assert event["timestamp"].tzinfo is not None
    assert normalize_virtual_op(_raw_reward(111), {"fill_order"}) is None


def test_range_matches_get_ops_in_block_then_serves_from_cache():
    rpc = _Rpc({99: [_raw_reward(99)], 100: [_raw_reward(100, producer="bob")]})
    fetcher = VirtualOpRangeFetcher(["producer_reward", "transfer"])
    first = fetcher.events_for(rpc, 99, 101, head=20_000, near_head_blocks=50)
    assert first is not None
    events, served_end = first
    assert served_end == 101
    assert [op["block_num"] for op in events] == [99, 100]
    assert [op["producer"] for op in events] == ["alice", "bob"]
    assert rpc.block_calls == [99]
    assert len(rpc.enum_calls) == 1
    # No type bitmask. op_in_trx counts untracked virtual ops too.
    assert "filter" not in rpc.enum_calls[0]
    assert rpc.enum_calls[0]["block_range_begin"] == 99

    second = fetcher.events_for(rpc, 100, 101, head=20_000, near_head_blocks=50)
    assert second is not None
    cached, cached_end = second
    assert cached_end == 101
    assert [op["producer"] for op in cached] == ["bob"]
    assert len(rpc.enum_calls) == 1


def test_pagination_follows_next_block_range_begin():
    # The fetcher asks for a 200-block window, then follows the resume cursor.
    page_one = {
        "ops": [_raw_reward(10)],
        "next_block_range_begin": 11,
        "next_operation_begin": 4,
    }
    page_two = {
        "ops": [_raw_reward(11)],
        "next_block_range_begin": 210,
        "next_operation_begin": 0,
    }
    rpc = _Rpc({10: [_raw_reward(10)]}, pages=[page_one, page_two])
    fetcher = VirtualOpRangeFetcher(["producer_reward"])
    served = fetcher.events_for(rpc, 10, 12, head=10_000, near_head_blocks=50)
    assert served is not None
    events, served_end = served
    assert served_end == 12
    assert [op["block_num"] for op in events] == [10, 11]
    assert rpc.enum_calls[1]["operation_begin"] == 4
    assert rpc.enum_calls[1]["block_range_begin"] == 11


def test_verify_mismatch_disables_fast_path():
    class MismatchRpc(_Rpc):
        def get_ops_in_block(self, params: dict) -> dict:
            self.block_calls.append(params["block_num"])
            return {"ops": [_raw_reward(params["block_num"], producer="other")]}

    mismatch = MismatchRpc({50: [_raw_reward(50)]})
    fetcher = VirtualOpRangeFetcher(["producer_reward"])
    assert fetcher.events_for(mismatch, 50, 51, head=10_000, near_head_blocks=50) is None
    assert fetcher.disabled is True
    assert fetcher.events_for(mismatch, 50, 51, head=10_000, near_head_blocks=50) is None
    assert len(mismatch.enum_calls) == 1


def test_near_head_stays_on_the_slow_path():
    rpc = _Rpc({100: [_raw_reward(100)]})
    fetcher = VirtualOpRangeFetcher(["producer_reward"])
    assert fetcher.events_for(rpc, 100, 101, head=140, near_head_blocks=50) is None
    assert rpc.enum_calls == []
    assert fetcher.disabled is False


def _raw_curation(block_num: int) -> dict:
    return {
        "trx_id": "0000000000000000000000000000000000000000",
        "block": block_num,
        "trx_in_block": 4294967295,
        "op_in_trx": 1,
        "virtual_op": True,
        "timestamp": "2026-08-01T12:00:00",
        "op": {
            "type": "curation_reward_operation",
            "value": {"curator": "bob"},
        },
    }


def test_untracked_virtual_ops_stay_in_the_range_for_op_in_trx():
    """Curation rewards are not stored, but they share the zero trx id.

    Dropping them here is what made catch-up assign a different op_in_trx
    from the per-block stream.
    """
    reward = _raw_reward(10)
    reward["op_in_trx"] = 64
    rpc = _Rpc({10: [_raw_curation(10), reward]})
    fetcher = VirtualOpRangeFetcher(["producer_reward"])
    served = fetcher.events_for(rpc, 10, 11, head=10_000, near_head_blocks=50)
    assert served is not None
    events, _served_end = served
    assert [op["type"] for op in events] == ["curation_reward", "producer_reward"]
    assert "op_in_trx" not in events[1]
    assert "filter" not in rpc.enum_calls[0]


def test_identity_ignores_dict_key_order():
    left = normalize_virtual_op(_raw_reward(5), {"producer_reward"})
    swapped = _raw_reward(5)
    shares = swapped["op"]["value"]["vesting_shares"]
    swapped["op"]["value"]["vesting_shares"] = {
        "nai": shares["nai"],
        "precision": shares["precision"],
        "amount": shares["amount"],
    }
    right = normalize_virtual_op(swapped, {"producer_reward"})
    assert left is not None and right is not None
    assert virtual_op_identity(left) == virtual_op_identity(right)
