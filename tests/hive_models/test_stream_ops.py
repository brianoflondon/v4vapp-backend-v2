import os
from datetime import timedelta
from pathlib import Path

import pytest

from tests.load_data import load_hive_events
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_base_counters import OpInTrxCounter
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes
from v4vapp_backend_v2.hive_models.stream_ops import (
    counted_virtual_op,
    stream_ops_async,
    virtual_blocks_to_fetch,
)


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


@pytest.mark.asyncio
async def test_stream_ops_async_live_from_hive():
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = []
    look_back = timedelta(seconds=10)

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(look_back=look_back, stop_now=True, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_stream_ops_async_live_from_hive_stop_now():
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = ["producer_reward"]
    look_back = timedelta(minutes=0.5)

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(look_back=look_back, stop_now=True, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


@pytest.mark.asyncio
async def test_stream_ops_block_range(mocker):
    """
    Test the stream_ops_async function to ensure it yields operations correctly.
    """
    opNames = ["update_proposal_votes"]

    fake_event = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    fake_event["block_num"] = 95_157_382

    fake_rpc = mocker.Mock(url="https://mock.hive.node")
    fake_rpc.next.return_value = None
    fake_rpc.timeout = 12  # fail-fast shape so _ensure_stream_hive keeps this client
    fake_rpc.nodes = None
    fake_hive = mocker.Mock(rpc=fake_rpc, keys=None)
    fake_hive.set_default_nodes.return_value = None

    fake_blockchain = mocker.Mock()
    fake_blockchain.get_current_block_num.return_value = 95_157_382

    def fake_stream(*args, **kwargs):
        if kwargs.get("only_virtual_ops"):
            return iter([])
        return iter([fake_event])

    fake_blockchain.stream.side_effect = fake_stream

    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.TrackedBaseModel.update_quote",
        return_value=None,
    )
    # Clients are built via make_stream_hive (not Hive() in this module).
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.make_stream_hive",
        return_value=fake_hive,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.close_hive_client",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_blockchain_instance",
        return_value=fake_blockchain,
    )

    # Call the async generator function and collect results
    results = []
    async for op in stream_ops_async(start=95_157_371, stop=95_157_382, opNames=opNames):
        print(op.log_str)
        results.append(op)

    # Check that the results are as expected
    assert len(results) > 0


def test_virtual_blocks_one_step_keeps_two_block_lag():
    """A step from 766 to 767 still fetches only 765."""
    assert list(virtual_blocks_to_fetch(766, 767)) == [765]


def test_virtual_blocks_gap_includes_block_just_left():
    """Order 1839180890: real stream jumped 111268746 → 111268748.

    Block 111268747 had no tracked real op. The fills live in 111268746.
    """
    assert list(virtual_blocks_to_fetch(111_268_746, 111_268_748)) == [
        111_268_745,
        111_268_746,
    ]


def test_virtual_blocks_wider_gap_stops_before_new_block():
    assert list(virtual_blocks_to_fetch(100, 105)) == [99, 100, 101, 102, 103]


def test_virtual_blocks_ignores_non_advancing_and_genesis():
    assert list(virtual_blocks_to_fetch(50, 50)) == []
    assert list(virtual_blocks_to_fetch(50, 40)) == []
    assert list(virtual_blocks_to_fetch(1, 2)) == []


def _stream_test_doubles(mocker, real_events):
    fake_rpc = mocker.Mock(url="https://mock.hive.node")
    fake_rpc.next.return_value = None
    fake_rpc.timeout = 12
    fake_rpc.nodes = None
    fake_hive = mocker.Mock(rpc=fake_rpc, keys=None)
    fake_hive.set_default_nodes.return_value = None

    virtual_starts: list[int] = []

    def fake_stream(*args, **kwargs):
        if kwargs.get("only_virtual_ops"):
            virtual_starts.append(kwargs["start"])
            return iter([])
        return iter(real_events)

    fake_blockchain = mocker.Mock()
    fake_blockchain.get_current_block_num.return_value = real_events[-1]["block_num"]
    fake_blockchain.stream.side_effect = fake_stream

    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.TrackedBaseModel.update_quote",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.make_stream_hive",
        return_value=fake_hive,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.close_hive_client",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_blockchain_instance",
        return_value=fake_blockchain,
    )
    return virtual_starts


@pytest.mark.asyncio
async def test_stream_fetches_virtuals_for_block_left_behind_by_a_gap(mocker):
    """A quiet block between two real ops must not drop the earlier block's virtuals."""
    base = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    first = base.copy()
    first["block_num"] = 100
    second = base.copy()
    second["block_num"] = 102
    virtual_starts = _stream_test_doubles(mocker, [first, second])

    results = []
    async for op in stream_ops_async(start=100, stop=102, opNames=["update_proposal_votes"]):
        results.append(op.block_num)

    assert results == [100, 102]
    assert virtual_starts == [99, 100]


@pytest.mark.asyncio
async def test_stream_one_block_step_fetches_only_the_lag_block(mocker):
    base = next(load_hive_events(OpTypes.UPDATE_PROPOSAL_VOTES)).copy()
    first = base.copy()
    first["block_num"] = 100
    second = base.copy()
    second["block_num"] = 101
    virtual_starts = _stream_test_doubles(mocker, [first, second])

    async for _op in stream_ops_async(start=100, stop=101, opNames=["update_proposal_votes"]):
        pass

    assert virtual_starts == [99]


def test_counted_virtual_op_includes_untracked_ops_in_the_index():
    """The index is the count of virtual ops with this trx id, not the tracked subset.

    Chain op_in_trx is not used. On a real transaction it also counts signed ops,
    which the per-block stream never did.
    """
    zero = "0000000000000000000000000000000000000000"
    counter = OpInTrxCounter()
    curation = OpBase(
        type="curation_reward",
        trx_id=zero,
        block_num=10,
        trx_num=4294967295,
        timestamp="2026-08-01T12:00:00",
    )
    reward = OpBase(
        type="producer_reward",
        trx_id=zero,
        block_num=10,
        trx_num=4294967295,
        timestamp="2026-08-01T12:00:00",
        op_in_trx=64,
    )
    assert counted_virtual_op(counter, curation, ["producer_reward"]) is None
    kept = counted_virtual_op(counter, reward, ["producer_reward"])
    assert kept is reward
    assert reward.op_in_trx == 2
    assert reward.group_id == f"10_{zero}_2_{reward.realm}"
    assert reward.group_id_query["op_in_trx"] == 2


def _raw_producer_reward(block_num: int) -> dict:
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
                "producer": "alice",
                "vesting_shares": {
                    "amount": "1000",
                    "precision": 6,
                    "nai": "@@000000037",
                },
            },
        },
    }


@pytest.mark.asyncio
async def test_catchup_virtuals_come_from_enum_virtual_ops(mocker):
    """Far behind head, virtual ops are read in one unfiltered range.

    The per-block virtual stream must not run, and the yielded order stays
    the same as the slow path: the real op, then virtuals for the lag blocks.
    An untracked curation reward before producer_reward still consumes an
    op_in_trx slot. The chain value on that reward (64) is not the id.
    """
    # all_ops_log.jsonl is not filtered by OpTypes, so build the real ops here.
    first = {
        "type": "update_proposal_votes",
        "approve": True,
        "extensions": [],
        "proposal_ids": [340],
        "voter": "detlev",
        "timestamp": "2026-08-01T12:00:00",
        "block_num": 100,
        "trx_num": 1,
        "trx_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    }
    second = first.copy()
    second["block_num"] = 102
    second["trx_id"] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    reward_99 = _raw_producer_reward(99)
    reward_99["op_in_trx"] = 64
    rewards = {99: reward_99, 100: _raw_producer_reward(100)}
    curation_99 = {
        "trx_id": "0000000000000000000000000000000000000000",
        "block": 99,
        "trx_in_block": 4294967295,
        "op_in_trx": 1,
        "virtual_op": True,
        "timestamp": "2026-08-01T12:00:00",
        "op": {"type": "curation_reward_operation", "value": {"curator": "bob"}},
    }

    def raw_ops_for(block_num: int) -> list[dict]:
        ops = []
        if block_num == 99:
            ops.append(curation_99)
        reward = rewards.get(block_num)
        if reward is not None:
            ops.append(reward)
        return ops

    class FakeRpc:
        url = "https://mock.hive.node"

        def __init__(self) -> None:
            self.enum_calls: list[dict] = []
            self.block_calls: list[int] = []

        def enum_virtual_ops(self, params: dict) -> dict:
            self.enum_calls.append(params)
            begin = params["block_range_begin"]
            end = params["block_range_end"]
            ops = []
            for block_num in range(begin, end):
                ops.extend(raw_ops_for(block_num))
            return {
                "ops": ops,
                "next_block_range_begin": end,
                "next_operation_begin": 0,
            }

        def get_ops_in_block(self, params: dict) -> dict:
            self.block_calls.append(params["block_num"])
            return {"ops": raw_ops_for(params["block_num"])}

    fake_rpc = FakeRpc()
    fake_hive = mocker.Mock(rpc=fake_rpc, keys=None)
    fake_hive.set_default_nodes.return_value = None

    def fake_stream(*args, **kwargs):
        if kwargs.get("only_virtual_ops"):
            raise AssertionError("catch-up must not open a per-block virtual stream")
        return iter([first, second])

    fake_blockchain = mocker.Mock()
    fake_blockchain.get_current_block_num.return_value = 20_000
    fake_blockchain.stream.side_effect = fake_stream

    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.TrackedBaseModel.update_quote",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.make_stream_hive",
        return_value=fake_hive,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.close_hive_client",
        return_value=None,
    )
    mocker.patch(
        "v4vapp_backend_v2.hive_models.stream_ops.get_blockchain_instance",
        return_value=fake_blockchain,
    )

    results = []
    async for op in stream_ops_async(
        start=100,
        stop=102,
        opNames=["update_proposal_votes", "producer_reward"],
        hive=fake_hive,
    ):
        producer = op.producer if op.op_type == "producer_reward" else None
        results.append((op.op_type, op.block_num, producer, op.op_in_trx))

    assert results == [
        ("update_proposal_votes", 100, None, 1),
        ("producer_reward", 99, "alice", 2),
        ("producer_reward", 100, "alice", 1),
        ("update_proposal_votes", 102, None, 1),
    ]
    assert len(fake_rpc.enum_calls) == 1
    assert fake_rpc.enum_calls[0]["block_range_begin"] == 99
    assert "filter" not in fake_rpc.enum_calls[0]
    assert 99 in fake_rpc.block_calls
