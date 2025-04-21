from collections import deque
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

from v4vapp_backend_v2.hive_models.op_base import OpRealm
from v4vapp_backend_v2.hive_models.op_base_counters import (
    TIME_DIFFERENCE_CHECK,
    BlockCounter,
    OpInTrxCounter,
)


# Fixture to reset class-level stacks before each test
@pytest.fixture(autouse=True)
def reset_stacks():
    OpInTrxCounter.real_trx_id_stack = deque(maxlen=50)
    OpInTrxCounter.virtual_trx_id_stack = deque(maxlen=50)


def test_initial_state():
    counter = OpInTrxCounter()
    assert counter.op_in_trx == 1
    assert counter.last_trx_id == ""
    assert counter.realm == OpRealm.REAL
    assert len(OpInTrxCounter.real_trx_id_stack) == 0
    assert len(OpInTrxCounter.virtual_trx_id_stack) == 0


# Mock Hive class
class MockHive:
    def __init__(self):
        self.rpc = Mock(url="node1", next=Mock(return_value=None))


@pytest.fixture
def mock_dependencies():
    with (
        patch("v4vapp_backend_v2.hive_models.op_base_counters.Hive", MockHive),
        patch(
            "v4vapp_backend_v2.hive_models.op_base_counters.check_time_diff",
        ) as mock_check_time_diff,
        patch("v4vapp_backend_v2.hive_models.op_base_counters.logger") as mock_logger,
    ):
        mock_check_time_diff.return_value = timedelta(seconds=5)
        yield mock_check_time_diff, mock_logger


@pytest.fixture
def block_counter():
    return BlockCounter(last_good_block=100, id="test")


def test_post_init_default_current_block():
    counter = BlockCounter(last_good_block=50)
    assert counter.current_block == 50  # Set to last_good_block if 0
    assert counter.id == ""  # Empty id gets a space
    counter = BlockCounter(id="some_id")
    assert counter.id == "some_id "


def test_post_init_with_current_block():
    counter = BlockCounter(last_good_block=50, current_block=60, id="test")
    assert counter.current_block == 60  # Retains provided value
    assert counter.id == "test "  # Appends space to id


def test_inc_new_block_and_marker(mock_dependencies):
    mock_check_time_diff, mock_logger = mock_dependencies
    counter = BlockCounter(last_good_block=100, hive_client=MockHive())
    hive_event = {"block_num": 101, "timestamp": "2025-04-09T12:00:00"}

    new_block, marker = counter.inc(hive_event)
    assert new_block is True  # New block detected
    assert marker is True  # marker
    assert counter.current_block == 101
    assert counter.last_good_block == 101
    assert counter.block_count == 1
    assert mock_logger.info.call_count == 1
    assert counter.hive_client.rpc.next.call_count == 1


def test_inc_same_block(mock_dependencies):
    mock_check_time_diff, mock_logger = mock_dependencies
    counter = BlockCounter(last_good_block=100, current_block=100)
    hive_event = {"block_num": 100, "timestamp": "2025-04-09T12:00:00"}

    new_block, marker = counter.inc(hive_event)
    assert new_block is False  # Same block, no increment
    assert marker is False
    assert counter.block_count == 0
    assert mock_logger.info.call_count == 0


def test_log_time_diff_exceeds_threshold(mock_dependencies):
    mock_check_time_diff, mock_logger = mock_dependencies
    mock_check_time_diff.return_value = TIME_DIFFERENCE_CHECK + timedelta(
        seconds=15
    )  # Exceeds threshold
    counter = BlockCounter(id="test_block_counter")

    counter.log_time_difference_errors("2025-04-09T12:00:00")
    assert counter.time_diff == TIME_DIFFERENCE_CHECK + timedelta(seconds=15)
    assert "test_block_counter" in counter.error_code


def test_log_time_diff_within_threshold_clears_error(mock_dependencies):
    mock_check_time_diff, mock_logger = mock_dependencies
    mock_check_time_diff.return_value = timedelta(seconds=5)  # Within threshold
    counter = BlockCounter(
        id="test_block_counter", error_code="testHive Time diff greater than 10 s"
    )

    counter.log_time_difference_errors("2025-04-09T12:00:00")
    assert counter.time_diff == timedelta(seconds=5)
    assert counter.error_code == ""  # Cleared
    assert mock_logger.warning.call_count == 1
    assert "test_block_counter" in mock_logger.warning.call_args.args[0]  # Check if id is included


def test_log_time_diff_no_change(mock_dependencies):
    mock_check_time_diff, mock_logger = mock_dependencies
    mock_check_time_diff.return_value = timedelta(seconds=5)  # Within threshold
    counter = BlockCounter(id="test")

    counter.log_time_difference_errors("2025-04-09T12:00:00")
    assert counter.time_diff == timedelta(seconds=5)
    assert counter.error_code == ""  # No error to clear, no new error
    assert mock_logger.warning.call_count == 0  # No log triggered
