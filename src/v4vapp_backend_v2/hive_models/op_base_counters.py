from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import ClassVar, Deque, Tuple

from nectar.hive import Hive

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import check_time_diff, format_time_delta
from v4vapp_backend_v2.hive.hive_extras import DEFAULT_GOOD_NODES, HIVE_BLOCK_TIME
from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm

TIME_DIFFERENCE_CHECK = timedelta(seconds=120)

BLOCK_MARKER_NORMAL_MIN = 1
BLOCK_MARKER_CATCHUP_MIN = 1


@dataclass
class OpInTrxCounter:
    """
    A class to track operation counts within transactions, with a shared history
    of the last 100 transaction IDs stored in a class-level deque.
    """

    op_in_trx: int = 1
    last_trx_id: str = ""
    last_block_num: int = 0
    realm: OpRealm = OpRealm.REAL

    # Class variables: Two separate deques for REAL and VIRTUAL transactions,
    # limited to 50 IDs each
    real_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)
    virtual_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)

    def op_in_trx_inc(self, op: OpBase) -> int:
        """
        Increment the operation counter based on the realm and transaction context.

        This method updates the operation counter (`op_in_trx`) for a given operation (`op`)
        depending on whether the operation belongs to the REAL or VIRTUAL realm. It also
        updates the last transaction ID (`last_trx_id`) and, for virtual operations, the
        last block number (`last_block_num`).

        Args:
            op (OpBase): The operation object containing realm, transaction ID, and block number.

        Returns:
            int: The updated operation counter (`op_in_trx`) for the current transaction.
        """
        if op.realm == OpRealm.REAL:
            if self.last_trx_id == op.trx_id:
                self.op_in_trx += 1
                op.op_in_trx = self.op_in_trx
            else:
                self.last_trx_id = op.trx_id
                self.op_in_trx = 1
                op.op_in_trx = 1
                # self.real_trx_id_stack.append(op.trx_id)
        elif op.realm == OpRealm.VIRTUAL:
            if self.last_block_num == op.block_num and self.last_trx_id == op.trx_id:
                self.op_in_trx += 1
                op.op_in_trx = self.op_in_trx
            else:
                self.last_trx_id = op.trx_id
                self.last_block_num = op.block_num
                self.op_in_trx = 1
                op.op_in_trx = 1
                # self.virtual_trx_id_stack.append(op.trx_id)
        return self.op_in_trx


@dataclass
class BlockCounter:
    last_good_block: int = 0
    current_block: int = 0
    block_count: int = 0
    event_count: int = 0
    last_event_count: int = 0
    hive_client: Hive = Hive(node=DEFAULT_GOOD_NODES[0])
    time_diff: timedelta = timedelta(seconds=0)
    running_time: float = 0
    error_code: str = ""
    id: str = ""
    next_marker: int = 0
    marker_point: int = int(30 * 60 / HIVE_BLOCK_TIME)  # 30 minutes in blocks
    icon: str = "🧱"
    last_marker: float = timer()
    start: float = 0

    def __post_init__(self):
        self.start = timer()
        if self.current_block == 0:
            self.current_block = self.last_good_block
        self.id = self.id + " " if self.id else ""

    def log_extra(self) -> dict:
        """
        Returns a dictionary containing the current state of the BlockCounter instance.

        This method is used for logging purposes to provide additional context about
        the current state of the BlockCounter.

        Returns:
            dict: A dictionary containing the current state of the BlockCounter instance.
        """
        rpc_url = (
            self.hive_client.rpc.url if self.hive_client and self.hive_client.rpc else "No RPC"
        )
        return {
            "last_good_block": self.last_good_block,
            "current_block": self.current_block,
            "block_count": self.block_count,
            "last_event_count": self.last_event_count,
            "event_count": self.event_count,
            "hive_client": rpc_url,
            "time_diff": str(self.time_diff),
            "running_time": str(self.running_time),
            "error_code": self.error_code,
            "id": self.id,
            "next_marker": self.next_marker,
            "marker_point": self.marker_point,
        }

    @property
    def is_catching_up(self) -> bool:
        """
        Determines if the BlockCounter is currently catching up based on the time difference.

        Returns:
            bool: True if the time difference exceeds the predefined threshold, indicating
                  that the BlockCounter is catching up; False otherwise.
        """
        return self.time_diff > TIME_DIFFERENCE_CHECK

    def inc(self, hive_event: dict, notification: bool = False) -> Tuple[bool, bool]:
        """
        Increment the block counter and handle marker updates based on the provided hive event.

        This method processes a hive event to update the current block, block count, and marker.
        It also logs relevant information and switches to the next RPC node if a marker is reached.

        Args:
            hive_event (dict): A dictionary containing details of the hive event. Expected keys:
                - "block_num" (int): The current block number.
                - "timestamp" (datetime, optional): The timestamp of the event. Defaults to the current UTC time.
            notification (bool, optional): Flag to indicate if a notification should be logged. Defaults to False.

        Returns:
            Tuple[bool, bool]: A tuple containing:
                - new_block (bool): True if the current block is greater than the last good block.
                - marker (bool): True if the block count has reached or exceeded the next marker.
        """

        self.current_block = hive_event.get("block_num", 0)
        new_block = False
        marker = False
        if self.last_good_block < self.current_block:
            timestamp = hive_event.get("timestamp", datetime.now(tz=timezone.utc))
            self.block_count += self.current_block - self.last_good_block
            self.last_good_block = self.current_block
            new_block = True
            if self.block_count >= self.next_marker:
                marker = True
                self.log_time_difference_errors(timestamp=timestamp)
                old_node = ""
                if self.hive_client and self.hive_client.rpc:
                    old_node = self.hive_client.rpc.url
                    self.hive_client.rpc.next()
                last_marker_time = timer() - self.last_marker
                last_marker_time_str = format_time_delta(last_marker_time)
                catch_up_in = format_time_delta(
                    self.time_diff.total_seconds()
                    / ((self.marker_point * HIVE_BLOCK_TIME) / last_marker_time)
                )
                self.time_diff = check_time_diff(timestamp)

                speed_up_factor = (
                    min(self.marker_point, self.block_count) * HIVE_BLOCK_TIME
                ) / last_marker_time

                self.marker_point = int(
                    (BLOCK_MARKER_CATCHUP_MIN * 60 / HIVE_BLOCK_TIME)
                    if self.is_catching_up
                    else (BLOCK_MARKER_NORMAL_MIN * 60 / HIVE_BLOCK_TIME)
                )
                self.next_marker += self.marker_point

                self.running_time = timer() - self.start
                rpc_url = (
                    str(self.hive_client.rpc.url)
                    if self.hive_client and self.hive_client.rpc
                    else "No RPC"
                )
                logger.info(
                    f"{self.icon} {self.id:>9}{self.block_count:,} "
                    f"time: {last_marker_time_str} "
                    f"block: {self.current_block:,} "
                    f"speed up: x{speed_up_factor:.2f} "
                    f"delta: {self.time_diff} catch up: {catch_up_in} "
                    f"running time: {format_time_delta(self.running_time)} "
                    f"events: {self.event_count - self.last_event_count:,} "
                    f"Node: {old_node} -> {rpc_url}",
                    extra={
                        "notification": notification,
                        "block_counter": self.log_extra(),
                    },
                )
                self.last_event_count = self.event_count
                self.last_marker = timer()
        self.event_count += 1
        return new_block, marker

    def log_time_difference_errors(
        self,
        timestamp: str | datetime,
    ):
        """
        Logs warnings based on the time difference between the provided timestamp and the
        current time.

        If the time difference exceeds a predefined threshold, an error code is generated
        and logged. If the time difference is within the threshold and an error code exists,
        the error code is cleared and logged.

        Args:
            timestamp (str | datetime): The timestamp to compare against the current time.
                Can be a string or a datetime object.
            error_code (str, optional): An existing error code to be cleared if the time
                difference is within the threshold. Defaults to an empty string.
            id (str, optional): An identifier for the error code. Defaults to an empty string.

        Returns:
            Tuple[str, timedelta]: A tuple containing the updated error code (or an empty
                string if cleared) and the calculated time difference.
        """
        self.time_diff = check_time_diff(timestamp)
        comparison_text = f"than {TIME_DIFFERENCE_CHECK}"

        if not self.error_code and self.time_diff > TIME_DIFFERENCE_CHECK:
            self.error_code = f"{self.id}Hive Time diff greater {comparison_text}"
            logger.warning(
                f"{self.icon} {self.id}Time diff: {self.time_diff} greater {comparison_text}",
                extra={
                    "notification": True,
                    "error_code": self.error_code,
                },
            )
        if self.error_code and self.time_diff <= TIME_DIFFERENCE_CHECK:
            logger.warning(
                f"{self.icon} {self.id}Time diff: {self.time_diff} less {comparison_text}",
                extra={
                    "notification": True,
                    "error_code_clear": self.error_code,
                },
            )
            self.error_code = ""
