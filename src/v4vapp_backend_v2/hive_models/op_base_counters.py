from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from timeit import default_timer as timer
from typing import ClassVar, Deque, Tuple

from nectar import Hive

from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.general_purpose_funcs import check_time_diff, format_time_delta
from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm

TIME_DIFFERENCE_CHECK = timedelta(seconds=120)


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

    def inc2(self, op: OpBase) -> int:
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

    def inc(self, trx_id: str) -> int:
        """
        Increment the operation count for a given transaction ID and return the count.
        If the transaction ID is new, reset the instance's count and add it to the shared stack.
        If it matches the instance's last transaction ID or is in the stack, increment the count.

        NOTE: this assumes transactions are presented sequentially from the Hive blockchain in
        sequence as they appear in blocks and that the same transaction ID will not be presented
        in different blocks.

        Args:
            trx_id (str): The transaction ID to process.

        Returns:
            int: The current operation count for the transaction in this instance.
        """
        # Case 1: Same transaction as last time for this instance, just increment
        if trx_id == "0000000000000000000000000000000000000000":
            self.last_trx_id = trx_id
            self.op_in_trx = 1
            return self.op_in_trx

        if self.realm == OpRealm.REAL and self.last_trx_id == trx_id:
            self.op_in_trx += 1
            return self.op_in_trx

        # Case 2: Transaction exists in the shared stack,
        # update instance's last_trx_id and increment
        if self.realm == OpRealm.REAL:
            use_stack = OpInTrxCounter.real_trx_id_stack
        else:  # OpRealVirtual.VIRTUAL
            use_stack = OpInTrxCounter.virtual_trx_id_stack

        if trx_id in use_stack:
            self.last_trx_id = trx_id
            self.op_in_trx += 1
            return self.op_in_trx

        # Case 3: New transaction, reset instance count and add to shared stack
        use_stack.append(trx_id)  # Access class variable
        self.last_trx_id = trx_id
        self.op_in_trx = 1  # Reset count for new transaction in this instance
        return self.op_in_trx


@dataclass
class BlockCounter:
    last_good_block: int = 0
    current_block: int = 0
    block_count: int = 0
    hive_client: Hive = Hive()
    time_diff: timedelta = timedelta(seconds=0)
    error_code: str = ""
    id: str = ""
    next_marker: int = 0
    marker_point: int = 100  # 100 blocks is 300s 5 minutes
    icon: str = "🧱"
    last_marker = timer()

    def __post_init__(self):
        if self.current_block == 0:
            self.current_block = self.last_good_block
        self.id = self.id + " " if self.id else ""

    def inc(self, hive_event: dict, notification: bool = False) -> Tuple[bool, bool]:
        """
        Increment the block count and update the current block number.
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
                self.next_marker += self.marker_point
                marker = True
                self.log_time_difference_errors(timestamp=timestamp)
                old_node = self.hive_client.rpc.url
                self.hive_client.rpc.next()
                last_marker_time = format_time_delta(timer() - self.last_marker)
                logger.info(
                    f"{self.icon} {self.id:>9}{self.block_count:,} "
                    f"blocks processed in: {last_marker_time} "
                    f"delta: {self.time_diff} "
                    f"Node: {old_node} -> {self.hive_client.rpc.url}",
                    extra={
                        "notification": notification,
                        "time_diff": self.time_diff,
                        "block_count": self.block_count,
                    },
                )
                self.last_marker = timer()
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
