from collections import deque
from dataclasses import dataclass
from typing import ClassVar, Deque

from v4vapp_backend_v2.hive_models.op_base import OpRealm


@dataclass
class OpInTrxCounter:
    """
    A class to track operation counts within transactions, with a shared history
    of the last 100 transaction IDs stored in a class-level deque.
    """

    op_in_trx: int = 1
    last_trx_id: str = ""
    realm: OpRealm = OpRealm.REAL

    # Class variables: Two separate deques for REAL and VIRTUAL transactions,
    # limited to 50 IDs each
    real_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)
    virtual_trx_id_stack: ClassVar[Deque[str]] = deque(maxlen=50)

    def inc(self, trx_id: str) -> int:
        """
        Increment the operation count for a given transaction ID and return the count.
        If the transaction ID is new, reset the instance's count and add it to the shared stack.
        If it matches the instance's last transaction ID or is in the stack, increment the count.

        Args:
            trx_id (str): The transaction ID to process.

        Returns:
            int: The current operation count for the transaction in this instance.
        """
        # Case 1: Same transaction as last time for this instance, just increment
        if trx_id == "0000000000000000000000000000000000000000":
            self.op_in_trx = 1
            return self.op_in_trx

        if self.last_trx_id == trx_id:
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
