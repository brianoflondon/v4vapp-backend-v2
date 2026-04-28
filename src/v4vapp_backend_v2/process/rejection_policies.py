# WORK IN PROGRESS - DO NOT USE OR TEST YET

from typing import Callable

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger

type RejectionPolicyFn = Callable[[TrackedAny], bool]


async def existing_ledger_entry(tracked_op: TrackedAny) -> bool:
    """
    Rejection policy that checks if a ledger entry already exists for the tracked object.
    If a ledger entry exists, the event will be rejected to prevent duplicate processing.
    """
    existing_entry = await LedgerEntry.load(group_id=tracked_op.group_id_p)
    if existing_entry:
        logger.warning(
            f"Ledger entry for {tracked_op.short_id} already exists. {existing_entry.group_id}",
            extra={"notification": False},
        )
        return True
    return False
