from decimal import Decimal
from typing import List

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent


async def process_forward(
    tracked_forward_event: TrackedForwardEvent,
) -> List[LedgerEntry]:
    """
    Marks the given TrackedForwardEvent as included on the ledger by updating its
    `included_on_ledger` field to True and setting the `ledger_entry_id`.

    Args:
        tracked_forward_event (TrackedForwardEvent): The HTLC forward event to update.

    Returns:
        None
    """

    total_fee, pending_for_ledger = await TrackedForwardEvent.pending_for_ledger_inclusion()
    for event in pending_for_ledger:
        logger.info(event.log_str, extra={"notification": False, **event.log_extra})
    if total_fee > Decimal("500"):
        pass

    return []
