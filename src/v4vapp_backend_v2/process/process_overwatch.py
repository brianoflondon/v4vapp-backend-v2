from dataclasses import dataclass, field
from timeit import default_timer as timer
from typing import List

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger

ICON = "📒"


@dataclass
class OverwatchLogEntry:
    ledger_entry: LedgerEntry | None = None
    op: TrackedAny | None = None
    overwatch_id: str = ""
    start_time: float = field(default_factory=timer)
    end_time: float | None = None
    duration: float | None = None
    status: str = "processing"

    def __init__(
        self, ledger_entry: LedgerEntry | None = None, op: TrackedAny | None = None
    ) -> None:
        self.ledger_entry = ledger_entry
        self.op = op
        self.overwatch_id = ""
        self.start_time = timer()
        self.end_time = None
        self.duration = None
        self.status = "processing"


@dataclass
class OverwatchLog:
    entries: List[OverwatchLogEntry] = field(default_factory=list)

    def add_entry(self, entry: OverwatchLogEntry):
        self.entries.append(entry)


overwatch_log = OverwatchLog()


async def overwatch_ledger_entry(ledger_entry: LedgerEntry) -> None:
    """Process a ledger entry by running sanity checks and logging the results.

    Args:
        ledger_entry: The LedgerEntry object to be processed.
    """
    overwatch_log.add_entry(OverwatchLogEntry(ledger_entry=ledger_entry))
    logger.info(
        f"{ICON} {ledger_entry.short_id} {ledger_entry.ledger_type_str}",
        extra={"notification": False},
    )


async def overwatch_op(op: TrackedAny) -> None:
    """Process a TrackedAny object by running sanity checks and logging the results.

    Args:
        op: The TrackedAny object to be processed.
    """
    overwatch_log.add_entry(OverwatchLogEntry(op=op))
    logger.info(
        f"{ICON} {op.short_id} {op.op_type}",
        extra={"notification": False},
    )
