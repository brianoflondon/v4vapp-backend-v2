"""
Overwatch System: End-to-end transaction flow tracking.

This module provides a framework for defining expected transaction flows,
recording events as they occur, and verifying flow completeness. It supports:

- FlowStage: An expected event (ledger entry or operation) in a flow.
- FlowDefinition: A blueprint for a complete transaction flow.
- FlowEvent: A recorded event that has occurred.
- FlowInstance: A tracked flow instance that collects events and checks completeness.
- OverwatchLog: Central registry managing flow instances and incoming events.

Flow definitions describe what ledger entries and operations are expected for a
particular transaction type (e.g., Hive-to-Keepsats conversion). Events are fed
into the system and matched to flow instances. Completeness can be checked at any
time against the flow definition.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
from timeit import default_timer as timer
from typing import Any, ClassVar, List, Literal

from pydantic import BaseModel, Field

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger

ICON = "📒"


# ---------------------------------------------------------------------------
# Flow status enum
# ---------------------------------------------------------------------------


class FlowStatus(StrEnum):
    """Status of a flow instance."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    STALLED = "stalled"


# ---------------------------------------------------------------------------
# Flow definition models
# ---------------------------------------------------------------------------


class FlowStage(BaseModel):
    """An expected event within a flow definition.

    A stage can represent either a ledger entry (identified by ledger_type)
    or an operation (identified by op_type). Stages are matched against
    incoming FlowEvents to determine flow progress.
    """

    name: str = Field(..., description="Human-readable stage name")
    event_type: Literal["ledger", "op"] = Field(
        ..., description="Whether this stage expects a ledger entry or an operation"
    )
    ledger_type: LedgerType | None = Field(
        None, description="Expected ledger type (for ledger events)"
    )
    op_type: str | None = Field(None, description="Expected operation type (for op events)")
    required: bool = Field(True, description="Whether this stage must be fulfilled")
    group: str = Field(
        "primary",
        description=(
            "Stage group: 'primary' for events sharing the trigger's short_id, "
            "or a descriptive name for reply-linked events"
        ),
    )

    def matches(self, event: FlowEvent) -> bool:
        """Check if an event fulfils this stage.

        Ledger events are matched solely by ledger_type (which is unique
        across stages). Op events are matched by op_type **and** group so
        that two stages with the same op_type (e.g. trigger_transfer vs
        change_transfer_op, both "transfer") are disambiguated.
        """
        if self.event_type != event.event_type:
            return False
        if self.event_type == "ledger":
            return self.ledger_type is not None and self.ledger_type == event.ledger_type
        # op event: must also match group to disambiguate
        return (
            self.op_type is not None
            and self.op_type == event.op_type
            and self.group == event.group
        )


class FlowDefinition(BaseModel):
    """Blueprint describing the expected stages of a complete transaction flow.

    A flow definition is a named template listing every FlowStage that should
    occur for a particular transaction type. FlowInstances are validated
    against a FlowDefinition to determine completeness.
    """

    name: str = Field(..., description="Flow type identifier (e.g. 'hive_to_keepsats')")
    description: str = Field("", description="Human-readable description")
    trigger_op_type: str = Field(..., description="The op_type that initiates this flow")
    stages: List[FlowStage] = Field(
        default_factory=list, description="Ordered list of expected stages"
    )

    @property
    def required_stages(self) -> List[FlowStage]:
        """Return only the stages marked as required."""
        return [s for s in self.stages if s.required]

    @property
    def stage_names(self) -> List[str]:
        """Return names of all stages."""
        return [s.name for s in self.stages]


# ---------------------------------------------------------------------------
# Flow event and instance models
# ---------------------------------------------------------------------------


class FlowEvent(BaseModel):
    """A recorded event within a flow instance.

    Each event wraps either a LedgerEntry or a TrackedAny operation along
    with identifiers used for matching (group_id, short_id, ledger_type).
    """

    event_type: Literal["ledger", "op"] = Field(..., description="Type of event")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When the event was recorded",
    )
    ledger_entry: LedgerEntry | None = Field(
        None, description="The ledger entry if event_type='ledger'"
    )
    op: Any = Field(None, description="The TrackedAny operation if event_type='op'")
    group_id: str = Field("", description="Group ID from the event source")
    short_id: str = Field("", description="Short ID from the event source")
    ledger_type: LedgerType | None = Field(None, description="Ledger type if applicable")
    op_type: str | None = Field(None, description="Operation type if applicable")
    group: str = Field(
        "primary",
        description="Which group this event belongs to (primary or a reply group name)",
    )

    @classmethod
    def from_ledger_entry(
        cls,
        ledger_entry: LedgerEntry,
        group: str = "primary",
    ) -> FlowEvent:
        """Create a FlowEvent from a LedgerEntry."""
        return cls(
            event_type="ledger",
            timestamp=ledger_entry.timestamp,
            ledger_entry=ledger_entry,
            op=None,
            group_id=ledger_entry.group_id,
            short_id=ledger_entry.short_id,
            ledger_type=ledger_entry.ledger_type,
            op_type=None,
            group=group,
        )

    @classmethod
    def from_op(cls, op: TrackedAny, group: str = "primary") -> FlowEvent:
        """Create a FlowEvent from a TrackedAny operation."""
        return cls(
            event_type="op",
            timestamp=getattr(op, "timestamp", datetime.now(tz=timezone.utc)),
            ledger_entry=None,
            op=op,
            group_id=getattr(op, "group_id", ""),
            short_id=getattr(op, "short_id", ""),
            ledger_type=None,
            op_type=getattr(op, "op_type", getattr(op, "type", "")),
            group=group,
        )


class FlowInstance(BaseModel):
    """A tracked instance of a transaction flow.

    Collects FlowEvents and checks completeness against a FlowDefinition.
    This is the primary object for both real-time tracking and test assertions.
    """

    flow_definition: FlowDefinition = Field(
        ..., description="The flow definition this instance tracks"
    )
    trigger_group_id: str = Field("", description="Group ID of the triggering event")
    trigger_short_id: str = Field("", description="Short ID of the triggering event")
    cust_id: str = Field("", description="Customer ID associated with this flow")
    status: FlowStatus = Field(FlowStatus.PENDING, description="Current status of the flow")
    started_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="When the flow was initiated",
    )
    completed_at: datetime | None = Field(None, description="When the flow completed")
    events: List[FlowEvent] = Field(
        default_factory=list, description="Recorded events in this flow"
    )

    def add_event(self, event: FlowEvent) -> str | None:
        """Add an event and return the matched stage name, or None if unmatched."""
        # Snapshot which stages are already fulfilled BEFORE adding the event
        # so the new event isn't counted against itself.
        previously_matched = self.matched_stage_names

        self.events.append(event)
        if self.status == FlowStatus.PENDING:
            self.status = FlowStatus.IN_PROGRESS

        # Try to match against stages not yet fulfilled
        for stage in self.flow_definition.stages:
            if stage.name not in previously_matched and stage.matches(event):
                # Check completeness after adding
                if self.is_complete:
                    self.status = FlowStatus.COMPLETED
                    self.completed_at = event.timestamp
                return stage.name

        return None

    @property
    def matched_stage_names(self) -> set[str]:
        """Return names of stages that have been matched by events."""
        matched: set[str] = set()
        for stage in self.flow_definition.stages:
            for event in self.events:
                if stage.matches(event):
                    matched.add(stage.name)
                    break
        return matched

    @property
    def missing_stages(self) -> List[FlowStage]:
        """Return required stages that have not yet been fulfilled."""
        matched = self.matched_stage_names
        return [s for s in self.flow_definition.required_stages if s.name not in matched]

    @property
    def is_complete(self) -> bool:
        """Check if all required stages have been fulfilled."""
        return len(self.missing_stages) == 0

    @property
    def progress(self) -> str:
        """Human-readable progress summary."""
        total = len(self.flow_definition.required_stages)
        done = total - len(self.missing_stages)
        return f"{done}/{total} required stages complete"

    @property
    def duration(self) -> float | None:
        """Duration in seconds from start to completion, or None if not complete."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def summary(self) -> dict[str, Any]:
        """Return a summary dict of this flow instance."""
        return {
            "flow_type": self.flow_definition.name,
            "trigger_group_id": self.trigger_group_id,
            "trigger_short_id": self.trigger_short_id,
            "cust_id": self.cust_id,
            "status": self.status.value,
            "progress": self.progress,
            "matched_stages": sorted(self.matched_stage_names),
            "missing_stages": [s.name for s in self.missing_stages],
            "event_count": len(self.events),
            "duration": self.duration,
        }


# ---------------------------------------------------------------------------
# Overwatch log: backward-compatible entry point for db_monitor integration
# ---------------------------------------------------------------------------


class OverwatchLogEntry:
    """Simple wrapper for an incoming event (ledger entry or op)."""

    __slots__ = (
        "ledger_entry",
        "op",
        "overwatch_id",
        "start_time",
        "end_time",
        "duration",
        "status",
    )

    def __init__(
        self, ledger_entry: LedgerEntry | None = None, op: TrackedAny | None = None
    ) -> None:
        self.ledger_entry = ledger_entry
        self.op = op
        self.overwatch_id = ""
        self.start_time = timer()
        self.end_time: float | None = None
        self.duration: float | None = None
        self.status = "processing"


class OverwatchLog:
    """Central registry for overwatch events and flow instances.

    Receives ledger entries and operations from db_monitor, stores them
    as OverwatchLogEntries, and manages active FlowInstances.
    """

    entries: ClassVar[List[OverwatchLogEntry]] = []
    flow_instances: ClassVar[List[FlowInstance]] = []

    def add_entry(self, entry: OverwatchLogEntry) -> None:
        self.entries.append(entry)

    @classmethod
    def add_flow_instance(cls, instance: FlowInstance) -> None:
        """Register a new flow instance for tracking."""
        cls.flow_instances.append(instance)

    @classmethod
    def get_active_flows(cls) -> List[FlowInstance]:
        """Return flow instances that are not yet completed."""
        return [f for f in cls.flow_instances if not f.is_complete]

    @classmethod
    def get_completed_flows(cls) -> List[FlowInstance]:
        """Return flow instances that are completed."""
        return [f for f in cls.flow_instances if f.is_complete]

    @classmethod
    async def scan_entries(cls) -> None:
        """Scan entries and run sanity checks (placeholder for future logic)."""
        for entry in cls.entries:
            logger.info(f"Scanning entry: {entry}")

    @classmethod
    def reset(cls) -> None:
        """Clear all entries and flow instances (useful for tests)."""
        cls.entries.clear()
        cls.flow_instances.clear()


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
