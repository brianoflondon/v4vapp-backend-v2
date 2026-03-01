"""
Overwatch System: End-to-end transaction flow tracking.

This module provides a framework for defining expected transaction flows,
recording events as they occur, and verifying flow completeness.

Core models:

- FlowStage: An expected event (ledger entry or operation) in a flow.
- FlowDefinition: A blueprint for a complete transaction flow.
- FlowEvent: A recorded event that has occurred.
- FlowInstance: A tracked flow instance that collects events and checks completeness.
- Overwatch: Singleton that ingests events, manages active flows, and runs a
  periodic reporting loop.

Usage from db_monitor::

    from v4vapp_backend_v2.process.process_overwatch import Overwatch

    overwatch = Overwatch()                   # singleton
    await overwatch.ingest_ledger_entry(le)   # feed entries
    await overwatch.ingest_op(op)             # feed ops

    # Start the periodic reporter as an asyncio task:
    asyncio.create_task(overwatch.report_loop(interval=30, shutdown_event=evt))
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from enum import StrEnum
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
# Overwatch singleton: clean entry point for db_monitor integration
# ---------------------------------------------------------------------------

# Default timeout — if a flow hasn't received new events for this long it
# is marked as STALLED during the periodic report.
DEFAULT_STALL_TIMEOUT = timedelta(minutes=5)


class Overwatch:
    """Singleton that ingests events, manages active flows, and reports progress.

    Instantiate anywhere — you always get the same instance::

        overwatch = Overwatch()
        await overwatch.ingest_ledger_entry(ledger_entry)
        await overwatch.ingest_op(op)

    Start the periodic reporter as a long-running asyncio task::

        asyncio.create_task(
            overwatch.report_loop(interval=30, shutdown_event=shutdown_event)
        )
    """

    _instance: ClassVar[Overwatch | None] = None

    # ---- state (shared across all references) ----
    flow_instances: ClassVar[List[FlowInstance]] = []
    _flow_definitions: ClassVar[dict[str, FlowDefinition]] = {}
    stall_timeout: ClassVar[timedelta] = DEFAULT_STALL_TIMEOUT

    def __new__(cls) -> Overwatch:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # ---- flow definition registry ----

    @classmethod
    def register_flow(cls, definition: FlowDefinition) -> None:
        """Register a FlowDefinition so triggers can auto-create instances."""
        cls._flow_definitions[definition.name] = definition

    @classmethod
    def registered_flows(cls) -> dict[str, FlowDefinition]:
        """Return the currently registered flow definitions."""
        return dict(cls._flow_definitions)

    # ---- event ingestion ----

    async def ingest_ledger_entry(
        self,
        ledger_entry: LedgerEntry,
        group: str = "primary",
    ) -> str | None:
        """Feed a ledger entry into overwatch.

        The entry is wrapped in a FlowEvent, matched against every active
        (non-completed) flow, and the matched stage name is returned (or
        ``None`` if no flow consumed it).
        """
        event = FlowEvent.from_ledger_entry(ledger_entry, group=group)
        logger.info(
            f"{ICON} {ledger_entry.short_id} {ledger_entry.ledger_type_str}",
            extra={"notification": False},
        )
        return self._dispatch(event)

    async def ingest_op(
        self,
        op: TrackedAny,
        group: str = "primary",
    ) -> str | None:
        """Feed a TrackedAny operation into overwatch.

        If the op matches a registered trigger_op_type **and** no active flow
        already owns this ``group_id``, a new FlowInstance is automatically
        created.  Returns the matched stage name, or ``None``.
        """
        event = FlowEvent.from_op(op, group=group)
        logger.info(
            f"{ICON} {op.short_id} {op.op_type}",
            extra={"notification": False},
        )

        # Auto-create a flow instance when a trigger op arrives
        matched = self._dispatch(event)
        if matched is None:
            matched = self._try_create_flow(event, op)
        return matched

    # ---- internal dispatch ----

    def _dispatch(self, event: FlowEvent) -> str | None:
        """Try to match *event* against every active flow instance."""
        for flow in self.active_flows:
            result = flow.add_event(event)
            if result is not None:
                if flow.is_complete:
                    logger.info(
                        f"{ICON} ✅ Flow '{flow.flow_definition.name}' completed "
                        f"({flow.trigger_short_id}) in {flow.duration:.1f}s",
                        extra={"notification": False},
                    )
                return result
        return None

    def _try_create_flow(self, event: FlowEvent, op: TrackedAny) -> str | None:
        """If *event* matches a registered trigger, spin up a new FlowInstance."""
        for defn in self._flow_definitions.values():
            if event.op_type == defn.trigger_op_type and event.group == "primary":
                instance = FlowInstance(
                    flow_definition=defn,
                    trigger_group_id=event.group_id,
                    trigger_short_id=event.short_id,
                    cust_id=getattr(op, "from_account", ""),
                )
                self.flow_instances.append(instance)
                result = instance.add_event(event)
                logger.info(
                    f"{ICON} 🆕 New flow '{defn.name}' started "
                    f"({event.short_id}) cust={instance.cust_id}",
                    extra={"notification": False},
                )
                return result
        return None

    # ---- queries ----

    @property
    def active_flows(self) -> List[FlowInstance]:
        """Return flow instances that are not yet completed or failed."""
        return [
            f
            for f in self.flow_instances
            if f.status not in (FlowStatus.COMPLETED, FlowStatus.FAILED)
        ]

    @property
    def completed_flows(self) -> List[FlowInstance]:
        """Return completed flow instances."""
        return [f for f in self.flow_instances if f.status == FlowStatus.COMPLETED]

    @property
    def stalled_flows(self) -> List[FlowInstance]:
        """Return stalled flow instances."""
        return [f for f in self.flow_instances if f.status == FlowStatus.STALLED]

    # ---- stall detection ----

    def check_stalls(self, now: datetime | None = None) -> List[FlowInstance]:
        """Mark active flows that haven't received events recently as STALLED.

        Returns the list of flows whose status was changed.
        """
        now = now or datetime.now(tz=timezone.utc)
        newly_stalled: List[FlowInstance] = []
        for flow in self.active_flows:
            last_event_time = flow.events[-1].timestamp if flow.events else flow.started_at
            if now - last_event_time > self.stall_timeout:
                flow.status = FlowStatus.STALLED
                newly_stalled.append(flow)
                logger.warning(
                    f"{ICON} ⚠️ Flow '{flow.flow_definition.name}' "
                    f"({flow.trigger_short_id}) stalled — "
                    f"{flow.progress}, last event {last_event_time:%H:%M:%S}",
                    extra={"notification": False},
                )
        return newly_stalled

    # ---- periodic reporter ----

    async def report_loop(
        self,
        interval: float = 30,
        shutdown_event: asyncio.Event | None = None,
    ) -> None:
        """Long-running coroutine that periodically logs flow status.

        Args:
            interval: Seconds between reports.
            shutdown_event: When set, the loop exits gracefully.
        """
        logger.info(
            f"{ICON} Overwatch report loop started (interval={interval}s)",
            extra={"notification": False},
        )
        while True:
            if shutdown_event and shutdown_event.is_set():
                break
            try:
                self.check_stalls()
                self._log_report()
            except Exception as e:
                logger.error(
                    f"{ICON} Error in overwatch report loop: {e}",
                    extra={"error_code": "overwatch_report_error"},
                )
            # Sleep in small increments so we can break promptly on shutdown
            for _ in range(int(interval)):
                if shutdown_event and shutdown_event.is_set():
                    break
                await asyncio.sleep(1)

        logger.info(
            f"{ICON} Overwatch report loop stopped",
            extra={"notification": False},
        )

    def _log_report(self) -> None:
        """Emit a single log line summarising current state."""
        active = self.active_flows
        stalled = self.stalled_flows
        completed = self.completed_flows

        if not active and not stalled:
            return  # nothing interesting to report

        parts: list[str] = [f"{ICON} Overwatch:"]
        if active:
            parts.append(f"{len(active)} active")
        if stalled:
            parts.append(f"{len(stalled)} stalled")
        parts.append(f"{len(completed)} completed")
        logger.info(" | ".join(parts), extra={"notification": False})

        for flow in active:
            logger.info(
                f"{ICON}   ↳ {flow.flow_definition.name} "
                f"({flow.trigger_short_id}) {flow.progress}",
                extra={"notification": False},
            )
        for flow in stalled:
            logger.warning(
                f"{ICON}   ↳ STALLED {flow.flow_definition.name} "
                f"({flow.trigger_short_id}) {flow.progress} "
                f"missing: {[s.name for s in flow.missing_stages]}",
                extra={"notification": False},
            )

    # ---- housekeeping ----

    @classmethod
    def reset(cls) -> None:
        """Clear all state — useful for tests."""
        cls.flow_instances.clear()
        cls._flow_definitions.clear()
        cls.stall_timeout = DEFAULT_STALL_TIMEOUT
        cls._instance = None
