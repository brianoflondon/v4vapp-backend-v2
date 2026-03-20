# Overwatch System — End-to-End Transaction Flow Tracking

## Overview

The Overwatch system monitors transaction flows in real time by matching
incoming events (ledger entries and blockchain operations) against predefined
flow templates. It answers the question: *"Did this transaction complete all
its expected steps?"*

It runs as a passive observer inside `db_monitor` — no core business logic is
changed. State is persisted in Redis so in-progress flows survive process
restarts.

### Key source files

| File | Purpose |
|------|---------|
| `src/v4vapp_backend_v2/process/process_overwatch.py` | Core engine: models, matching, Redis persistence |
| `src/v4vapp_backend_v2/process/overwatch_flows.py` | Registry of all flow definitions |
| `tests/overwatch/` | Unit tests for each flow type and the engine |

---

## Core concepts

### FlowStage

A single expected event within a flow — either a **ledger entry** (matched by
`LedgerType`) or an **operation** (matched by `op_type`). Stages can be marked
`required=False` for optional steps (e.g. notifications that may not always
fire).

Each stage has a `group` label (`"primary"`, `"fee_notification"`,
`"payment"`, etc.) used for documentation purposes. Matching itself is
**group-agnostic** because `db_monitor` dispatches all events with
`group="primary"`.

### FlowDefinition

A named blueprint listing all `FlowStage`s for a particular transaction type.
Each definition specifies a `trigger_op_type` — the operation type that
initiates the flow (e.g. `"transfer"` or `"custom_json"`).

Currently registered flows:

| Name | Trigger | Required stages | Description |
|------|---------|-----------------|-------------|
| `hive_to_keepsats` | `transfer` | 14 | HIVE deposit converted to sats stored on system |
| `hive_to_keepsats_external` | `transfer` | 17 | HIVE converted to keepsats then paid to external Lightning invoice |
| `keepsats_to_hive` | `custom_json` | 12 (+ 5 optional) | Keepsats converted to HBD via exchange |
| `keepsats_to_external` | `custom_json` | 6 (+ 1 optional) | Keepsats paid to external Lightning invoice |
| `external_to_keepsats` | `invoice` | 4 (+ 3 optional) | External Lightning payment received, converted to keepsats |
| `external_to_hive` | `invoice` | 6 (+ 1 optional) | External Lightning payment received, converted to HIVE and sent on-chain |
| `keepsats_internal_transfer` | `custom_json` | 2 (+ 1 optional) | Internal keepsats transfer between two customers |

### FlowEvent

A thin wrapper around a `LedgerEntry` or `TrackedAny` operation carrying the
metadata needed for matching: `event_type`, `group_id`, `short_id`,
`ledger_type` / `op_type`, and `timestamp`.

### FlowInstance

A live tracking record for one transaction. It holds a reference to its
`FlowDefinition`, the trigger identifiers (`trigger_group_id`,
`trigger_short_id`), and an ordered list of `FlowEvent`s received so far.
Progress is computed by replaying events against the definition's stages.

Status lifecycle: `PENDING` → `IN_PROGRESS` → `COMPLETED` (or `STALLED` / `FAILED`).

### Overwatch (singleton)

The entry point. `db_monitor` calls:

```python
overwatch = Overwatch()
await overwatch.ingest_ledger_entry(ledger_entry)
await overwatch.ingest_op(op)
```

Internally the singleton maintains:
- A **registry** of `FlowDefinition`s (populated at startup).
- A list of `FlowInstance`s (active, stalled, completed).
- A **periodic reporter** (`report_loop`) that logs status and detects stalls.

---

## Multi-candidate flow disambiguation

### The problem

Multiple flow definitions can share the same `trigger_op_type`. For example
both `keepsats_to_hbd` and `keepsats_to_external` are triggered by a
`custom_json` operation. At trigger time we don't know which flow the
transaction will turn out to be — the distinguishing events arrive later.

### The solution: create all candidates, let events decide

When a trigger operation arrives, `_try_create_flow` creates a **candidate
FlowInstance for every definition** that matches the trigger's `op_type`.
All candidates start in `PENDING` and receive the trigger event.

```
Trigger (custom_json) arrives
  └─► Candidate: keepsats_to_hbd   (PENDING, 1/12 stages)
  └─► Candidate: keepsats_to_external (PENDING, 1/6 stages)
```

Subsequent events are dispatched to **all** active flows (not just the first
match). Each candidate independently accumulates whichever stages it can
match:

```
hold_keepsats ledger arrives
  ├─► keepsats_to_hbd:      matches → 2/12
  └─► keepsats_to_external: matches → 2/6

payment op arrives
  ├─► keepsats_to_hbd:      no match (no "payment" stage) → still 2/12
  └─► keepsats_to_external: matches → 3/6

withdraw_lightning ledger arrives
  ├─► keepsats_to_hbd:      no match → still 2/12
  └─► keepsats_to_external: matches → 4/6

... eventually keepsats_to_external completes all 6 required stages
```

### Resolution

When a candidate **completes** (all required stages fulfilled),
`_resolve_candidates` runs:

1. The winning flow is marked `COMPLETED`.
2. Each remaining candidate sharing the same `trigger_group_id` is checked:
   if **every event** the candidate has received can be matched by a stage
   in the winner's definition, the candidate is removed (it's a redundant
   subset).  If the candidate has events the winner **cannot** explain
   (e.g. a `payment` op absent from the winner's definition), the candidate
   is **kept alive** — it's tracking an extended/superset flow.

```
keepsats_to_external completes (6/6)
  └─► _resolve_candidates:
        ├─► keepsats_to_external: COMPLETED ✅ (kept)
        └─► keepsats_to_hbd: all events coverable → FAILED 🗑️ (removed)
```

### Superset flows

Some flow definitions are strict supersets of another (e.g.
`hive_to_keepsats_external` includes all 14 stages of `hive_to_keepsats`
plus 3 external-payment stages).  Both are triggered by `transfer`, so both
candidates are created.

- **Simple deposit** (no external payment): `hive_to_keepsats` completes
  first.  The external candidate only has events the winner can also explain
  → removed.
- **Back-to-back external payment**: payment events arrive before the flow
  completes.  The external candidate now has events (payment, withdraw_l,
  fee_exp) that `hive_to_keepsats` cannot match → kept alive.  Both flows
  complete independently.

```
hive_to_keepsats completes (14/14)
  └─► _resolve_candidates:
        └─► hive_to_keepsats_external: has payment events → 📌 KEPT

hive_to_keepsats_external completes (17/17)  ← later, independently
```

### Superset grace and shared events

When a simpler flow completes and the superset candidate is kept alive, it
enters a **grace period** (default 30 seconds, configurable via
`Overwatch.superset_grace_period`).  During this window, distinguishing
events (e.g. `payment`, `withdraw_l`) can still arrive to prove the superset
flow is the correct one.

The `FlowInstance` tracks:
- `superset_grace_expires` — when the grace window ends.
- `superset_winner_name` — the name of the sibling flow that completed first.

**Shared events don't clear grace.** If a subsequent event matches a required
stage that both the candidate *and* the winner share (e.g. a notification
`custom_json` common to both flows), the grace timer is **not** cleared.
Only events matching stages exclusive to the candidate (not in the winner's
required stages) count as distinguishing evidence.

If no distinguishing event arrives before the grace period expires,
`check_stalls` cancels the candidate.

### Why this approach?

- **No content inspection** — we don't need to parse memo fields or inspect
  operation payloads at trigger time. The system is purely structural.
- **Self-correcting** — if a new flow type is added that shares a trigger,
  it participates automatically with no matcher logic needed.
- **Low overhead** — for triggers with a unique `trigger_op_type` (e.g.
  `transfer` → `hive_to_keepsats`), only one instance is created. Candidates
  only exist when there's genuine ambiguity.

### Late-arriving optional events

Some optional stages (e.g. the notification `custom_json`) arrive **after**
all required stages have completed the flow. Since the flow is already
`COMPLETED`, it would normally be invisible to `_dispatch`.

To handle this, `_dispatch` has a **second pass**: if no active flow matches
the event, it tries completed flows. If a completed flow has an unfulfilled
stage that matches, the event is absorbed there (logged as a "late event")
and `_try_create_flow` is never reached. This prevents spurious candidate
flows from being created for reply/notification operations.

---

## Event dispatch flow

```
db_monitor receives a change-stream event
  │
  ├─► LedgerEntry  →  overwatch.ingest_ledger_entry(le)
  │                       └─► _dispatch(event) to all active flows
  │
  └─► TrackedAny op  →  overwatch.ingest_op(op)
                            ├─► _dispatch(event) to all active flows
                            └─► if no match: _try_create_flow(event, op)
                                  └─► create candidates for all matching definitions
```

### Deduplication

MongoDB change streams can fire both `insert` and `update` for the same
document (e.g. a trigger op being updated with reply IDs). The `_is_duplicate`
check prevents double-counting:
- **Op events**: deduplicated by `(event_type, group_id)`.
- **Ledger events**: deduplicated by `(event_type, group_id, ledger_type)`.

---

## Redis persistence

All flow state is mirrored to Redis so that in-progress flows survive
`db_monitor` restarts.

| Redis structure | Key format | Purpose |
|----------------|------------|---------|
| Hash | `overwatch:flows:active` | All non-completed flows. Field key: `{trigger_group_id}:{flow_name}` |
| String (with TTL) | `overwatch:flows:completed:{trigger_group_id}:{flow_name}` | Completed flows, 24-hour TTL |

The composite key `trigger_group_id:flow_name` allows multiple candidate
flows for the same trigger to coexist in Redis.

On startup, `load_from_redis` hydrates in-memory state. If a flow definition
has changed since the flow was persisted, the definition is refreshed and
completeness is re-evaluated — a flow that now meets all required stages is
auto-completed.

---

## Stall detection

The `report_loop` coroutine runs periodically (default 30 seconds) and calls
`check_stalls`, which applies three rules in order:

### Rule 1: Superset grace expiry

Candidates whose `superset_grace_expires` has passed are cancelled (status
`FAILED`, removed from active flows).

### Rule 2: Trigger-only timeout

Flows that only contain their initial trigger op (`len(events) == 1` and
`event_type == "op"`) and have been alive longer than
`trigger_only_timeout` (default 60 seconds) are cancelled.

This prevents false positives from **internal operational transfers** (e.g.
server-to-exchange rebalancing, change returns) that match the trigger type
but will never produce the expected conversion ledger entries. Without this
rule, such flows linger until the 5-minute stall timeout and generate
spurious warnings.

Flows that have received *any* subsequent matched event (even one ledger
entry) are exempt from this rule and use the normal stall timeout instead.

### Rule 3: Normal stall timeout

If no event has arrived within the `stall_timeout` (default 5 minutes),
the flow is marked `STALLED`. Stalled flows remain tracked — if new events
arrive later they resume normally.

### Configurable timeouts

| Timeout | Default | ClassVar |
|---------|---------|----------|
| Stall timeout | 5 minutes | `Overwatch.stall_timeout` |
| Superset grace period | 30 seconds | `Overwatch.superset_grace_period` |
| Trigger-only timeout | 60 seconds | `Overwatch.trigger_only_timeout` |

---

## Internal account filter

Before creating candidate flows, `_try_create_flow` checks whether the
trigger operation is an **internal transfer between known system accounts**
(server, treasury, funding, exchange — from
`InternalConfig().config.hive.all_account_names`).

If both `from_account` and `to_account` are internal accounts, candidate
creation is skipped entirely. This prevents operational transfers
(server ↔ treasury, server → exchange rebalancing, etc.) from spawning
false flow candidates that can never complete.

This filter works together with the trigger-only timeout as a two-layer
defense:
1. **Proactive** — internal account filter blocks candidate creation
   instantly.
2. **Safety net** — trigger-only timeout catches any other non-customer ops
   that slip through (e.g. if config is temporarily unavailable).

---

## Selective event append

`FlowInstance.add_event` only appends events that match a stage to the
`events` list. Unmatched events are silently ignored and return `None`
without modifying the instance.

This prevents unmatched events from:
- Polluting the event list and inflating progress/debug output.
- Interfering with `_resolve_candidates` logic (which checks whether a
  candidate has events the winner "cannot explain").
- Preventing the trigger-only timeout from firing (since `len(events)`
  stays at 1 until a real stage-matched event arrives).

---

## Adding a new flow definition

1. **Identify the stages** from log data or the transaction code path. Each
   stage is either a ledger entry type or an operation type.

2. **Create the `FlowDefinition`** in `overwatch_flows.py`:

   ```python
   NEW_FLOW = FlowDefinition(
       name="my_new_flow",
       description="Description of the flow",
       trigger_op_type="custom_json",  # or "transfer", etc.
       stages=[
           FlowStage(name="trigger_op", event_type="op", op_type="custom_json"),
           FlowStage(name="some_ledger", event_type="ledger",
                     ledger_type=LedgerType.SOME_TYPE),
           FlowStage(name="optional_step", event_type="op",
                     op_type="notification", required=False),
       ],
   )
   ```

3. **Register it** in the `FLOW_DEFINITIONS` dict at the bottom of the file.

4. **Add test data** — extract a real transaction from `db_monitor.jsonl`
   into `tests/data/overwatch/my_new_flow.json`.

5. **Write tests** — see the existing test files for patterns (stage
   matching, event replay, Overwatch dispatch, completeness checks).

If the new flow shares a `trigger_op_type` with an existing flow, no
additional disambiguation logic is needed — the multi-candidate system
handles it automatically.
