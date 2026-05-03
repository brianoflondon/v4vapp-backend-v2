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

| Name | Trigger | Req. stages (+ optional) | Description |
|------|---------|--------------------------|-------------|
| `hive_to_keepsats` | `transfer` | 14 | HIVE deposit converted to sats stored on system |
| `hive_to_keepsats_external` | `transfer` | 16 (+ 1) | HIVE converted to keepsats then paid to external Lightning invoice |
| `keepsats_to_hive` | `custom_json` | 10 (+ 7) | Keepsats converted to HBD via exchange (reclassify, limit-order, fill-order all optional) |
| `keepsats_to_external` | `custom_json` | 5 (+ 2) | Keepsats paid to external Lightning invoice |
| `external_to_keepsats` | `invoice` | 4 (+ 3) | External Lightning payment received, stored in keepsats |
| `external_to_hive` | `invoice` | 6 (+ 1) | External Lightning payment received, converted to HIVE and sent on-chain |
| `external_to_keepsats_loopback` | `invoice` | 2 (+ 3) | Loopback/self-payment: invoice lands on same node, stored in keepsats (no Lightning ledger entries) |
| `external_to_hive_loopback` | `invoice` | 4 (+ 1) | Loopback/self-payment: invoice lands on same node, converted to HIVE |
| `external_to_magisats` | `invoice` | 6 (+ 1) | #MAGISATS-tagged invoice forwarded to MagiSats (VSC) wallet via VSC custom_json |
| `keepsats_internal_transfer` | `custom_json` | 2 (+ 1) | Internal keepsats transfer between two customers |
| `hive_transfer_paywithsats` | `transfer` | 4 (+ 1) | HIVE transfer with `#paywithsats` memo triggers internal keepsats transfer |
| `balance_request` | `transfer` | 4 | Balance inquiry: customer sends HIVE, server replies with balance in encrypted memo |
| `hive_transfer_failure` | `transfer` | 4 | Failed HIVE transfer: full amount refunded to sender |

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
| Hash | `overwatch:flows:active` | All non-completed flows. Field key: `{cust_id}:{trigger_group_id}:{flow_name}` |
| String (with TTL) | `overwatch:flows:completed:{cust_id}:{trigger_group_id}:{flow_name}` | Completed flows, 24-hour TTL |

The composite key `cust_id:trigger_group_id:flow_name` includes `cust_id` so
flows for different customers sharing the same trigger group ID never collide.
Multiple candidate flows for the same trigger coexist in Redis with different
`flow_name` suffixes.

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
| Stall log interval | 1 hour | `Overwatch.stall_log_interval` |

`stall_log_interval` throttles repeated stall-warning log lines for the same
flow — a stalled flow is only re-logged once per interval, preventing log
spam for long-running stalls.

---

## Event filters

`FlowStage` accepts an optional `event_filter` callable:

```python
event_filter: Callable[[FlowEvent], bool] | None
```

When set, a stage only matches if both the structural criteria
(`event_type` + `op_type`/`ledger_type`) **and** the filter return `True`.
Filters prevent false-positive matches when multiple flows share the same
structural stage signature.

### Named filters in `overwatch_flows.py`

| Filter | Applied to | Effect |
|--------|-----------|--------|
| `check_balance_request` | `balance_request` trigger stage | Accepts only transfer ops whose `balance_request` attribute is `True` |
| `check_magisats_invoice` | `external_to_magisats` trigger | Accepts only invoice ops tagged `is_magisats=True` |
| `check_not_magisats_invoice` | all other invoice triggers | Accepts only invoice ops where `is_magisats` is falsy |
| `check_vsc_call` | `external_to_magisats` VSC send stage | Accepts only custom_json ops whose `cj_id` starts with `"vsc."` |

**Serialisation note**: `event_filter` is excluded from Redis serialisation
(`exclude=True` in the Pydantic field).  On reload from Redis the definition
is refreshed from the registered `_flow_definitions`, which restores the
callable automatically.

---

## Completion report deduplication

When a flow completes, `Overwatch` does **not** log the result immediately.
Instead it enqueues the flow in `_pending_completion_reports` (keyed by
`trigger_group_id`) and arms a delayed task:

```
flow A completes (trigger_group_id = XYZ)
  └─► _enqueue_completion_report(flow_A)
        └─► schedule _fire_completion_report("XYZ") after 5 s

flow B (superset of A, same trigger) completes 2 s later
  └─► _enqueue_completion_report(flow_B)
        └─► cancel previous task, reschedule _fire_completion_report("XYZ")

5 s later (no more completions)
  └─► _fire_completion_report picks the flow with the most required stages
        └─► logs: "✅ hive_to_keepsats_external ..."
```

The delay (default `COMPLETION_REPORT_DELAY = 5 s`) gives sibling candidates
time to also complete before the report fires, so only the most specific
(highest required-stage count) flow is announced — preventing duplicate
completion notifications when both `hive_to_keepsats` and
`hive_to_keepsats_external` succeed for the same transaction.

---

## Payment failure handling

When a payment fails the server sends a terminal notification rather than
letting the flow complete normally.  `Overwatch.ingest_op` handles two
failure paths after every op is dispatched:

### Path 1 — custom_json notification reply

If the incoming op is a `custom_json` with `parent_id` set,
`notification=True`, and `"Payment failed"` in the memo, then
`_complete_by_notification` is called with the `parent_id` as the lookup key.

```
custom_json notification (parent_id="XYZ", notification=True, memo="Payment failed ...")
  └─► _complete_by_notification(parent_id="XYZ")
        ├─► find all active flows with trigger_group_id == "XYZ"
        ├─► force-complete the most-progressed candidate
        └─► cancel the rest
```

### Path 2 — transfer refund

If the incoming op is a `transfer` (no `parent_id`) with `"Payment failed"`
in the memo and a `§` short-ID back-reference, `_complete_by_notification`
is called with `trigger_short_id` as the lookup key.

```
transfer (memo="Payment failed §XY-ZW", no parent_id)
  └─► find_short_id("Payment failed §XY-ZW") → "XY-ZW"
      _complete_by_notification(trigger_short_id="XY-ZW")
```

In both cases the best candidate is force-completed (even if not all required
stages were fulfilled) and the remaining candidates are cancelled.

---

## VSC / MAGI op skip

`_try_create_flow` contains a guard that prevents `vsc.` custom_json
operations from spawning flow candidates:

```python
if cj_id.startswith("vsc."):
    # skip — VSC ops don't produce customer-facing ledger entries
    return None
```

These are MAGI BTC transactions sent by the VSC layer.  They are stage events
**within** the `external_to_magisats` flow (matched via `check_vsc_call`
filter) but must not trigger independent flow candidates.

### `cancel_flows_for_trigger`

The processing pipeline can call `cancel_flows_for_trigger(trigger_group_id)`
when a trigger op turns out to be irrelevant — for example a transfer between
untracked accounts that produces no ledger entries.  All active candidates
sharing that `trigger_group_id` are immediately cancelled rather than waiting
for the trigger-only timeout.

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

---

## Reply op filter (parent_id check)

Before creating candidate flows, `_try_create_flow` checks whether the
trigger operation carries a **`parent_id`** (on the op's `json_data`).
Custom JSON ops broadcast as side-effects of another transaction — fees,
notifications, keepsats balance updates — always include a `parent_id`
linking them to the originating operation.

If a non-empty `parent_id` is present, candidate creation is skipped.
This prevents reply ops from spawning independent flow candidates that
stall forever.

**Loopback scenario example**: A Hive deposit with a lightning address
memo (e.g. `lightning:user@sats.v4v.app`) triggers an outbound payment
that loops back to the same LND node. This creates more custom_json ops
than a normal deposit — the extra fee/notification custom_jsons exhaust
the existing flow's custom_json slots. Without this filter, the leftover
custom_json would create false `keepsats_to_hive` / `keepsats_to_external`
candidates.

These filters work together with the trigger-only timeout as a multi-layer
defense:
1. **Proactive** — reply op filter and internal account filter block
   candidate creation instantly.
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
           FlowStage(name="some_ledger", event_type="ledger", ledger_type=LedgerType.SOME_TYPE),
           FlowStage(name="optional_step", event_type="op", op_type="notification", required=False),
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
