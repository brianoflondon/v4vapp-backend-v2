# Comprehensive Report: Mapping Transaction Flows and Overwatch System Proposal for v4vapp-backend-v2

## 1. Mapped Transaction Flows

Based on analysis of the codebase, the system processes transactions across Hive blockchain and Lightning Network integrations. Transactions are tracked via `group_id` (unique identifier), `short_id` (human-readable), and ledger entries. Flows are initiated by monitors (hive_monitor_v2.py, lnd_monitor_v2.py) emitting events processed by `process_tracked_events.py`. Below are the primary flows, mapped from start to end.

### 1.1 Incoming Lightning Invoice (Lightning to Hive Conversion)
**Trigger:** LND monitor detects settled invoice.
**Path:** Invoice → `process_lightning_receipt` → Custom JSON Transfer → `process_custom_json_func` → `process_lightning_receipt_stage_2` → Conversion or Change.
**Steps:**
1. Deposit Lightning: Credit VSC Liability (external payment), debit External Lightning Payments (asset/contra).
2. Send Keepsats: Custom JSON transfer from server to customer (cust_id) for msats amount.
3. Process Reply: When custom JSON is processed, if parent is invoice, convert Keepsats to Hive/HBD via `conversion_keepsats_to_hive` or send change if SATS.
4. Ledger: DEPOSIT_LIGHTNING entry.
**Errors:** Bad actor check → Send to v4vapp.sus account.
**Tracking:** group_id links invoice, custom JSON, and ledger entries.

### 1.2 Outgoing Lightning Payment (Hive to Lightning Conversion)
**Trigger:** Hive transfer from user to server with Lightning memo.
**Path:** Transfer → `process_hive_op` → `process_transfer_op` → `conversion_hive_to_keepsats` → Custom JSON → `process_custom_json_func` → `follow_on_transfer` → Payment → `process_payment_success`.
**Steps:**
1. Convert Hive: If paywithsats, convert Hive to Keepsats, send custom JSON to user.
2. User Pays: User sends custom JSON to server with Lightning invoice in memo.
3. Create Payment: Generate Lightning payment, pay via LND.
4. Record Success: Withdraw from VSC Liability to External Lightning Payments, record fees as expense.
5. Ledger: CONV_HIVE_TO_KEEPSATS, WITHDRAW_LIGHTNING, FEE_EXPENSE (if applicable).
**Errors:** Failed payment → Refund Hive transfer.
**Tracking:** group_id chains Hive transfer, custom JSON, payment, ledger entries.

### 1.3 Internal Keepsats Transfers
**Trigger:** Custom JSON with KeepsatsTransfer data.
**Path:** Custom JSON → `process_custom_json_func` → `custom_json_internal_transfer`.
**Steps:**
1. Transfer: Move msats between VSC Liabilities (from_account to to_account).
2. Ledger: Internal transfer entries.
3. If to server: Process as inbound (e.g., pay Lightning invoice if memo contains invoice).
**Errors:** Insufficient balance → Reversal.
**Tracking:** group_id for custom JSON and ledger.

### 1.4 Direct Hive Deposits/Withdrawals
**Trigger:** Hive transfer between accounts.
**Path:** Transfer → `process_hive_op` → `process_transfer_op`.
**Steps:**
1. Ledger: Deposit (asset increase) or withdrawal (liability decrease) based on accounts.
2. Special: Funding memos → Owner Loan entries.
**Tracking:** group_id for transfer and ledger.

### 1.5 Trading Orders (Limit/Fill)
**Trigger:** Hive limit order or fill.
**Path:** Order → `process_hive_op` → `process_create_fill_order_op`.
**Steps:**
1. Ledger: Trading-related entries (e.g., asset reclassifications).
**Tracking:** group_id for order and fills.

### 1.6 Errors, Refunds, and Notifications
**Trigger:** Failures in any flow.
**Path:** Various processors → Error handling → `reply_with_hive` for refunds.
**Steps:**
1. Refund: Send Hive back to user.
2. Notifications: Custom JSON notifications.
**Tracking:** Linked via group_id, logged with errors.

### 1.7 Other Flows
- **Witness Events:** Producer rewards/missed → `process_witness_event` (no ledger).
- **Forward Events:** HTLC forwards → `process_forward` (no ledger).
- **Custom JSON Notifications:** Ignored or processed minimally.

**Flow Diagram (Text):**
```
Hive Transfer (User → Server)
    ↓
Conversion Hive → Keepsats
    ↓
Custom JSON (Server → User)
    ↓
Custom JSON (User → Server, with LN Invoice)
    ↓
Lightning Payment
    ↓
Ledger: Withdraw + Fees

Invoice (LN → Settled)
    ↓
Deposit Lightning
    ↓
Custom JSON (Server → User)
    ↓
Conversion Keepsats → Hive/HBD or Change
    ↓
Ledger: Deposit
```

## 2. Current Tracking Mechanisms
- **Identifiers:** `group_id` (UUID-like, unique per operation), `short_id` (human-readable), and ledger entries.
- **Ledger Entries:** Stored in MongoDB, linked by `group_id`. Types: DEPOSIT_LIGHTNING, WITHDRAW_LIGHTNING, CONV_*, etc. Include debit/credit accounts, amounts, timestamps, conversions.
- **Logging:** Custom logger with emojis, levels (debug/info/warning/error). Extra fields: `notification` (bool), `ledger_items` (list), `sanity_results` (post-process checks).
- **Process Metadata:** `process_time` recorded on tracked objects. Replies linked via `parent_id` and `reply_type`.
- **State:** Tracked objects saved to DB with states (e.g., InvoiceState.SETTLED).
- **Deduplication:** Checks for existing ledger/group_id to prevent reprocessing.

## 3. Current Monitoring Mechanisms
- **Logging Levels:** Debug (detailed), Info (successes), Warning (skips/retries), Error (failures). Notifications sent for errors/successes.
- **Sanity Checks:** Run after every transaction via `run_all_sanity_checks()`. Includes:
  - Balance sheet integrity (check_balance_sheet_mongodb).
  - Account balances (one_account_balance).
  - Held msats (all_held_msats).
  - Hive balances (account_hive_balances).
  - In-progress results validation.
- **Notifications:** Via Hive custom JSON or logging extras.
- **Admin Interface:** FastAPI app (`admin_app.py`) for viewing ledger entries, reports, sanity results.
- **Ledger Balance Checks:** Implicit in sanity checks; explicit in pipelines/account_balances.py.
- **Limitations:** Reactive (post-transaction), no real-time flow visualization, no end-to-end alerting.

## 4. Proposed Overwatch System Architecture

The overwatch system layers over existing code to track, visualize, and alert on transaction flows without altering core logic. It models transactions as state machines, collects events, and provides dashboards.

### 4.1 High-Level Components
- **Event Collector:** Async hooks in `process_tracked_events.py` to emit events (e.g., "operation_started", "ledger_created", "conversion_completed").
- **State Machine Engine:** Finite state machines per flow type (e.g., Hive-to-LN: states like "hive_received", "converted", "paid", "completed"). Transitions logged.
- **Data Storage:** New MongoDB collections: `overwatch_events` (timestamped events), `flow_states` (current state per group_id), `alerts` (threshold breaches).
- **Visualizer/Dashboard:** Web UI (e.g., integrated into admin app) showing flow graphs, timelines, metrics. Use libraries like D3.js or Plotly for diagrams.
- **Alert Engine:** Rules-based (e.g., stuck flows >5min, balance discrepancies). Integrates with existing logging/notifications.
- **API Layer:** REST endpoints for querying flows, states, metrics.

### 4.2 Data Models
- **Event:** {group_id, event_type, timestamp, data (e.g., operation details), state_transition}.
- **Flow State:** {group_id, flow_type, current_state, start_time, last_update, metadata}.
- **Alert:** {group_id, alert_type, severity, message, resolved}.

### 4.3 Integration Points
- **Hooks:** Add async calls in processors (e.g., `await overwatch.emit_event("conversion_started", data)`). Minimal: <10 lines per processor.
- **Existing DB:** Reuse MongoDB; add indexes on group_id/timestamp.
- **Config:** New section in config YAML for overwatch settings (e.g., alert thresholds).

### 4.4 Performance and Scalability
- **Overhead:** Async/non-blocking; batch events if needed. Target <1% latency increase.
- **Storage:** Compress old events; use TTL indexes for retention.
- **Real-Time:** WebSockets for live updates; poll DB for states.
- **Scalability:** Horizontal via multiple instances; Redis for caching states if needed.

### 4.5 Visualization and Alerts
- **Dashboard:** Flow diagrams (e.g., Sankey charts for value movement), heatmaps for errors, timelines per group_id.
- **Alerts:** Email/SMS via existing notification system; thresholds (e.g., >10 failed conversions/hour).
- **Metrics:** Prometheus/Grafana integration for KPIs (flow completion rate, avg process time).

## 5. Feasibility Notes and Potential Blockers
- **Feasibility:** High. Layered design minimizes risk; reuse existing DB/logging. Prototype: Add event emission to one processor, build simple dashboard.
- **Blockers:**
  - DB Load: Sanity checks already query heavily; monitor with profiling.
  - State Complexity: Flows have branches (e.g., errors); define clear state machines.
  - Real-Time: LND/Hive monitors are polled; overwatch could be event-driven.
  - Dependencies: Add Pydantic models, async libraries (e.g., aiofiles for logs).
- **Timeline:** 2-4 weeks for MVP (collector + basic dashboard); 2 months for full alerts/visualization.
- **Testing:** Unit tests for state machines; integration tests with existing flows.

This architecture enables proactive monitoring, reducing manual debugging and improving reliability. If implemented, it would provide end-to-end visibility into the v4vapp ecosystem.
