---
name: v4v-tx-report
description: Analyse V4V Lightning↔Hive transaction chains from the Graylog "Gad V4Vapp Logs" stream. Use when the user asks for a transaction summary, chain analysis, settlement report, or error review of the v4vapp service over a time window.
---

# V4V transaction chain report

Analyse transaction chains in the Graylog stream **"Gad V4Vapp Logs"** via the `graylog` MCP server. Default window: last 24h (`range_seconds: 86400`); use whatever window the user specifies.

**Resolve the stream ID first:** call `graylog_list_streams` and pick the `id` where `title` is `Gad V4Vapp Logs`. Never hardcode the ID — it can change if the stream is recreated. Use that ID in the `streams` parameter of every query below. The other stream in the instance is router syslog and is irrelevant.

## Key concepts

- The `process_overwatch` module tracks each transaction as one or more **candidate flow chains**, each tagged with a `flow_type` and a **chain ID in parentheses** in the message (e.g. `KxgTaVywG7`, `1206_8e4145_1`).
- Flow types come in superset/subset pairs:
  - `external_to_hive` (superset) vs `external_to_keepsats` (subset) — sats → HIVE
  - `hive_to_keepsats_external` (superset) vs `hive_to_keepsats` (subset) — HIVE/HBD → sats
  - `*_loopback` variants, plus `hive_transfer_paywithsats` and `hive_transfer_failure` as transient candidates.
- Message meanings:
  - `✅ … completed N/N required stages complete` → chain finished successfully (customer + amounts are in the message).
  - `📌 Keeping candidate` → superset/subset reconciliation, normal.
  - `🗑️ Removed candidate … — 'X' completed` → subset retired, normal.
  - `🗑️ Superset candidate … cancelled — grace period (30s) expired` → the full chain did NOT complete in the window. Check `app_level:ERROR` for the cause.
  - `📎 Late event … absorbed by completed flow` → normal.
- Direction mapping: `external_to_*` = BTC sats → HIVE; `hive_to_*` = HIVE/HBD → sats.

## Workflow

1. **Volume by chain type** — `graylog_aggregate_messages`:
   `{"groupings":[{"field":"flow_type","limit":20}],"metrics":[{"function":"count","sort":"Descending"}],"query":"flow_type:*","range_seconds":<N>,"streams":["<v4v stream id>"]}`
2. **Completed chains** — `graylog_search_messages` with `query: "flow_type:* app_message:completed"`, fields `["timestamp","flow_type","app_message"]`, limit ~50. Extract chain ID, customer, amounts, and stage counts from the `✅` lines.
3. **Log health** — aggregate `app_level` (count, no query filter).
4. **Errors** — search `app_level:ERROR` fields `["timestamp","app_message","app_module"]`.
5. **Warnings** — search `(app_level:WARNING) NOT app_message:candidate`, limit ~30; summarise the dominant recurring warning instead of listing each occurrence.
6. **Reconcile** — match any "grace period expired" cancellations or partial stage counts (e.g. 4/6) against the ERROR results to identify failed chains and their cause.

## Output format

1. Header: window + stream + number of distinct chains.
2. One table per direction (sats→HIVE, HIVE→sats): chain ID, customer, amount (sats + HIVE/HBD + block), completed time, status (✅ / ⚠️ partial / ❌ failed).
3. Detail block for any non-clean chain, quoting the relevant ERROR/WARNING lines.
4. Totals & health: sats in/out, success rate, log level counts, dominant recurring warnings, overwatch reconciliation sanity (active vs completed counts).

## Tips

- Pass the timerange as `range_seconds` (or via the tool's absolute range), never inside the Lucene query.
- Always scope to the resolved v4v stream; the other stream in the instance is router syslog and is irrelevant.
- If a chain ID from a completion line needs follow-up, search `app_message:"<chain ID>"`.
- Dust payments (very small sats) can fail conversion with "Net msats to receive … is negative" — fees exceed the amount; note when the customer retries successfully.
