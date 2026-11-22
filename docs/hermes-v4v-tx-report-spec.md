# Hermes Agent Spec — V4V 4-Hourly Transaction Report

**Purpose:** Every 4 hours, analyse the V4V Lightning↔Hive transaction chains in the Graylog
stream **"Gad V4Vapp Logs"** over the trailing 4-hour window and email the operator a per-
transaction rundown plus error/health summary.

---

## 1. Schedule

- Run every **4 hours** at minute 0: `0 */4 * * *` (00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC).
- Window: **trailing 4 hours** → `range_seconds: 14400`. Never embed the time range in the Lucene query.
- On consecutive runs, overlap is fine; the report just reflects the window it was given.

## 2. Inputs / prerequisites

- **Graylog MCP server** (`graylog`) must be connected and authenticated.
- Tools used: `graylog_list_streams`, `graylog_search_messages`, `graylog_aggregate_messages`.
- **Mail:** send via the operator's configured SMTP (credentials from Hermes secrets; do not
  hardcode). Recipient: `<operator email>`.
- If the Graylog MCP is unreachable or auth fails: **retry once after 5 minutes**; if still
  failing, send a short email "V4V report skipped at <time>: Graylog unavailable — <error>"
  instead of the full report. Never fail silently.

## 3. Stream resolution (every run)

1. Call `graylog_list_streams`.
2. Pick the `id` where `title == "Gad V4Vapp Logs"`.
3. **Never hardcode the ID** (it can change if the stream is recreated).
4. Use that ID in the `streams` parameter of every query. Ignore other streams (router syslog,
   Edi V4Vapp Logs).

## 4. Queries (all scoped to the resolved stream, `range_seconds: 14400`)

### 4.1 Volume by flow type
```
graylog_aggregate_messages
{
  "groupings": [{"field": "flow_type", "limit": 20}],
  "metrics": [{"function": "count", "sort": "Descending"}],
  "query": "flow_type:*",
  "range_seconds": 14400,
  "streams": ["<v4v stream id>"]
}
```

### 4.2 Completed / final chain lines
```
graylog_search_messages
{
  "query": "app_message:stages app_message:complete NOT app_message:started NOT app_message:candidate NOT app_message:STALLED",
  "fields": ["timestamp", "flow_type", "app_message"],
  "limit": 60,
  "range_seconds": 14400,
  "streams": ["<v4v stream id>"]
}
```
> Results arrive newest-first; page with `offset` until exhausted (4h window rarely exceeds 60 lines,
> but a busy window can). From the ✅ lines extract: chain ID, customer, HIVE/HBD amount, sats amount,
> block, completion time, stages (N/N).

### 4.3 Log health
```
graylog_aggregate_messages
{
  "groupings": [{"field": "app_level", "limit": 10}],
  "metrics": [{"function": "count", "sort": "Descending"}],
  "range_seconds": 14400,
  "streams": ["<v4v stream id>"]
}
```

### 4.4 Errors
```
graylog_search_messages
{
  "query": "app_level:ERROR",
  "fields": ["timestamp", "app_message", "app_module"],
  "limit": 50,
  "range_seconds": 14400,
  "streams": ["<v4v stream id>"]
}
```

### 4.5 Warnings (excluding chatty candidate-reconciliation lines)
```
graylog_search_messages
{
  "query": "(app_level:WARNING) NOT app_message:candidate",
  "fields": ["timestamp", "app_message", "app_module"],
  "limit": 30,
  "range_seconds": 14400,
  "streams": ["<v4v stream id>"]
}
```

### 4.6 Failed/stalled chain follow-ups (conditional)
For each chain ID that did NOT complete cleanly (see §5 reconciliation):
```
graylog_search_messages
{ "query": "app_message:\"<chain ID>\"", "fields": ["timestamp", "app_level", "app_module", "app_message"], "limit": 30, "range_seconds": 14400, "streams": ["<v4v stream id>"] }
```
Quote the decisive ERROR/WARNING line in the report.

## 5. Domain knowledge (needed to interpret the logs)

- The `process_overwatch` module tracks each transaction as one or more **candidate flow chains**,
  each tagged with a `flow_type` and a **chain ID in parentheses** in the message
  (e.g. `KxgTaVywG7`, `1206_8e4145_1`).
- Flow types come in superset/subset pairs:
  - `external_to_hive` (superset) vs `external_to_keepsats` (subset) — sats → HIVE
  - `hive_to_keepsats_external` (superset) vs `hive_to_keepsats` (subset) — HIVE/HBD → sats
  - `*_loopback` variants, plus `hive_transfer_paywithsats` and `hive_transfer_failure` as
    transient candidates.
- Message meanings:
  - `✅ … completed N/N required stages complete` → chain finished successfully (customer + amounts in the message).
  - `📌 Keeping candidate` → superset/subset reconciliation, normal.
  - `🗑️ Removed candidate … — 'X' completed` → subset retired, normal.
  - `🗑️ Superset candidate … cancelled — grace period (30s) expired` → the full chain did NOT
    complete in the window. Check `app_level:ERROR` for the cause.
  - `📎 Late event … absorbed by completed flow` → normal.
  - `⚠️ Flow … stalled — N/M required stages complete` → chain stuck; find the last event and
    any ERROR near the stall time.
  - `↳ <flow_type> (<chain ID>) N/M required stages complete` inside periodic "Overwatch:"
    heartbeat lines → repeated noise for the same chain; collapse to one entry per chain ID.
- Direction mapping: `external_to_*` = BTC sats → HIVE; `hive_to_*` = HIVE/HBD → sats.
- Deduplicate chains: the same chain ID appears in multiple candidate lines; report **one row
  per chain ID** using the most advanced state (✅ complete > stalled > partial).
- Zero-amount `keepsats_to_external` flows (0 HIVE / 0 sats, 5/5) are routine micro-ops — list
  them under a "transient/zero-value" note, not in the main tables.
- **Do not double-count superset+subset completions of the same chain ID** (e.g.
  `external_to_keepsats 4/4` + `external_to_hive 6/6` is ONE transaction).

## 6. Reconciliation step

After collecting §4 data:
1. List every distinct chain ID from §4.2.
2. Mark ✅ only if a `✅ completed N/N` line exists for the superset (or the highest-level)
   candidate.
3. Mark ⚠️ partial if a `grace period expired` cancellation or an N<M stage count exists without
   a matching ✅.
4. Mark ❌/⚠️ stalled if a `stalled` or `STALLED` line exists with no later ✅.
5. For each non-clean chain, cross-reference the ERROR results (§4.4) by chain ID or by time
   proximity, and run §4.6 to get the decisive line.
6. Sanity check: overwatch heartbeat lines show `| N active | M stalled | K completed`; if
   active/stalled counts are non-zero at window end, those chains MUST appear as non-clean in the
   report.

## 7. Email output format

**Subject:** `V4V tx report <YYYY-MM-DD HH:MM>–<HH:MM> UTC — <n> chains (<x>✅ <y>⚠️ <z>❌)`

Body (plain text or simple HTML; keep tables as text tables):

1. **Header** — window start/end UTC, stream name + ID, distinct chain count.
2. **Table: BTC sats → HIVE** — columns: chain ID | customer | amount (HIVE / sats / block) |
   completed time | status (✅ / ⚠️ partial / ❌ failed).
3. **Table: HIVE/HBD → sats** — same columns.
4. **Transient/zero-value** — one-liner list.
5. **Detail blocks** — for each non-clean chain, quote the decisive ERROR/WARNING/stall lines
   verbatim, with timestamps.
6. **Totals & health** — sats in (sats→HIVE) and out (HIVE→sats) for completed chains, success
   rate, log-level counts (INFO/WARNING/ERROR), dominant recurring warning (summarise once, e.g.
   "Slow cache set ×23"), overwatch reconciliation sanity (active/stalled/completed at window
   end).
7. If there are **zero transactions** in the window: send a one-liner
   "No V4V transactions in the last 4h. Health: <level counts>." — still send it.

### Known-good reference example (abridged)

> # V4V Transaction Report — last 4h
> Window: 2026-11-22 04:15 → 08:15 UTC · Stream: Gad V4Vapp Logs
> Distinct chains: 6 (5 ✅ · 1 ⚠️ stalled)
>
> ## BTC sats → HIVE — 3 chains, all ✅
> | Chain ID | Customer | Amount | Completed | Status |
> | 8739_6a67db_1 | canelo23 | 768.670 HIVE / 43,620 sats (56) | 07:05:58 | ✅ 10/10 |
> | UgHwltigMX | canelo23 | 766.678 HIVE / 43,620 sats | 06:52:14 | ✅ 6/6 |
> | 8154… (n/a) | — | — | — | — |
>
> ## HIVE/HBD → sats — 2 chains
> | 8154_231fe7_1 | borsengelaber | 25.000 HIVE / 1,422 sats (57) | 06:36:42 | ⚠️ subset ✅, superset cancelled (grace 30s) |
> | 5080_4d82e2_1 | binance-hivehw | 9,999.990 HIVE / 563,377 sats | — | ⚠️ STALLED 2/14 since 04:03 |
>
> ## Non-clean chains
> ### ⚠️ 5080_4d82e2_1 — binance-hivehw 9,999.990 HIVE — STALLED
> 04:08:35  🌟 ⚠️ Flow 'hive_to_keepsats' (5080_4d82e2_1) stalled — 2/14 …
> (quote decisive lines)
>
> ## Errors (N total) …  ## Log health: INFO x · WARNING y · ERROR z …

## 8. Edge cases & gotchas

- **Dust payments** can fail with `Net msats to receive … is negative` (fees exceed amount) —
  note it; if the same customer retries successfully in-window, say so.
- **Failed Hive transfers** (`Not enough to pay … HIVE … Available: …`) for a customer whose
  chain still closed ✅ usually mean the notification transfer retried after a top-up — flag it
  as "confirm customer received HIVE" rather than a hard failure.
- **App restarts** (`Application shutdown complete` / `Loaded N flow(s) from Redis`) reset the
  overwatch counters; stalled flows can silently vanish if they're no longer in the heartbeat.
  Treat "stalled flow disappears after restart" as ⚠️ needs-follow-up, not ✅.
- **Rate-limit errors** (`Lightning conversions for <user> in the last 4 hours: … exceeded`) are
  per-customer throttles — list them under errors, they are not chain failures unless the
  customer's chain also failed to complete.
- ANSI colour codes (`\u001b[37m` etc.) appear in messages — strip them before rendering.
- `graylog_search_messages` has no sort parameter; results are newest-first. Use `offset` to page.
- Some searches that look field-scoped still full-text match; verify results actually belong to the
  flow type you asked for, and prefer narrowing with `NOT` clauses.
- Chain IDs can contain `/`, `+`, `-` (Lightning-style) — always quote them in Lucene:
  `app_message:"7iljE/SZGH"`.

## 9. Acceptance criteria

- [ ] Runs unattended every 4h; emails within 5 min of window end.
- [ ] One row per chain ID, no superset/subset double counting.
- [ ] Every non-clean chain has a quoted decisive log line.
- [ ] Zero-traffic windows still produce an email.
- [ ] Graylog outage produces a "skipped" email, never silence.
- [ ] No hardcoded stream IDs or credentials in the agent code.
