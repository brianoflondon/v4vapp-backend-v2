# Plan: Voltage → Umbrel production node switch (revised)

**Scope:** Code/DB analysis only.  
**User direction:** Treat ledger as functional history; archive `invoices`/`payments` and adopt Umbrel with a hard floor on indexes so pre-cutover Umbrel (or Voltage) LND history is never re-processed.

---

## Answer: are invoices/payments “never used again” after ledgering?

**For completed financial history: yes, the ledger is the source of truth.**

Customer Keepsats, External Lightning, fees, Hive conversions, etc. live in `ledger`. Balances and accounting do **not** re-scan historical `invoices`/`payments` to rebuild state.

**But the collections are not pure archives.** They still serve active pipeline roles:

| Role | Still needs live `invoices` / `payments`? |
|------|-------------------------------------------|
| Long-term balance sheet / Keepsats | **No** — ledger only |
| **SubscribeInvoices watermark** (`add_index` / `settle_index`) | **Yes** — max invoice in Mongo drives resume |
| Live OPEN → SETTLED and IN_FLIGHT → SUCCEEDED/FAILED | **Yes** — documents are the event records change streams watch |
| Dedup / `process_time` / ledger `group_id` | **Yes** for *current* events; historical completed ones don’t need to stay online |
| **LN deposit stage 2** | **Yes while in flight:** `KeepsatsTransfer.parent_id` = invoice `r_hash`; `load_tracked_object` loads the **Invoice** from `invoices` before `process_lightning_receipt_stage_2` |
| Outbound pay success | Loads **initiating Hive/custom_json** via `v4vapp_group_id` (usually `hive_ops`), not old payment history |
| Forensics / admin / overwatch | Nice-to-have; can live in archive collections |

**Cutover rule:** archive only when there are **no in-flight** Voltage deposits (custom_json not yet stage-2’d) and **no open holds** tied to Voltage payments. Once a flow is fully ledgered and finished, that invoice/payment row is optional for *function*.

---

## Revised strategy (preferred): archive + index floor

### Intent

1. Keep **ledger** (and `hive_ops`) as the only functional history.
2. **Archive** Voltage-era `invoices` / `payments` / optionally `htlc_events` out of the live collections.
3. Point production at Umbrel.
4. **Never process LND history before a chosen floor** on Umbrel’s indexes (and avoid replaying Umbrel’s pre-production history into `db_monitor`).

### Important code facts for this plan

1. **Invoice resume only**  
   `invoices_loop` uses Mongo’s most recent invoice:

   ```text
   SubscribeInvoices(add_index=max_add, settle_index=max_settle)
   ```

   There is **no config today** for a hardcoded floor — it is whatever is on the latest invoice document (or `0,0` if empty).

2. **Payment stream has no index watermark**  
   `TrackPayments` is live-from-subscription-start.  
   Hardcoding `payment_index` does **not** affect the live payment stream.  
   Historical catch-up is `ListPayments` in `_background_sync` / `read_all_payments`.

3. **Empty collections + subscribe from 0 is dangerous on Umbrel**  
   If Umbrel already has settled `#v4vapp` / funding / Magi invoices, LND will stream or backfill them; `db_monitor` will treat new SETTLED inserts as real business events.

4. **So the “floor” must be real Umbrel indexes at cutover**, not arbitrary zeros, unless Umbrel is empty of business-relevant history.

---

## Concrete cutover plan

### Phase 0 — Preconditions

- [ ] Drain Voltage: no OPEN customer invoices you still expect to settle under this stack.
- [ ] No IN_FLIGHT outbound payments; no open Keepsats holds for LN pays.
- [ ] No pending stage-2 deposits (invoice settled → custom_json not yet fully processed).  
  If any exist, finish them **before** archiving `invoices` (stage 2 needs the parent invoice doc).
- [ ] Freeze marketing of Voltage deposit destinations.

### Phase 1 — Capture Umbrel floors (on Umbrel, before pointing prod at it)

From Umbrel LND (or a one-off script using existing client):

- Current max **`add_index`**, max **`settle_index`** among invoices
- Current max **`payment_index`** (for awareness / optional seed; not used by TrackPayments)

These become the **never look before** values for live production.

### Phase 2 — Archive live LND event collections

On production Mongo (`v4vapp-backend`):

```text
invoices      → invoices_voltage_archive   (or dated name)
payments      → payments_voltage_archive
htlc_events   → htlc_events_voltage_archive  (optional)
```

Recreate empty live collections with the same unique indexes (`r_hash`, `add_index`, `payment_hash`, `payment_index`, etc.).

**Leave alone:** `ledger`, `hive_ops`, `ledger_checkpoints`, rates, Magi collections, etc.

### Phase 3 — Install the index floor (choose one implementation)

**Option 3A — Seed sentinel documents (no code change, works with current monitor)**

Insert one synthetic invoice (and optionally payment) into empty collections **with Umbrel’s floor indexes**, e.g.:

- Invoice: real-looking enough for the model, `add_index = UMBREL_MAX_ADD`, `settle_index = UMBREL_MAX_SETTLE`, unique `r_hash` (random), state not required for watermark path  
- Payment: optional; only helps `get_most_recent_payment` for “stale DB” sync timing, not payment stream resume

On monitor start, `get_most_recent_invoice()` returns the sentinel → SubscribeInvoices starts **after** Umbrel’s existing history.

**Caveats for 3A:**

- Sentinel must not look like a business SETTLED `#v4vapp` invoice if it can hit change streams, or use a non-SETTLED state / memo that process ignores.
- Unique indexes: only one doc per `add_index` — fine for a single sentinel.

**Option 3B — Small code/config change (cleaner long-term)**

Add config, e.g.:

```yaml
lnd_config:
  default: umbrel
  subscription_floor:
    add_index: 12345      # Umbrel max at cutover
    settle_index: 6789
  # optional: disable historical backfill on startup
  skip_historical_sync: true
```

Wire into `invoices_loop` (use floor if higher than DB max / if DB empty) and skip or gate `read_all_invoices` / `read_all_payments` so Umbrel’s past is never bulk-inserted into live Mongo.

**Recommendation:** Prefer **3B** if you want an explicit, auditable cutover; **3A** is fine for a one-shot ops procedure with care.

### Phase 4 — Config + node identity for ledger

```yaml
lnd_config:
  default: umbrel   # new ledger sub-account name
  connections:
    umbrel:
      # certs, address, ssl_target_name_override: umbrel.local, ...
```

- Historical External Lightning / fees stay under **`sub=voltage`** (frozen).
- New activity posts under **`sub=umbrel`**.
- On `db_monitor` start, `reset_lightning_opening_balance()` will compare Umbrel channel local msats to ledger `External Lightning Payments` / `sub=umbrel` and may post FUNDING/adjustment. Review that entry deliberately (capital into the Umbrel node).

Also:

- [ ] Deploy Umbrel macaroon + TLS under `.certs/`
- [ ] Fix hardcoded self-pay channel `800082725764071425` in `lnd_functions.py` for Umbrel (or remove hardcode)
- [ ] Point external deposit/LNURL generators at Umbrel
- [ ] All processes that pay LN must load the same config (`hive` path, monitors, API)

### Phase 5 — Disable or constrain historical re-import

Even with a subscription floor, **`_background_sync` → `ListInvoices` / `ListPayments`** will still try to pull Umbrel history into Mongo if those docs are missing.

For this strategy you want **one** of:

1. **`skip_historical_sync: true`** (or temporarily comment out / guard the background task) until floors are trusted; or  
2. Keep sync but **filter** `add_index > floor` / `payment_index > floor` before upsert; or  
3. Allow full import **only after** ensuring `db_monitor` is stopped and then mark imported docs with `process_time` / non-business filters so they never ledger — fragile.

Simplest for cutover: **skip historical sync** and rely on live streams from the floor forward. Gaps only exist for events between floor capture and monitor start — minimize that window.

### Phase 6 — Start services and smoke-test

1. Start `lnd_monitor` → confirm GetInfo alias/pubkey is Umbrel; log shows subscribe from expected indexes.  
2. Start `db_monitor` → review opening-balance FUNDING for `umbrel`.  
3. Small inbound `#v4vapp` settle → ledger under `sub=umbrel`, custom_json, stage 2.  
4. Small outbound pay → TrackPayments → WITHDRAW / fees under `umbrel`.  
5. Admin: node balance vs `External Lightning Payments` / `umbrel` delta.

### Phase 7 — What you deliberately never do

- Do **not** re-import Voltage invoices/payments into live collections for accounting.
- Do **not** leave Voltage max indexes in live Mongo (would skip Umbrel events).
- Do **not** start from empty with floor `0` if Umbrel already has business-like settled history.
- Do **not** rename config key to keep `voltage` while connecting to Umbrel without a conscious ledger re-baseline (blends two nodes under one sub).

---

## Ledger picture after cutover

```
ledger:
  External Lightning Payments / voltage   ← frozen history (Voltage era)
  External Lightning Payments / umbrel    ← new node, opening FUNDING + live traffic
  Treasury Lightning / umbrel             ← fees from new node
  VSC Liability / customers               ← unchanged continuous Keepsats history

invoices / payments (live):
  only Umbrel events after floor (+ optional sentinel)

invoices_voltage_archive / payments_voltage_archive:
  forensic only; not read by monitor resume if renamed away
```

---

## Implementation work (if you want code support for 3B)

Small, focused changes — not required if sentinel + ops discipline is enough:

| Item | Where |
|------|--------|
| `subscription_floor` (add/settle) in config models | `setup.py` + prod YAML |
| Use floor in `invoices_loop` | `lnd_monitor_v2.py` |
| Flag to skip `synchronize_db` / historical read | `lnd_monitor_v2.py` |
| Dynamic self-pay channel (or Umbrel channel id) | `lnd_functions.py` |
| Optional: store `node` on new invoice/payment docs for future multi-node clarity | models + save paths |

No multi-PR stack required for a pure ops cutover with sentinel + archive.

---

## Risk summary (revised)

| Risk | Mitigation in this plan |
|------|-------------------------|
| Miss all Umbrel settles (Voltage watermark) | Archive Voltage rows; floor from **Umbrel** max indexes |
| Re-ledger Umbrel/dev history | Floor + skip historical sync; don’t start at 0 on a non-empty Umbrel |
| Unique index collisions | Empty live collections after archive |
| Broken stage-2 mid-flight | Finish in-flight deposits before archive |
| Opening balance shock | Expected FUNDING on `sub=umbrel`; review before live traffic |
| Self-pay breaks | Update/remove hardcoded channel id |
| Split ledger subs | Accept `voltage` frozen + `umbrel` new (clearer than renaming) |

---

## Bottom line

Your model is right for **completed** work: **the ledger is the functional history.**  
`invoices` / `payments` are the **live event bus + resume cursor**, and briefly the **parent object** for in-flight LN→Keepsats stage 2 — not a second balance book.

**Plan:** archive live LND event collections → set production to Umbrel → **install an Umbrel-based index floor** (sentinel doc or config) → **do not backfill** pre-floor history into live Mongo → open a clean `External Lightning Payments` / `umbrel` ledger sub via controlled opening balance → smoke-test.

That is the correct shape for adopting Umbrel without replaying either Voltage or Umbrel’s prior history into customer balances.
