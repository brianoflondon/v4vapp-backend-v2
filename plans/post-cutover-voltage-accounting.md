# Post-cutover: Voltage node accounting (after Legion switch)

**Status:** Legion cutover done. This doc is the handoff for a **new session** focused only on closing out / adjusting ledger state for the disconnected **Voltage** node.

**Related:** `plans/voltage-to-umbrel-node-switch.md` (cutover strategy; target was Umbrel then adapted to Legion).  
**Repos:** `v4vapp-backend-v2` (primary). `v4vapp-api-ext` already mints/decodes on Legion REST.

---

## What already happened (cutover — do not redo)

### Runtime
- Production **backend** points LND gRPC at **legion** (`lnd_config.default: legion` in the live config used on gad-v4vapp).
- **api-ext** mints/checks/decodes via Legion REST + env macaroon (`LND_*`), TLS not verified.
- Voltage is **disconnected** for v4vapp business traffic (no new invoices/payments should land under Voltage).

### Mongo LND event collections
Live collections (code hardcodes these names):

| Live (current) | Archive (Voltage era — forensics only) |
|----------------|----------------------------------------|
| `invoices` | e.g. `invoices_voltage_archive` |
| `payments` | e.g. `payments_voltage_archive` |
| `htlc_events` | e.g. `htlc_events_voltage_archive` |

- Monitors resume from live collections only; archives are **not** watched for new processing.
- After rename, Redis change-stream resume tokens for the three live LND collections had to be cleared (`InvalidResumeToken` / invalidate). Pattern:  
  `resume_token:db_monitor:<default_connection>:<default_name>:<collection>`

### Ledger identity (critical)
- Ledger node sub is `InternalConfig.node_name` = **`lnd_config.default`**.
- Historical Voltage-era rows stay under **`sub=voltage`** (frozen history).
- New activity posts under **`sub=legion`**.
- Cutover opening balance for Legion already ran (`reset_lightning_opening_balance`); logs showed channel local ≈ External Lightning / **legion** matched after first adjust.

### Do **not**
- Point `default: voltage` at Legion (blends nodes under one sub).
- Re-import Voltage archive into live `invoices`/`payments` for accounting.
- Expect live LND streams to “settle” remaining Voltage channel reality — the node is offline to this stack.

---

## Accounting model (what “Voltage left behind” means)

Typical accounts stamped with node sub:

| Account | Sub during Voltage era | Sub now (live) |
|---------|------------------------|----------------|
| External Lightning Payments (asset / contra) | `voltage` | `legion` |
| Treasury Lightning | `voltage` | `legion` |
| Opening / FUNDING adjustments | per node | per node |

Customer Keepsats / VSC liability / Hive sides are **not** node-scoped the same way — they should already be continuous across the cutover. The problem domain is almost entirely:

1. **What balance remains on ledger under `sub=voltage`?**
2. **What real funds (if any) remain on the Voltage node or were swept?**
3. **How to close, reclass, or write-off the Voltage sub so the books match economic reality without double-counting Legion.**

---

## Goals for the new workstream

Figure out and implement (or ops-document) adjustments so that:

1. **Balance sheet** no longer implies Voltage is a live operating node with uncontrolled open exposure.
2. Any residual **External Lightning Payments / voltage** (and related Treasury Lightning / voltage) is:
   - matched to a real residual on Voltage and kept as a frozen asset sub, **or**
   - transferred/reclassed to Legion / owner / expense / write-off with explicit ledger entries, **or**
   - zeroed with a documented closing entry.
3. Future **opening-balance** and admin dashboards that key off `node_name` only look at **legion** for “current node” health.
4. No accidental re-processing of Voltage-era invoice/payment archives into new ledger lines.

Out of scope unless needed: changing collection names in code; re-enabling Voltage monitors.

---

## Investigation checklist (start here in the new window)

### 1. Snapshot current ledger (Voltage vs Legion)

Query production Mongo `ledger` (DB name typically `v4vapp-backend`):

- Sum / list open balances for:
  - `External Lightning Payments` debit/credit with `sub=voltage` and `sub=legion`
  - `Treasury Lightning` with `sub=voltage` and `sub=legion`
  - Any other accounts with `debit.sub` / `credit.sub` = `voltage`
- Note last Voltage-era timestamps vs first Legion FUNDING / live posts.
- Confirm Legion channel local (from GetInfo / channel balance) still ≈ External Lightning / legion.

Useful code paths (read-only first):

- `src/v4vapp_backend_v2/accounting/startup/lnd_openting_balance.py` — opening / FUNDING logic  
- `src/v4vapp_backend_v2/admin/data_helpers.py` — External Lightning vs node balance  
- `src/v4vapp_backend_v2/process/process_invoice.py` / `process_payment.py` — how `sub=node_name` is set  
- Admin ledger edit presets: `src/v4vapp_backend_v2/admin/routers/ledger_edit_presets.py` (may already have exchange ↔ lightning helpers)

### 2. Economic reality of Voltage node

Outside the ledger, answer:

- Is Voltage still funded? Channel balances? On-chain residual?
- Was capacity closed/swept to Legion / cold / exchange?
- Any unpaid invoices or in-flight pays that were abandoned at cutover? (should have been drained; verify)

### 3. Choose an accounting close strategy

Pick one (or a hybrid) and document it:

| Strategy | When | Rough ledger effect |
|----------|------|---------------------|
| **A. Freeze residual** | Real sats still sit on Voltage | Leave `sub=voltage` as frozen asset; no more live traffic; report separately |
| **B. Sweep reclass** | Funds moved to Legion (or exchange) | Transfer entry: reduce External Lightning / voltage, increase External Lightning / legion (or Exchange Holdings / …) with memo |
| **C. Write-off / owner** | Funds lost, fees, or owner takes residual | Expense or Owner Loan / equity style entry vs voltage sub |
| **D. Zero via offset** | Books wrong but economic zero | Adjustment only after reconciling against chain/node export |

Prefer **explicit manual ledger entries** (admin) over replaying LND history.

### 4. Implement / execute

- Prefer admin ledger edit or a one-shot script with dry-run, not silent auto on every db_monitor start.
- Tag entries clearly: memo / `ledger_type` / cust_id so they are auditable (“Voltage node closeout T0”).
- Invalidate ledger cache after posts if needed.
- Re-run balance sheet / node dashboard: only **legion** should move with live LN.

### 5. Cleanup (optional later)

- Expense rules still named `Voltage` in some configs (hosting) — leave or rename for clarity.
- Config may still contain a dormant `voltage` connection block — OK if `default` is not voltage.
- Archive collections retention policy.
- api-ext: any leftover `VOLTAGE_*` env / pubkey defaults.

---

## Known pitfalls

1. **Opening balance only compares current `node_name` (legion)** — it will not auto-close Voltage. Residual on `sub=voltage` will sit until you adjust it.
2. **Do not double-count:** if you FUNDING Legion for full channel capacity *and* still show full Voltage External Lightning, total LN assets are overstated if Voltage was drained into Legion.
3. **Contra accounts:** External Lightning often uses `contra=True` on some legs — mirror existing patterns when building closeout entries.
4. **Change streams:** any bulk insert into live `invoices`/`payments` will re-trigger db_monitor processing — do not dump archive back into live collections.
5. **HBD price “low” warnings** during cutover were unrelated noise.

---

## Paste-ready prompt for a new chat window

```text
Context: Production v4vapp cut over from Voltage LND to Legion LND. Cutover is done.

Facts:
- backend-v2: lnd_config.default = legion; gRPC to legion-witness; .certs legion-admin + tls-legion
- api-ext: LND_REST_URL + LND_MACAROON_HEX for Legion; mint/check/decode work; no TLS cert verify
- Mongo live collections invoices/payments/htlc_events are empty-of-Voltage (Voltage era renamed to *_voltage_archive)
- Ledger: historical External Lightning / Treasury under sub=voltage frozen; live under sub=legion; Legion opening balance already aligned to channel local
- Customer Keepsats continuous; do not re-import archive into live LND collections
- Handoff doc: plans/post-cutover-voltage-accounting.md
- Older cutover plan: plans/voltage-to-umbrel-node-switch.md (same shape; target became Legion)

Task: Figure out how to adjust accounting for the now-disconnected Voltage node — reconcile residual sub=voltage balances with economic reality (freeze, sweep reclass to legion/exchange, or write-off), propose exact ledger entries, then implement or ops-run them safely.
```

---

## Suggested first questions in the new session

1. What is the current ledger balance for `External Lightning Payments / voltage` (and Treasury Lightning / voltage)?
2. What is the real residual on the Voltage node (or zero if empty/closed)?
3. Where did those sats go (Legion, exchange, owner, fees)?
4. Preferred close strategy: freeze vs reclass vs write-off?
5. Manual admin entry vs one-shot script?

---

---

## Production snapshot (2026-08-01, post-cutover investigation)

Queried live `v4vapp-backend.ledger` via `production.fromhome.config.yaml` / `one_account_balance` (cache bypassed).

| Account | sub=`voltage` | sub=`legion` |
|---------|---------------|--------------|
| **External Lightning Payments** | **19,757,789 sats** (19,757,789,152 msats) | **9,979,725 sats** |
| Owner Loan Payable | 54,012,850 sats | 10,001,019 sats |
| Treasury Lightning | −29,163 sats | ~0 (−758 msats) |
| Fee Expenses Lightning | 29,163 sats | ~0 |
| Routing Fee Income | 135,311 sats | — |

**Legion cutover book trail**

| When (UTC) | Type | Note |
|------------|------|------|
| 2026-08-01 12:47 | `funding` / `open` | Initial opening balance for legion: **10,001,019.120 sats** Dr EL/legion Cr Owner Loan/legion |
| 2026-08-01 13:08 | `deposit_l` | +1,018 sats test inbound |
| 2026-08-01 13:09 | `withdraw_l` + `fee_exp` | −22,311 sats outbound (fee 1) → EL/legion ≈ 9,979,725 |

**Voltage last activity**

- Last **funding** adjust: 2026-07-31 10:33 (−741 sats)
- Last **business** EL legs: 2026-08-01 morning withdraws + **11:09** deposit_l (+2,911 sats) still stamped `sub=voltage`
- Combined book LN assets if both kept live: **~29.7M sats** — only correct if Voltage still holds ~19.76M

**Do not touch (historical / continuous)**

- Customer VSC Liability / Keepsats
- Fee Expenses Lightning / voltage, Routing Fee Income / voltage, Treasury Lightning / voltage (fee history already balanced Expense ↔ Treasury)
- Voltage-era LND archive collections

---

## Why books can be below physical residual (on-chain never tracked)

`reset_lightning_opening_balance()` only compares ledger External Lightning to **channel local** (`balances.channel.local_msat`). It never includes LND **on-chain wallet** balance.

As channels close, capacity moves wallet-side. That on-chain residual was always economically on the Voltage node / owner capital, but never booked — so:

| | example |
|--|--:|
| Book External Lightning / voltage | ~19.76M |
| Physical total (channel + chain) | ~24M |
| Untracked on-chain delta | ~4.2M |

Owner Loan / voltage (~54M) is **not** the repay amount. It is cumulative owner capital attributed to the Voltage era; much of it already left the node into customer Keepsats / ops. Only residual still on the node is repaid.

**Operator decision (confirmed intent):** do **not** sweep to Legion; Legion’s ~10M FUNDING is the separate starting loan. Residual on Voltage is returned to **owner funding** (reverse FUNDING vs Owner Loan / voltage).

---

## Recommended close strategy (decision tree)

```
Q1: Returning residual to owner (not Legion)?
 ├─ YES → repay-owner with measured physical R (channel + on-chain)
 │         1) if R > book EL: FUNDING align for untracked on-chain gap
 │         2) Dr Owner Loan / voltage, Cr External Lightning / voltage for full R
 ├─ Keep offline with funds → A FREEZE
 └─ Empty and books already match → book-only writeoff of External Lightning residual
```

**Default for this cutover:** **repay-owner** (not full Owner Loan wipe, not reclass to legion).

---

## Exact proposed ledger entries (repay owner — preferred)

**BOS source of truth (operator-confirmed):**

| BOS | sats |
|-----|-----:|
| Channel Balance | 19,371,509 |
| Chain Confirmed | 6,826,232 |
| **Total residual R** | **26,197,741** |

Book External Lightning / voltage at dry-run: **19,757,789.152** sats  
(Untracked gap R − book = **6,439,951.848** sats — mostly on-chain never booked; book slightly above BOS channel alone from msat/timing drift.)

### Entry 1 — recognize untracked residual gap

```
ledger_type : funding
short_id    : voltage-align
memo        : Voltage node closeout T0

DEBIT   Asset      External Lightning Payments  sub=voltage   6,439,951,848 msats
CREDIT  Liability  Owner Loan Payable           sub=voltage   6,439,951,848 msats
```

### Entry 2 — repay full BOS residual to owner funding

```
ledger_type : funding
short_id    : voltage-repay
memo        : Voltage node closeout T0

DEBIT   Liability  Owner Loan Payable           sub=voltage   26,197,741,000 msats
CREDIT  Asset      External Lightning Payments  sub=voltage   26,197,741,000 msats
```

**Projected after:**

| Account | Before | After |
|---------|--------|-------|
| External Lightning / voltage | 19,757,789 | **0** |
| Owner Loan Payable / voltage | 54,012,850 | **~34,255,061** |
| External Lightning / legion | ~9.98M | unchanged |
| Owner Loan / legion | ~10.00M | unchanged |

Residual ~34M Owner Loan is historical capital that already left the node (Keepsats etc.), not cash still on Voltage. Legion starting loan untouched.

### Book-only shortcut (if physical ≈ book)

Single entry amount = book External Lightning (no align).

### Partial residual (Strategy A hybrid)

If Voltage still holds e.g. `R` sats economically:

```text
--target-sats R   # leave R on books; write off only (current − R)
```

### Strategy B — reclass to Legion (rare; double-count risk)

Only if residual sats **physically** moved into Legion **and** Legion books do **not** already reflect them:

```
DEBIT   Asset External Lightning Payments  sub=legion    R msats
CREDIT  Asset External Lightning Payments  sub=voltage   R msats
```

Prefer instead: physical sweep → `reset_lightning_opening_balance` on legion (+FUNDING) + Strategy C on voltage.

### Strategy B — reclass to exchange

```
DEBIT   Asset Exchange Holdings            sub=<exchange>  R msats
CREDIT  Asset External Lightning Payments  sub=voltage      R msats
```

---

## Ops tooling (implemented)

### Script (preferred)

```bash
# Snapshot only
uv run python scripts/script_voltage_node_closeout.py \
  -c production.fromhome.config.yaml snapshot

# Preferred: dry-run repay owner with measured residual (channel + on-chain)
uv run python scripts/script_voltage_node_closeout.py \
  -c production.fromhome.config.yaml repay-owner --physical-sats 24000000

# Execute only after R confirmed and sats returned / will return to owner
uv run python scripts/script_voltage_node_closeout.py \
  -c production.fromhome.config.yaml repay-owner --physical-sats 24000000 \
  --execute --i-understand

# Book-only (no on-chain align)
uv run python scripts/script_voltage_node_closeout.py \
  -c production.fromhome.config.yaml writeoff
```

Also: `reclass-legion`, `reclass-exchange`, `freeze`, `--target-sats`.

### Admin UI presets

Ledger editor presets:

- **Voltage closeout — write-off residual (vs Owner Loan)**
- **Voltage closeout — reclass residual → Legion**

Fill amount = current External Lightning / voltage (sats). Prefer the script for dry-run + automatic re-snapshot.

---

## Execute checklist

1. [ ] Confirm real Voltage residual (node export / Voltage dashboard / on-chain).
2. [ ] Confirm Legion channel local still ≈ External Lightning / legion.
3. [ ] Choose A / B / C (default C if Voltage empty).
4. [ ] `snapshot` then dry-run of chosen strategy.
5. [ ] `--execute --i-understand` **or** admin preset with exact sats amount.
6. [ ] Re-snapshot: External Lightning / voltage = target; legion unchanged (for C).
7. [ ] Admin dashboard delta only tracks legion.

---

*Generated after successful Legion cutover discussion; balances snapshotted 2026-08-01 investigation session.*
