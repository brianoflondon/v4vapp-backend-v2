# Code review — `perf/hive-monitor-catchup-virtual-range`

- **Base:** `develop`
- **Commits:** `7995c63e`, `37f9a88c`, `2a55bdf8`, `2b355633` (4 ahead of `develop`)
- **Files changed (8):**
  - `src/v4vapp_backend_v2/hive_models/virtual_ops_range.py` (new, 328 lines)
  - `src/v4vapp_backend_v2/hive_models/stream_ops.py` (+134)
  - `src/v4vapp_backend_v2/process/process_hive.py` (+164)
  - `tests/process/test_hive_ledger_replay.py` (new)
  - `tests/hive_models/test_virtual_ops_range.py` (new)
  - `tests/hive_models/test_stream_ops.py`
  - `scripts/compare_virtual_op_normalize.py` (new)
  - `.vscode/launch.json`
- **Focus:** `hive_monitor_v2` / `process_hive` — whether the newly generated Hive
  `group_id` is reliably unique for catch-up scans and live chain scans, the
  defensive coding around `op_in_trx` counter inconsistencies, and the
  likelihood that virtual operations get double-processed.

---

## Scope of the change

- A new `VirtualOpRangeFetcher` that reads virtual ops far behind head via
  `account_history_api.enum_virtual_ops` (one block range per window) instead of
  one `get_ops_in_block` per block.
- Its integration into `stream_ops_async`: far-behind batches use the fast path;
  near head (last 50 blocks) always use the per-block Nectar stream.
- A ledger replay guard in `process_hive.py`
  (`find_existing_hive_ledger_entry` / `booked_op_in_trx` /
  `hive_ledger_trx_filter` / `HiveLedgerOrderMismatch`) that detects
  already-booked slots for the same Hive transaction.
- Tests for all three, plus a manual live-node comparison script.

---

## How `group_id` uniqueness is achieved

`group_id = f"{block_num}_{trx_id}_{op_in_trx}_{realm}"` where `op_in_trx` is
**not** the chain value — it is assigned locally by `OpInTrxCounter`
(`src/v4vapp_backend_v2/hive_models/op_base_counters.py:37`):

- **Real ops:** 1-based count per `trx_id` of the *filtered* real stream (ops in
  `opNames` = `OP_TRACKED`).
- **Virtual ops:** 1-based count per `(block_num, trx_id)` group, of *every*
  virtual op in API order.

Determinism therefore rests on three invariants, and the branch gets all three
right:

1. **Both paths count untracked virtual ops.** The new `counted_virtual_op()`
   (`stream_ops.py:86`) calls `op_in_trx_inc()` *before* the `op_names` filter,
   and the fast path normalizes with `op_names=None` (keeps every well-formed
   op, including unknown future types → `OpBase`, still counted). The per-block
   path already did this (the diff only extracts the shared helper — no
   semantic change). If either path skipped untracked ops, the same logical op
   would get different indices across paths → two `hive_ops` rows → double
   processing. This is the core correctness fix, and it is tested
   (`test_counted_virtual_op_includes_untracked_ops_in_the_index`).
2. **Unknown op names keep the fast path off.** `virtual_op_filter_mask()`
   returns `None` (fetcher stays disabled) if any tracked name is neither a
   known real nor known virtual op, or its bit ≥ 2⁵³ (JSON number safety). The
   mask is only a gate — the RPC call itself sends no filter, by design
   (docstring in `_fetch_window`): filtering by type would drop untracked ops
   and shift the counter.
3. **Indexes enforce the last line of defense.** `hive_ops` has the unique
   compound `[block_num, trx_id, op_in_trx, realm]` (matches `save()`'s upsert
   filter exactly → same key = update, never insert), and `ledger` has unique
   `group_id`. `db_store_op()`'s `DuplicateKeyError` catch is a redundant
   belt-and-braces layer.

Counter interleaving is safe: a real trx's real ops are always counted before
its virtual ops of that block (the virtual batch for block B is triggered only
when block B+2's first real op arrives, after all of B's real ops were
yielded), and the virtual branch resets on `(block_num, trx_id)` change, so a
real and virtual op of the same `trx_id` each start their own 1-based sequence
in separate realms — matching the ledger filter's realm anchor.

---

## Double-processing likelihood, by scenario

### Within one session: effectively zero

`virtual_blocks_to_fetch()` (`stream_ops.py:102`) yields disjoint ranges:

- step A→A+1 fetches `[A-1]` (block N's virtuals are read when N+2's first real
  op arrives),
- jump A→B fetches `[A-1 … B-2]`,
- the next fetch starts at `[B-1]`.

Each block's virtuals are fetched exactly once per session, counted exactly
once. The `HiveLedgerOrderMismatch` / duplicate layers cannot even be triggered
in-session.

### Across a restart with path-mode crossing: low (the residual risk)

The monitor resumes from `max(hive_ops.block_num) + 1`
(`op_base.py:417`, `get_last_good_block`). After a restart the new session *can*
re-fetch the virtuals of the last saved block (block `S-1` where `S =
max_saved+1`) if tracked real ops arrive in the first two scanned blocks.
Normally that is harmless: same path mode → identical op list → identical
indices → upsert no-op. Harm requires **all** of:

1. a crash/restart,
2. ≥ ~50 blocks (~2.5 min on 3 s blocks) of downtime so the block crosses the
   near-head boundary (`CATCHUP_NEAR_HEAD_BLOCKS = 50`) between the two reads,
   and
3. the two APIs/normalizations disagreeing on that block's op list or order.

The 50-block margin is also what keeps the fast path out of the fork /
irreversible window: `safe_end = head_cache − 50` and `head_cache` never
overestimates the true head, so fast-path blocks are always ≥ 50 behind head
(typical node irreversible lag is 15–30 blocks). Good design.

### Within one window, without any restart: one theoretical edge

`_fetch_window`'s pagination treats `next_operation_begin = null` as `0`
(`int(result.get("next_operation_begin") or 0)`). If the API ever returned a
page ending mid-block X with `next_block_range_begin = X` but a null
`next_operation_begin`, the next page re-reads block X from op 0 → duplicated
ops inside the window → that op saved under two different `op_in_trx` values
(both processed) **and** the indices of all later ops in block X shifted vs.
the per-block path. The runtime verification would not catch it unless X falls
in the window's first 3 blocks. This requires an API pagination bug (Hive's
`enum_virtual_ops` reliably returns `next_operation_begin` when continuing
mid-block, as the "made no progress" / "stopped early" checks assume), so this
is **very low likelihood**, but it is the only path to same-session double
processing.

---

## The `process_hive.py` replay guard — coverage

The guard is well-targeted at the restart scenario above. Verified details:

- Realm stays in the regex (`real` vs `virtual`) so a limit order's real op and
  its virtual fills, which share `trx_id` and each start at 1, never collide;
  block stays because the zero-trx is shared across blocks
  (`test_zero_trx_does_not_collide_across_blocks`).
- Fill buy+sell rows (same slot, `_fill_or_b` / `_fill_or_s` suffixes) count as
  one booked op; an exact group_id row is preferred over a suffixed one.
- **Same slot** → `LedgerEntryDuplicateException` (re-raised;
  `process_tracked_event` still runs `perform_finalize`, so the op is marked
  processed and cannot retry-loop in `db_monitor`).
- **Booked slot higher than incoming** → `HiveLedgerOrderMismatch` →
  `process_hive_op` returns `[]`, op marked processed. Conservative and correct:
  in-order streams never deliver op 3 after op 4 for one (block, trx, realm),
  so an out-of-order arrival is a replay.
- **Gaps are allowed** (earlier op that created no ledger row, e.g.
  `producer_reward`).
- Historical 0-based rows map onto slot 1 (`_ledger_op_slot`) — a migration
  courtesy.
- The regex is anchored (`^{block}_{trx}_\d+_realm($|_)`) so it is a short range
  on the unique `group_id` index; the filter deliberately uses `group_id`, not a
  Hive-field query, so ledger rows suffixed with a ledger type are found too.

**The one documented gap** (the docstring is honest about it): an incoming slot
*higher* than everything booked — i.e. a **shifted-up index on a re-read**
where the new slot is unbooked — is processed and re-books the op. The comment
in `process_hive_op` says exactly this: *"Do not replay or re-scan history after
changing this set: a higher index is booked again."* That is an operational
control, not a structural one. It can only be triggered by (a) changing the
tracked-op set and rescanning history, or (b) the two fetch paths disagreeing by
*extra* ops on a crossed-boundary re-read (the pagination edge above). A
shifted-*down* re-read is caught by the mismatch check.

Direction summary for a re-read of an already-booked op X (true slot 2):

| Re-read delivers X at | Booked | Guard result |
| --- | --- | --- |
| slot 2 | {2} | duplicate → dropped ✓ |
| slot 1 | {2} | later booked → mismatch → dropped ✓ |
| slot 3 | {2} | unbooked slot → **processed → double-booked ✗** (documented) |

For the five `HiveLedgerOp` types that create ledger rows
(`TransferBase`, `LimitOrderCreate`, `FillOrder`, `LimitOrderCancelled`,
`CustomJson`), the guard is **sufficient for the threat model**; the unguarded
higher-index case has a real but very low trigger probability.

---

## Findings, severity-ranked

1. **Medium — verification depth is one block per window.**
   `_verify_window` returns on the *first non-empty* block within the first 3,
   so ~1 of 200 blocks is verified per window and blocks after position 3 are
   never checked. A content/order divergence from block 4 onward in a
   200-block window is uncaught until it reaches the ledger guard. Mitigations
   already present: same underlying op log, the manual
   `compare_virtual_op_normalize.py`, and the replay guard.
   *Recommendation:* verify a small sample per window (e.g. first non-empty +
   2 random blocks) for a cheap large reduction, or at minimum log the verified
   block number so it is auditable.

2. **Low — runtime verification does not compare against Nectar's
   normalization.** `_slow_block_events` normalizes `get_ops_in_block` output
   with the *same* `normalize_virtual_op`, so the runtime check validates
   enum-vs-`get_ops_in_block`, not our flattening vs. Nectar's stream
   flattening. The latter is only covered by the manual script. The counter only
   cares about `block_num`, `trx_id`, op order, and type — all read from
   identical raw wrapper fields — so risk is low, but a field-shape drift in one
   API vs. the other (e.g. `block` vs `block_num` key) would silently produce
   different ids.
   *Recommendation:* run `compare_virtual_op_normalize.py` in CI against a
   stable testnet node, or add a one-time startup self-check on a sampled block
   through both Nectar's stream and the range fetcher.

3. **Low — the `next_operation_begin = null` pagination edge** (see "within one
   window" above). *Recommendation:* when `next_block_range_begin == cursor`
   (same block) and `next_operation_begin` is null, raise instead of rewinding
   to 0 — treat it as "no progress" rather than "restart the block".

4. **Info — skip-window data loss is intentional, not a bug.** After a per-block
   virtual fetch timeout, virtuals for `VIRTUAL_OPS_SKIP_BLOCKS` are skipped
   (lost, not duplicated). Fine for live operation; note it for any "gap fill
   after outage" runbook.

5. **Info — two concurrent monitor instances** (docker restart race) are handled
   by the same layers (upsert + exact-`group_id` short-circuit + the new guard +
   unique ledger index); there is a pre-existing micro-race on first ledger
   insert resolved by the unique index — not introduced by this branch.

---

## Tests and comparison script — do they match the guarantees?

Yes, with good coverage of the actual invariants (34/34 unit tests pass locally;
the 3 non-passing cases in this sandbox are environmental — live-node tests and
a Redis teardown fixture, `localhost:6379` refused).

- `tests/process/test_hive_ledger_replay.py` (13 tests): same-slot incl. fill
  suffix, cross-block/cross-realm/cross-trx non-collision, 0-based history
  mapping, gap allowance, later-booked drop (incl. the "op 3 behind booked op 4"
  case), exact-row preference, invalid-doc → `LedgerEntryCreationException`,
  sequential no-warn, and the filter's anchored regex shape.
- `tests/hive_models/test_virtual_ops_range.py`: mask gate (unknown name /
  bit ≥ 53 / `0`), normalization, identity, window-verification mismatch →
  disable, pagination. It pins the *current* `HIVE_VIRTUAL_OPS` protocol ids —
  a comment notes a wrong protocol id fails the mask test, which is the right
  coupling.
- `tests/hive_models/test_stream_ops.py`: the count-before-filter test is the
  key one (untracked op increments the index, tracked op gets `op_in_trx == 2`
  and the derived `group_id`).
- `scripts/compare_virtual_op_normalize.py` correctly encodes the invariants in
  prose and code: neither side copies the chain `op_in_trx` (the counter assigns
  it), the catch-up fetcher must hand *every* virtual op to the counter, and the
  chain `op_in_trx` differs inside user transactions because it also counts the
  signed ops.

---

## Bottom line

Virtual-op double processing is **very unlikely**.

- Within a session, fetch ranges are provably disjoint and the
  count-before-filter invariant makes `group_id` deterministic.
- Across restarts, the only trigger is a boundary-crossing re-read whose op list
  diverges between `enum_virtual_ops` and `get_ops_in_block`, and the new ledger
  guard then catches every direction of divergence except a shifted-up index
  landing on an unbooked slot (which the authors explicitly document and
  mitigate by policy: don't rescan history after changing the tracked-op set).

The strongest improvement available is deepening the per-window verification
from one block to a small sample, plus hardening the null
`next_operation_begin` edge.
