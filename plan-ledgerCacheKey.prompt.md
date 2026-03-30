## Plan: Fully Human-Readable Ledger Cache Keys + `use_checkpoints` Segment

Two problems to fix in one change:

1. **`use_checkpoints` not in cache  a result computed with checkpoints enabledkey** 
   can be served to a caller that requested `use_checkpoints=False`, causing silently
   wrong running totals.

2. **Hash suffix is  the current key ends with a 16-char SHA256 fragmentopaque** 
   (`ledger:bal:v{gen}:{sub}:{name}:{hash}`) that is unreadable in Redis tooling.

**Fix:** Replace the hash entirely. Encode every query dimension as an explicit,
human-readable colon-delimited segment. The ISO datetime colons must be sanitised
 `Z`).

New key format:
```
ledger:bal:v{gen}:{sub}:{name}:{account_type}:{contra}:{date_part}:{age_part}:cp{0|1}
```
Where:
- `{date_part}` = `live`  OR  `2026-02-28T2359Z`  (minute-truncated UTC, colons removed from time)
- `{age_part}` = `none`  OR  `3600s`  (seconds with `s` suffix for readability)
- `cp1` / `cp0` = `use_checkpoints` flag

Examples:
```
ledger:bal:v3:keepsats:VSC Liability:Liability:False:2026-02-28T2359Z:none:cp1
ledger:bal:v3:keepsats:VSC Liability:Liability:False:live:none:cp1
ledger:bal:v3:keepsats:VSC Liability:Liability:False:live:86400s:cp0
```

---

**Steps**

### Phase  `_make_cache_key` in `ledger_cache.py`1 

1. Add `use_checkpoints: bool = True` parameter to `_make_cache_key`.

2. Replace the hash construction entirely. Build each segment explicitly:
   - `date_part`: `"live"` when `as_of_date is None`; otherwise format as
     `as_of_date.replace(second=0, microsecond=0).strftime("%Y-%m-%dT%H%MZ")`
     (drops seconds, strips timezone colon, always UTC).
   - `age_part`: `"none"` when `age is None or age.total_seconds() <= 0`; otherwise
     `f"{int(age.total_seconds())}s"`.
   - `cp_part`: `"cp1"` when `use_checkpoints` is `True`, `"cp0"` otherwise.
 `"true"` or `"false"`.

3. Return:
   `f"ledger:bal:v{generation}:{account.sub}:{account.name}:{account.account_type.value}:{contra_part}:{date_part}:{age_part}:{cp_part}"`

4. Remove the `hashlib` import from `ledger_cache.py` if it is no longer used elsewhere.

### Phase  Propagate `use_checkpoints` parameter2 

5. Add `use_checkpoints: bool = True` to `get_cached_balance` and `set_cached_balance`.
   Pass it through to `_make_cache_key` in both.

6. In `one_account_balance` (`account_balances.py`):
   - Pass `use_checkpoints=use_checkpoints` to `get_cached_balance` (line ~382).
   - Pass `use_checkpoints=use_checkpoints` to `set_cached_balance` (line ~550).

### Phase  `invalidate_ledger_cache` scan pattern3 

7. Update the scan glob patterns in `invalidate_ledger_cache` from:
   `ledger:bal:v*:{sub}:{name}:*`
   to:
   `ledger:bal:v*:{sub}:{name}:*`
 **no change needed** because the existing trailing `*` already matches all the   
   new segments. Verify this in tests.

### Phase  Tests (`tests/accounting/test_ledger_cache.py`)4 

8. Update `test_set_and_get_cached_balance`:
   - Assert the stored Redis key contains the human-readable segments
     (account_type, contra, date_part, age_part, cp-flag) by scanning Redis keys
     and checking the key string directly.
   - Assert `get_cached_balance(..., use_checkpoints=True)` returns the stored value.
   - Assert `get_cached_balance(..., use_checkpoints=False)` returns `None`
     (different key, no cross-contamination).

9. Update `test_selective_invalidation_respects_account_filters`:
   - Confirm the SCAN pattern `ledger:bal:v*:{sub}:{name}:*` still matches and
     deletes keys for both `cp0` and `cp1` variants.

10. Add `test_checkpoint_flag_produces_distinct_cache_keys`:
    - Store one result with `use_checkpoints=True`, another with `use_checkpoints=False`
      for the same account/date.
    - Assert each lookup returns its own value (no bleed-through).

11. Add `test_cache_key_is_fully_human_readable`:
    - Call `set_cached_balance` for a known account + date.
    - Scan Redis for `ledger:bal:*` keys and assert the matching key:
      - Does NOT contain a 16-char hex sequence (no hash fragment).
      - DOES contain the account name, account_type string, `cp1` or `cp0`, and
        the expected date string.

---

**Relevant files**

- `src/v4vapp_backend_v2/accounting/ledger_cache. `_make_cache_key` (line ~56),py` 
  `get_cached_balance` (line ~191), `set_cached_balance` (line ~217),
  `invalidate_ledger_cache` (line ~124)
- `src/v4vapp_backend_v2/accounting/account_balances. `one_account_balance`,py` 
  cache read line ~382, cache write line ~550
- `tests/accounting/test_ledger_cache. all cache testspy` 

---

**Verification**

1. `uv run pytest tests/accounting/test_ledger_cache.py - all tests pass.v` 
2. `uv run pytest tests/accounting/ - no regressions.v` 
3. After a balance page load, run `redis-cli keys "ledger:bal:*"` and confirm every
   key is fully human-readable (no hex fragments, all segments visible).

---

**Decisions**

- **No hash at  fully human-readable. The key uniqueness is guaranteed by theall** 
  combination of all explicit segments; no hash needed for correctness.
- **Date format** `%Y-%m-%dT%H%MZ` (e.g. `2026-02- colons removed from28T2359Z`) 
  time part to avoid ambiguity with the `:` key separator; always UTC so no `+00:00`
  suffix needed.
- **Age format** `{n}s` (e.g. ` `s` suffix makes units obvious in Redis tooling.86400s`) 
- **`contra_part`** uses lowercase `true`/`false` to match Python bool repr lowercased.
- **`invalidate_ledger_cache`** scan pattern `ledger:bal:v*:{sub}:{name}:*` unchanged 
  trailing `*` already covers all the new trailing segments.
- **`hashlib` import  remove if and only if nothing else in `ledger_cache.py`removal** 
  uses it; check before removing.
- Default `use_checkpoints=True` everywhere to preserve existing call-site behaviour.
- Out of scope: `invalidate_all_ledger_cache` (bumps generation, clears everything),
  `get_cache_generation` (unaffected).
---
