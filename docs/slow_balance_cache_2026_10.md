# Slow balance cache, October 2026

Pickup note for branch `fix/slow-balance-cache-scan`. The code change
is commit `45114ef7`. This note is the commit after it. Written so the
investigation can be resumed without re-reading the logs. Nothing here
has been deployed.

The question was whether the Graylog warning `Slow cache set` means the
Redis cache is slow, whether that work is getting worse, and whether a
small change can speed it up without changing balances.

## What the warning actually measured

Before this branch, `Slow cache set` did not time Redis.

`one_account_balance` in `src/v4vapp_backend_v2/accounting/account_balances.py`
starts a timer `_t0` at entry. Just before it stores the result it takes
`_t5` and passes `report_time=_t5 - _t0` into `set_cached_balance`. That
span is the whole miss path:

- Redis cache lookup (the miss itself)
- optional checkpoint lookup
- the Mongo aggregation and `to_list()`
- datetime conversion and Pydantic validation
- the Python merge and running totals
- `all_held_msats()` when the caller did not pass `in_progress`

`set_cached_balance` used to warn `Slow cache set: {report_time}` whenever
that number was over 1 second. The Redis `SET`, and the
`model_dump_json()` in front of it, happened after the timer. A line
`Slow cache set: 38.805s` meant the balance took 38.8 seconds to
calculate and was then stored. The Redis write was only in a debug line,
`one_account_balance cache set done in ...`.

So the warning was a slow balance rebuild that happened to end in a cache
write. It was not evidence that Redis was slow.

## What Graylog showed

Stream `Gad V4Vapp Logs` (`6a40b89b47ee258b6d0f6ed6`), query
`message:"Slow cache set"`, looked up on 2026-10-11. Counts are per
Asia/Jerusalem day (Graylog grouped `timestamp` by `1d` for that user):

| Day | Warnings |
| --- | --- |
| 2026-10-04 | 5 |
| 2026-10-05 | 20 |
| 2026-10-06 | 12 |
| 2026-10-07 | 9 |
| 2026-10-08 | 29 |
| 2026-10-09 | 26 |
| 2026-10-10 | 42 |
| 2026-10-11 through 06:37 UTC | 19 |

The count rose across the week. That is "more rebuilds over 1 second",
not a measured percentile of every balance call. Hits and sub-second
misses are debug logs and were not in this count.

Samples from the last day of that window:

- 1–6 seconds, `cp` keys, `admin-interface`. Legion External Lightning
  Payments, Treasury Lightning, Binance and MagiSwap Exchange Holdings,
  and some `v4vapp` accounts. These are "as of this minute" dashboard
  reads that did use the checkpoint flag.
- 13–39 seconds, `no_cp` keys, also `admin-interface`, clustered at
  00:03–00:04 UTC on 2026-10-11. `v4vapp` Customer Deposits Hive (32s and
  38s), Traded Deposits Hive (14s and 32s), VSC Liability (21s and 33s).
  The date part was `2026-10-10T2359Z`, which is yesterday's closed
  daily period. The same key was calculated twice at the same time.
- One `api-v2` line: `memess` VSC Liability, `date-live`, 1.3 seconds.
  That is the customer balance path, not the dashboard.

Every sampled key was still generation `v812`. The cache was not being
flushed by `invalidate_all_ledger_cache`. These were misses, then full
rebuilds.

The 30 second scans run in `admin-interface`, but they read the same
Mongo ledger the payment containers use. While one is in flight it can
slow other ledger reads and writes. They are not themselves on the
Lightning send path. The send path calls
`one_account_balance(account=cust_id)` with `as_of_date=None`, which is
the `date-live` key. That one is cached for 20 minutes unless the
account is invalidated. The `memess` line is that path on a miss.

## How a balance is calculated and cached

`one_account_balance` is the only writer that produces `Slow cache set`.
`create_checkpoint` calls it with `use_checkpoints=False` and
`as_of_date` set to the closed period end (`ledger_checkpoints.py`).
That is the `no_cp` / `T2359Z` cluster. The dashboard
`/lnd-info` route calls `latest_period_create_checkpoint` and then
`one_account_balance(..., as_of_date=datetime.now())`. Sanity checks do
the same pair for the server Hive accounts.

Cache key, built in `ledger_cache._make_cache_key`:

```
ledger:bal:v{generation}:{sub}:{name}:{account_type}-{contra|normal}:{cp|no_cp}:date-{live|YYYY-MM-DDTHHMMZ}-age-{none|Ns}
```

`cp` means the caller passed `use_checkpoints=True`. It does not mean a
checkpoint document was found. A live query passes the default
`use_checkpoints=True` but then skips the checkpoint, because the lookup
is gated on `as_of_date is not None`. The stored `cp:date-live` value is
therefore a full history scan, not a checkpoint plus a delta.

`as_of_date=None` is stored as the literal `live`, so the key is stable.
Callers that pass `datetime.now()` get a key truncated to the minute.
The next minute is a different key, so the 1200 second TTL never serves
that request. Within one minute, the second request should hit.

TTL constants in `ledger_cache.py` are all 1200 seconds
(`DEFAULT_TTL_SECONDS`, `LIVE_TTL_SECONDS`, `HISTORICAL_TTL_SECONDS`).
`docs/ledger_balance_cache.md` still says 60 seconds live and 300 seconds
historical, and it still describes a hashed key. Trust the code.

On a hit, `in_progress_msats` is refreshed from `all_held_msats()` (or
from the caller's `InProgressResults`). The expensive lines are what the
cache stores. On a miss the pipeline is `one_account_transactions_pipeline`.

That pipeline's first stage used to be only:

- `reversed` does not exist
- `conv_signed` exists
- `timestamp` is `$exists`, `$lte as_of`, a range, or `$gt checkpoint`

The account predicate lived inside `$facet`, as a separate debit view and
credit view. `$facet` sub-pipelines cannot use indexes. For an open-ended
query the first stage therefore handed the whole ledger to `$facet`,
which filtered to one account in memory, pushed every surviving line into
one document, and returned it. Python then sorts those lines and
recomputes running totals. `create_checkpoint` only keeps
`balances_net`, `balances_totals`, and `last_transaction_date` from that
object, so the line list is thrown away after the expensive build.

A checkpoint delta (`from_date` set) is different. The timestamp predicate
is already a window from the checkpoint to `as_of`. Those are the 1–6
second `cp` warnings. An age window is the same idea.

The test config defines the indexes this wants: `timestamp`,
`debit.name`, `debit.sub`, `credit.name`, `credit.sub`, and the compound
indexes `debit.account_type + debit.name + debit.sub` and the credit
equivalent (`tests/data/config/config.yaml`, collection `ledger`).
Production config is not in this repo, so those indexes were not confirmed
on the live database. Even without them, filtering before `$facet` avoids
running the facet over every other account's rows. With the compound
indexes, an open-ended query can index-scan one account.

Selective invalidation deletes `ledger:bal:v*:{sub}:{name}:*` with
`SCAN`. That matches the key layout. It does not bump the generation.
`SCAN` still walks the keyspace; that cost was not what these warnings
measured, and this branch does not change it.

There is an existing stale-write window. A rebuild that starts before a
ledger save, and finishes after selective invalidation, writes a balance
that does not contain the new row. Nothing is deleted because the key
did not exist yet. The value then lives until the TTL or the next
invalidation of that account. A 30 second rebuild makes that window
large. This branch does not close it. Making the rebuild shorter makes
the window shorter.

## What this branch changes

Three changes. None of them change the rows that come back, or the
running-total math.

1. Open-ended account queries put the same debit and credit predicates
   on the top-level `$match`, as `$or`. Open-ended means `from_date` is
   None and `age` is unset or zero: live queries, and as-of queries that
   are not checkpoint deltas. The `$facet` filters are unchanged, so a
   document that is both a debit and a credit to the same account is
   still emitted twice. Checkpoint deltas and age windows are left on the
   timestamp plan on purpose. For a one-day window, forcing the account
   index could be worse than the timestamp index, and those queries were
   not the 30 second problem.

2. Concurrent callers for the same account and the same query share one
   in-process calculation (`_share_balance_flight`). The flight key
   matches the cache identity except the generation, and it also splits
   `use_cache=True` from `use_cache=False`, so a forced recompute does
   not join a cached flight. The leader's task is shielded from the
   caller's cancellation. A waiter gets `model_copy(deep=False)` and then
   its own `in_progress_msats`, so two callers do not share the model
   object the hit path mutates. Nested balance lines are still shared.
   Callers only read them today. A failed calculation is removed from the
   flight map, and the next caller tries again. This is per process.
   `admin-interface` and `api-v2` do not share a flight. The duplicate
   30 second scans in the logs were both `admin-interface`, so an
   in-process flight covers the case that was observed.

3. The warning is split. `Slow balance calculation: ... lines=N
   checkpoint=... use_checkpoints=...` is the rebuild, logged from
   `one_account_balance` when `_t5 - _t0` exceeds 1 second. `Slow cache
   set` is now only the JSON encode plus the Redis write, and only when
   that exceeds 1 second. Debug `SET` lines include both `calc=` and
   `redis=` when a calc time was passed. After deploy, a remaining
   `Slow cache set` really is Redis or a huge JSON document. A remaining
   `Slow balance calculation` is still the Mongo plus Python rebuild.
   `lines=` is the number of balance lines returned, which says whether
   the cost is "this account's own history" or "the scan around it".

## What was left alone, and why

- `datetime.now()` was not remapped onto the `live` key. The dashboard
  passes `now` specifically so checkpoints apply. The payment path uses
  `as_of_date=None`, stores under `cp:date-live`, and does not use a
  checkpoint. Putting both results under `live` could serve a checkpoint
  balance to a payment balance check. The minute in the key is also an
  accidental freshness bound: the next minute misses and recomputes.
  A stable key with a 1200 second TTL would keep a stale write for 20
  minutes instead of about a minute.
- Checkpoint creation still uses `use_checkpoints=False` and still
  materialises every line. Switching it to a `$sum` pipeline would change
  the adder from Python `Decimal` to Mongo `$sum`. That can disagree at
  the last digit, and the result is then the base of every later
  checkpoint balance. Not worth it without a parity check against the
  live ledger.
- `all_account_balances_summary_pipeline` has the same shape: a wide
  `$match`, then `$facet`. It is the admin user-list path, not this
  warning. Same fix is available there later.
- Invalidation is still a `SCAN` per ledger save. Fine while the
  `ledger:` key count is small. It becomes its own problem if the
  keyspace grows.
- The generation-at-SET race described above is unchanged.

## How to check it later

Pytest for this change needs Redis on `localhost:6379`. On 2026-10-11
that port was closed on the machine where the branch was written, so
pytest could not start (`InternalConfig` connects to Redis during
setup). The pipeline shape and the single-flight helper were run with
the project virtualenv instead, outside pytest. Both passed:
open-ended pipelines contain `$or` equal to the facet account matches,
age and `from_date` pipelines do not, two waiters share one factory
call, and a failed flight can be retried.

When Redis is up:

```
.venv/bin/pytest tests/accounting/test_account_balances.py::test_one_account_transactions_pipeline tests/accounting/test_account_balances.py::test_share_balance_flight_runs_factory_once tests/accounting/test_account_balances.py::test_share_balance_flight_retries_after_failure -q
```

After deploy, the useful Graylog checks on the same stream are:

- `message:"Slow balance calculation"` for rebuilds still over 1 second.
  The `lines=` field separates a fat account from a fat scan. The
  `no_cp` / `T2359Z` lines should drop if the account `$match` is doing
  its job. If they stay at tens of seconds and `lines=` is huge, the
  cost is this account's own history, and the next step is a totals-only
  checkpoint build with a parity test, not another cache-key tweak.
- `message:"Slow cache set"` should be rare. If it is not, the JSON
  document or Redis is the problem.
- The same key should not appear twice in the same second from one
  container. Two containers can still overlap.
- Confirm the live `ledger` collection has the compound debit and credit
  indexes before expecting the planner to index-scan. `explain` on one
  open-ended `v4vapp` Customer Deposits query is the direct check.

The branch was not pushed.
