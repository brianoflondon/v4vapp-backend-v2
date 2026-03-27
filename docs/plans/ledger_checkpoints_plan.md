# Ledger Balance Checkpoint System

## Problem

The `one_account_balance` function runs a full MongoDB aggregation pipeline over all
ledger entries from inception for every balance query.  For long-running accounts
this is increasingly expensive.

## Proposed Solution

Store pre-calculated balance snapshots ("checkpoints") in a new MongoDB collection
`ledger_checkpoints`.  When a historical balance is requested the system:

1. Finds the most recent checkpoint whose `period_end ≤ as_of_date`.
2. Runs the aggregation pipeline only for the **delta** period
   (`timestamp > checkpoint.period_end` and `timestamp ≤ as_of_date`).
3. Offsets all pipeline-computed running totals by the checkpoint's stored
   net-balance values before returning the merged result.

## Data Model – `LedgerCheckpoint`

Stored in MongoDB collection **`ledger_checkpoints`**.

| Field | Type | Description |
|---|---|---|
| `account_name` | `str` | Name of the ledger account |
| `account_sub` | `str` | Sub-account identifier |
| `account_type` | `str` | `AccountType` enum value |
| `contra` | `bool` | Whether this is a contra account |
| `period_type` | `PeriodType` | `"daily"` / `"weekly"` / `"monthly"` |
| `period_end` | `datetime` | Exclusive upper-bound of the period |
| `balances_net` | `Dict[str, Decimal]` | Net amount per currency at `period_end` |
| `conv_totals` | `Dict[str, CheckpointConvSummary]` | Conversion running-total per currency |
| `last_transaction_date` | `datetime \| None` | Timestamp of last included transaction |
| `created_at` | `datetime` | When this checkpoint document was written |

Compound index: `(account_name, account_sub, account_type, period_type, period_end)`
with unique constraint so a single period is never duplicated.

## Period Boundaries

| Period | `period_end` calculation |
|---|---|
| Daily | `date(Y, M, D)` at `23:59:59.999999 UTC` |
| Weekly | Last day of ISO week (Sunday) at `23:59:59.999999 UTC` |
| Monthly | Last day of calendar month at `23:59:59.999999 UTC` |

## API

```python
# Create / upsert a checkpoint for a single account+period
await create_checkpoint(account, period_type, period_end)

# Retrieve the best checkpoint before a given date
checkpoint = await get_latest_checkpoint_before(account, as_of_date)

# Batch: create checkpoints for all accounts for completed periods
await build_checkpoints_for_period(period_type, as_of_date)

# Helpers to compute canonical period end dates
period_end_for_date(period_type, date) -> datetime
completed_period_ends_since(period_type, since, until) -> list[datetime]
```

## Integration with `one_account_balance`

New boolean parameter `use_checkpoints: bool = True`.

When `as_of_date` is explicitly provided **and** a checkpoint exists:

1. Delta pipeline runs with `from_date = checkpoint.period_end`.
2. After `AccountBalances` is built from pipeline results, for each currency `c`:
   - Add `checkpoint.balances_net[c]` to every line's `amount_running_total`.
   - Add `checkpoint.conv_totals[c]` to every line's `conv_running_total`.
3. Reconstructs `LedgerAccountDetails` with the adjusted balance lines.
4. Falls back to the full-history pipeline if the checkpoint is empty or unavailable.

## Pipeline Change

`all_account_balances_pipeline` gains a new `from_date: datetime | None = None`
parameter.  When set, the timestamp filter becomes:

```python
{"$gt": from_date, "$lte": as_of_date}
```

instead of the normal `{"$lte": as_of_date}`.

## Work Plan

- [x] Create this plan file
- [ ] Implement `ledger_checkpoints.py` (model + CRUD + batch builder)
- [ ] Add `from_date` to `all_account_balances_pipeline`
- [ ] Modify `one_account_balance` to call checkpoint logic
- [ ] Write `tests/accounting/test_ledger_checkpoints.py`
- [ ] Run existing test suite to check for regressions
