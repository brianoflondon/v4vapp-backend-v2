# Ledger Audit — May 2026

Reviewed by GitHub Copilot acting as double-entry bookkeeping accountant.
**Convention throughout:** Debit increases Assets/Expenses; Credit increases Liabilities/Revenue/Equity.

---

## Scope

Files reviewed:
- `src/v4vapp_backend_v2/process/process_hive.py`
- `src/v4vapp_backend_v2/process/process_custom_json.py`
- `src/v4vapp_backend_v2/process/process_orders.py`
- `src/v4vapp_backend_v2/process/process_forward_events.py`
- `src/v4vapp_backend_v2/process/hold_release_keepsats.py`
- `src/v4vapp_backend_v2/conversion/hive_to_keepsats.py`
- `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py`
- `src/v4vapp_backend_v2/accounting/ledger_account_classes.py`

---

## ✅ Entries confirmed correct

| Location | Transaction | Debit | Credit |
|---|---|---|---|
| `process_hive.py` | Server → Treasury | Treasury Hive (Asset ↑) | Customer Deposits Hive (Asset ↓) |
| | Treasury → Server | Customer Deposits Hive (Asset ↑) | Treasury Hive (Asset ↓) |
| | Funding → Treasury | Treasury Hive (Asset ↑) | Owner Loan Payable (Liability ↑) |
| | Treasury → Funding | Owner Loan Payable (Liability ↓) | Treasury Hive (Asset ↓) |
| | Treasury/Server → Exchange | Exchange Holdings (Asset ↑) | Treasury/Customer Deposits Hive (Asset ↓) |
| | Exchange → Treasury | Treasury Hive (Asset ↑) | Exchange Holdings (Asset ↓) |
| | Expense payment | Expense Account (↑) | Treasury Hive (Asset ↓) |
| | Customer deposit (`CUSTOMER_HIVE_IN`) | Customer Deposits Hive (Asset ↑) | VSC Liability customer (↑) |
| | Customer withdrawal (`CUSTOMER_HIVE_OUT`) | VSC Liability customer (↓) | Customer Deposits Hive (Asset ↓) |
| | Suspicious account (2nd leg) | VSC Liability customer (↓) | VSC Liability v4vapp.sus (↑) |
| `process_custom_json.py` | Internal sats transfer | VSC Liability sender (↓) | VSC Liability receiver (↑) |
| | Fee revenue recognition (2-step) | VSC Liability server (↓) | Fee Income Revenue (↑) |
| `hold_release_keepsats.py` | Hold keepsats | VSC Liability customer (↓) | VSC Liability "keepsats" escrow (↑) |
| | Release keepsats | VSC Liability "keepsats" escrow (↓) | VSC Liability customer (↑) |
| `process_forward_events.py` | Routing fee | External Lightning Payments (Asset ↑) | Routing Fee Income (Revenue ↑) |
| `hive_to_keepsats.py` | Conversion + contra | Correct cross-currency offsets | |
| `keepsats_to_hive.py` | Conversion + contra + reclassify | Correct cross-currency offsets | |

---

## 🔧 Fixes Applied (May 2026)

### Fix 1 — `CONSUME_CUSTOMER_KEEPSATS` missing `contra=True`

**File:** `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py`
**Status:** ✅ Fixed in code; affected DB records patched with `updateMany`

Added `contra=True` to the credit `AssetAccount` in the `CONSUME_CUSTOMER_KEEPSATS` ledger entry
(the `is_lndtohive` path). The `contra` flag has no effect on signed amount maths but does affect
MongoDB aggregation grouping — without it, `Converted Keepsats Offset / from_keepsats` split into
two separate buckets, causing phantom rows in `all_account_balances` consumers.

DB patch applied:
```javascript
db.getCollection('ledger').updateMany(
  {"credit.name": "Converted Keepsats Offset", "credit.sub": "from_keepsats", "credit.contra": {$ne: true}},
  {$set: {"credit.contra": true}}
)
```

---

### Fix 2 — Inverted comments and descriptions on `FillOrder` entries

**File:** `src/v4vapp_backend_v2/process/process_orders.py`
**Status:** ✅ Fixed in code (comments and `description` strings only; accounting maths were already correct)

Corrected buyer/seller perspective in inline comments and `description` f-strings:

| Entry | Before | After |
|---|---|---|
| `buyer_entry` debit comment | "HIVE paid" | "HIVE received" |
| `buyer_entry` credit comment | "HBD received" | "HBD paid" |
| `buyer_entry` description | `pays {HIVE} for {HBD}` | `pays {HBD} for {HIVE}` |
| `seller_entry` debit comment | "HBD delivered" | "HBD received" |
| `seller_entry` credit comment | "HIVE received" | "HIVE delivered" |
| `seller_entry` description | `receives {HIVE} for {HBD}` | `pays {HIVE} for {HBD}` |
| Net entry debit comment (buyer-only branch) | "HIVE paid" | "HIVE received" |
| Net entry credit comment (buyer-only branch) | "HBD received" | "HBD paid" |

---

### Fix 3 — Docstring mismatch in `keepsats_to_hive.py` step 4

**File:** `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py` module docstring
**Status:** ✅ Fixed (documentation only)

Corrected step 4 to show that `cust_id` (not `server`) is debited for fee income.

---

## ❌ Issues Found (original, all now resolved)

### Issue 1 — Bug: `CONSUME_CUSTOMER_KEEPSATS` missing `contra=True`

**File:** `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py` ~L275
**Affects:** `is_lndtohive` path only (direct LND → Hive conversion)

Every other usage of the `Converted Keepsats Offset` account across the codebase
passes `contra=True`. This one entry did not. Without the flag the account was treated
as a regular asset, causing it to aggregate into a separate MongoDB bucket from the
correctly-flagged entries, producing phantom split rows in balance sheet queries.

---

### Issue 2 — Inverted comments on `FillOrder` entries

**File:** `src/v4vapp_backend_v2/process/process_orders.py`
**Affects:** `FILL_ORDER_BUY` and `FILL_ORDER_SELL` ledger entry descriptions / inline comments

The accounting debits/credits were economically correct, but the inline comments
and `description` strings had the economic direction reversed:

| Entry | Comment/description said | Economic reality |
|---|---|---|
| `buyer_entry` debit | "Buyer debits deposits for HIVE paid" | Buyer **receives** HIVE (`open_pays`) |
| `buyer_entry` credit | "Buyer credits deposits for HBD received" | Buyer **pays** HBD (`current_pays`) |
| `seller_entry` debit | "Seller debits escrow for HBD delivered" | Seller **receives** HBD (`current_pays`) |
| `seller_entry` credit | "Seller credits deposits for HIVE received" | Seller **delivers** HIVE (`open_pays`) |

---

### Issue 3 — Docstring mismatch: `keepsats_to_hive.py` step 4

**File:** `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py` module docstring
**Affects:** documentation only

The module docstring incorrectly stated the server was debited for fee income; the code
actually debits `cust_id`.

---

## Notes on `RECLASSIFY_VSC_SATS` (`r_vsc_sats`) entries

These entries (seen in DB as `ledger_type: 'r_vsc_sats'`) are **correctly formed**:

```
Debit:  VSC Liability (devser.v4vapp)              — Liability ↓ (reduces server sats owed)
Credit: Converted Keepsats Offset (from_keepsats, contra=True) — Contra Asset credited (offset ↑)
```

Both `debit_amount_signed` and `credit_amount_signed` appear negative in the stored
documents. This is expected:
- Debiting a Liability (credit-normal account) → signed negative ✅
- Crediting a contra Asset (treated as asset by type, but offset direction reversed) → signed negative ✅

**Balance sheet effect of r_vsc_sats entries:**
Total Assets and Total Liabilities are each reduced by the same amount → balance sheet remains balanced.
Specifically: `VSC Liability (devser.v4vapp)` decreases (good — server no longer owes the
server-held sats) and `Converted Keepsats Offset / from_keepsats` increases its positive balance
(partially offsetting `Treasury Lightning / from_keepsats` which is negative by the same magnitude).
The pair `Converted Keepsats Offset / from_keepsats` (+11,368) and `Treasury Lightning / from_keepsats` (-11,368)
netting to zero is the intended and correct result.

---

## Fix 4 — Accumulated fractional-msats rounding drift in VSC Liability (May 2026)

**Branch:** `fix/deal-with-residual-rounding-issues`

### Root cause

HIVE/HBD → msats conversions produce a fractional millisatoshi value:

```
53.999 HIVE × 81.565 sats/HIVE × 1000 = 4,544,449.839963 msats
```

Before this fix, ledger entries in the `CONV_HIVE_TO_KEEPSATS`, `CONV_CUSTOMER`,
`CONV_KEEPSATS_TO_HIVE`, `CONSUME_CUSTOMER_KEEPSATS`, and `RECLASSIFY_VSC_SATS`
flows stored the full-precision fractional value as their `debit_amount` / `credit_amount`.
The corresponding Lightning Network payments and on-chain keepsats transfers use
**integer** millisatoshis, so the offsetting ledger entries use an integer amount.
Each transaction leaves a tiny sub-msat residual in `VSC Liability (v4vapp)` — over
2,500+ transactions on the live server account this accumulated to **188.23 msats**.

### Changes

| File | Change |
|---|---|
| `src/v4vapp_backend_v2/helpers/crypto_conversion.py` | Added `msats_rounded` property to `CryptoConv` (ROUND_HALF_UP to integer Decimal, parallel to existing `sats_rounded`) |
| `src/v4vapp_backend_v2/conversion/hive_to_keepsats.py` | `CONV_HIVE_TO_KEEPSATS.debit_amount` and `CONV_CUSTOMER.credit_amount` now use `conv.msats_rounded` |
| `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py` | `CONV_KEEPSATS_TO_HIVE.credit_amount`, `CONSUME_CUSTOMER_KEEPSATS.debit/credit_amount`, and `RECLASSIFY_VSC_SATS.debit/credit_amount` now use `conv.msats_rounded` |
| `src/v4vapp_backend_v2/models/invoice_models.py` | `update_conv`: `float(amount_msat)` → `int(amount_msat)` |
| `src/v4vapp_backend_v2/models/payment_models.py` | `update_conv`: `float(fee_msat)` and `float(value_msat + fee_msat)` → `int(...)` |

### Test coverage

New test file: `tests/helpers/test_msats_rounding.py` (17 tests)

- `TestMsatsRoundedProperty` — unit tests for the new property (zero, fractions, integers, large values, return type)
- `TestCryptoConversionIntegerMsats` — verifies integer MSATS inputs stay integer; HIVE inputs produce fractional; `msats_rounded` eliminates fractional drift; `int()` vs `float()` inputs for LND values
- `TestAccumulatedRoundingDrift` — simulation tests demonstrating that without rounding the residual grows with transaction count, while with `msats_rounded` it is bounded to ≤ 0.5 msats per transaction

### Remaining residual in historical data

The **188 msats** in the live VSC Liability account as of 2026-05-08 remains as a
historical artefact. It can be corrected with a manual adjustment ledger entry if
desired. All **new** transactions from this fix forward will use rounded integer
msats and will not introduce further fractional drift.

### Why ROUND_HALF_UP (not floor/truncation)

The Lightning Network itself can send any integer msats value — there is no requirement
to truncate. Using ROUND_HALF_UP is consistent with `sats_rounded` and gives the customer
the nearest-integer approximation of the calculated value. The residual per transaction
is bounded to ±0.5 msats (whichever rounding direction is closest), which is negligible
and symmetric across many transactions.
