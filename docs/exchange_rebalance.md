# Exchange Rebalance System

This document describes the exchange rebalancing system that keeps Binance holdings balanced after customer conversions between Hive and Lightning.

## Overview

When a customer converts value between Hive and Lightning, the system's on-exchange holdings become imbalanced. The rebalance system corrects this by accumulating small pending amounts and executing a trade on Binance once a threshold is met.

The rebalance runs as a **background task** (`asyncio.create_task`) and never blocks or affects the customer's transaction. If a rebalance fails, the customer's conversion still succeeds.

---

## When Rebalancing Is Triggered

### Hive → Lightning (sell HIVE for BTC)

**File:** `src/v4vapp_backend_v2/conversion/hive_to_keepsats.py`

When a customer deposits HIVE/HBD to receive Lightning sats, the system needs to sell HIVE on Binance to replenish the BTC used for the Lightning payment. At the end of the conversion flow:

```python
asyncio.create_task(
    rebalance_queue_task(
        direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
        currency=from_currency,
        hive_qty=conv_result.to_convert_conv.hive,
        tracked_op=tracked_op,
    )
)
```

### Lightning → Hive (buy HIVE with BTC)

**File:** `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py`

When a customer sends Lightning sats to receive HIVE/HBD, the system needs to buy HIVE on Binance to replenish the HIVE sent to the customer:

```python
asyncio.create_task(
    rebalance_queue_task(
        direction=RebalanceDirection.BUY_BASE_WITH_QUOTE,
        currency=to_currency,
        hive_qty=conv_result.net_to_receive_conv.hive,
        tracked_op=tracked_op,
    )
)
```

> **Note:** Even when the customer requests HBD, the exchange always trades HIVE/BTC — Binance doesn't list HBD. The `hive_qty` is the HIVE equivalent of whatever the customer received.

---

## Flow Through the System

```
Customer conversion completes
        │
        ▼
rebalance_queue_task()              ← exchange_process.py
        │
        ├─ get_exchange_adapter()   ← reads config for testnet/mainnet
        │
        ▼
add_pending_rebalance()             ← exchange_rebalance.py
        │
        ├─ PendingRebalance.get_or_create()   ← load from MongoDB
        ├─ Update min thresholds from exchange
        ├─ Estimate quote value (qty × price)
        ├─ pending.add_pending(qty)           ← accumulate in memory
        │
        ▼
  pending.can_execute()?
        │
   ┌────┴────┐
   No        Yes
   │         │
   ▼         ▼
 save()    execute_rebalance_trade()
 return      │
             ├─ market_sell() or market_buy()
             ├─ pending.reset_after_execution()
             ├─ pending.save()
             ▼
          exchange_accounting()     ← exchange_process.py
             │
             ├─ Create LedgerEntry (EXCHANGE_CONVERSION)
             └─ Create LedgerEntry (EXCHANGE_FEES) if fee > 0
```

---

## Key Components

### `rebalance_queue_task()` — Orchestrator

**File:** `src/v4vapp_backend_v2/conversion/exchange_process.py`

Top-level coordinator that:
1. Gets the exchange adapter from config
2. Calls `add_pending_rebalance()` to accumulate and potentially execute
3. If a trade executed, calls `exchange_accounting()` to write ledger entries
4. Catches all exceptions — rebalance errors never propagate to the customer flow

### `PendingRebalance` — MongoDB-Persisted Accumulator

**File:** `src/v4vapp_backend_v2/conversion/exchange_rebalance.py`

A Pydantic model stored in the `pending_rebalances` MongoDB collection. One record exists per combination of `(base_asset, quote_asset, direction, exchange)`.

| Field | Description |
|---|---|
| `pending_qty` | Accumulated quantity of base asset waiting to be traded |
| `pending_quote_value` | Estimated value in quote asset (for threshold checks) |
| `min_qty_threshold` | Minimum lot size from exchange (updated on each call) |
| `min_notional_threshold` | Minimum notional value from exchange |
| `transaction_count` | Number of customer transactions accumulated |
| `transaction_ids` | List of transaction IDs for audit trail |
| `total_executed_qty` | Running total of all successfully executed trades |
| `execution_count` | Number of successful trade executions |

Key methods:
- `get_or_create()` — loads existing record from MongoDB or creates a new one
- `add_pending()` — adds qty and quote value, increments counters
- `can_execute()` — returns `(bool, reason)` checking against exchange minimums
- `reset_after_execution()` — clears pending amounts, keeps any remainder from partial fills

### `add_pending_rebalance()` — Main Entry Point

**File:** `src/v4vapp_backend_v2/conversion/exchange_rebalance.py`

The core function that:
1. Loads or creates a `PendingRebalance` record
2. Refreshes exchange minimum thresholds (gracefully handles connection errors)
3. Estimates the quote value for the new quantity (gracefully handles connection errors)
4. Adds the new amount to the pending record
5. Checks thresholds via `can_execute()`
6. If below threshold → saves to MongoDB and returns `RebalanceResult(executed=False)`
7. If threshold met → calls `execute_rebalance_trade()`, updates the pending record, saves the `RebalanceResult` to MongoDB

Error handling:
- `ExchangeBelowMinimumError` — logged as warning, returns non-executed result
- `ExchangeConnectionError` — logged as error, returns non-executed result with error
- Any other exception — logged with traceback, returns non-executed result with error

### `execute_rebalance_trade()` — Trade Dispatcher

Dispatches to `market_sell()` or `market_buy()` on the exchange adapter based on `pending.direction`. Uses the last `transaction_id` as the Binance `client_order_id` for traceability.

### `exchange_accounting()` — Ledger Writer

**File:** `src/v4vapp_backend_v2/conversion/exchange_process.py`

Creates two ledger entries after a successful trade:

1. **EXCHANGE_CONVERSION** — records the asset swap (debit/credit both to "Exchange Holdings" but in different currencies: HIVE vs MSATS)
2. **EXCHANGE_FEES** — records the trading fee (debit to "Exchange Fees Paid" expense, credit from "Exchange Holdings")

Uses the `trade_quote` from the order result for consistent pricing. Falls back to fetching a fresh market quote if the trade quote is unavailable.

---

## Threshold Logic (Why Trades Are Batched)

Binance enforces minimum order requirements:

- **Minimum lot size** — e.g., 1 HIVE (cannot trade fractional HIVE)
- **Minimum notional value** — e.g., 0.00001000 BTC (the order value in BTC must exceed this)

Many customer transactions are small (e.g., 5 HIVE, 10 HIVE). The rebalance system accumulates these until the combined amount exceeds both minimums, then executes a single trade.

Example sequence:

| Transaction | Qty Added | Pending Total | Min Lot | Min Notional | Action |
|---|---|---|---|---|---|
| Customer A sends 5 HIVE | 5 | 5 | 1 | 0.00001 BTC | Lot OK, check notional... below → **accumulate** |
| Customer B sends 20 HIVE | 20 | 25 | 1 | 0.00001 BTC | Both met → **execute sell of 25 HIVE** |

---

## Net Position and Netting

The system tracks buy and sell sides independently. When both sides have pending amounts, they can offset each other.

### `NetPosition` Model

Captures the relationship between opposing pending amounts:

- `sell_pending_qty=100, buy_pending_qty=60` → `net_qty=40` (direction: SELL)
- `sell_pending_qty=30, buy_pending_qty=30` → `net_qty=0` (balanced, no trade needed)

### `get_net_position()`

Calculates the net position by loading both SELL and BUY `PendingRebalance` records and computing: `net_qty = sell_pending - buy_pending`.

### `execute_net_rebalance()`

The preferred execution path — calculates the net position and executes only the residual trade. After execution, `_update_pending_after_net_execution()` clears the consumed side and reduces the other:

Example: `sell_pending=100, buy_pending=60, net=40 SELL`
- Execute sell of 40 HIVE
- Clear `buy_pending` entirely (it was fully offset)
- Reduce `sell_pending` by 100 (60 offset + 40 executed)

### `force_execute_pending()`

Bypasses threshold checks and attempts to execute whatever is pending. Use with caution — the trade will fail if the exchange rejects it for being below minimums.

---

## Failure Behavior

### Trade execution fails (e.g., connection error, zero price)

When `execute_rebalance_trade()` throws an exception inside `add_pending_rebalance()`:

1. The generic `except` handler catches it
2. Returns `RebalanceResult(executed=False, error=...)`
3. **The pending record is NOT saved** — the in-memory `add_pending()` changes are lost
4. The previously accumulated amount in MongoDB remains at its prior value
5. On the next customer transaction, accumulation resumes from the old base

**Impact:** The specific transaction's contribution to the pending pool is lost. The system self-heals on the next transaction — the lost quantity eventually gets re-accumulated.

### Threshold not met

When `can_execute()` returns `False`:

1. The pending record is saved to MongoDB with the new accumulated total
2. `RebalanceResult(executed=False, reason=...)` is returned
3. No trade is attempted — no risk of exchange errors

### Rebalance task itself throws

`rebalance_queue_task()` wraps everything in a try/except:

```python
except Exception as e:
    logger.error(
        f"Unexpected rebalance queuing failed: {e}",
        extra={"error": str(e), "group_id": tracked_op.group_id},
    )
```

Errors are logged but never propagate. The customer transaction is unaffected.

---

## MongoDB Collections

| Collection | Contents |
|---|---|
| `pending_rebalances` | One document per `(base_asset, quote_asset, direction, exchange)` — accumulates pending amounts |
| `rebalance_results` | One document per successful execution — audit trail of trades |

---

## Configuration

The exchange used for rebalancing is determined by `get_exchange_adapter()`, which reads the `exchange_config.default_exchange` from the config YAML. The adapter handles testnet vs mainnet selection based on the `exchange_config.testnet` flag.

---

## Files Reference

| File | Role |
|---|---|
| `src/v4vapp_backend_v2/conversion/exchange_rebalance.py` | Core rebalance logic: `PendingRebalance`, `add_pending_rebalance()`, netting functions |
| `src/v4vapp_backend_v2/conversion/exchange_process.py` | Orchestrator: `rebalance_queue_task()`, `exchange_accounting()` |
| `src/v4vapp_backend_v2/conversion/exchange_protocol.py` | Abstract protocol, base adapter, factory function |
| `src/v4vapp_backend_v2/conversion/binance_adapter.py` | Concrete Binance adapter |
| `src/v4vapp_backend_v2/conversion/hive_to_keepsats.py` | Triggers SELL rebalance |
| `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py` | Triggers BUY rebalance |
| `src/v4vapp_backend_v2/helpers/binance_extras.py` | Low-level Binance SDK wrappers |
