# Binance Convert (Swap) Adapter

This document describes the `BinanceSwapAdapter`, an alternative to the existing `BinanceAdapter` that uses Binance's **Convert API** instead of spot market orders.

## Motivation

The existing `BinanceAdapter` places market orders on the spot order book (`POST /api/v3/order`). This works well but has limitations:

- **Higher minimums** — spot trading enforces LOT_SIZE and MIN_NOTIONAL filters that can reject small rebalance trades
- **Lot size rounding** — HIVE must be traded in whole numbers on the spot market, losing fractional amounts
- **Explicit fees** — trading fees are deducted separately (in BNB or the trade asset) and must be tracked

The Convert API (`/sapi/v1/convert/*`) offers a simpler model:

- **Lower minimums** — convert pairs often accept smaller amounts than spot
- **No lot size constraints** — the API handles quantity precision internally
- **Fees hidden in the rate** — the quoted rate includes the spread/fee, so there are no separate fee deductions to track
- **Guaranteed execution price** — you see the exact rate before accepting

The trade-off is that the rate may be slightly worse than a pure market order (the spread compensates Binance), and there is **no testnet support**.

---

## How It Works

The Convert API uses a two-step quote-then-accept flow:

```
                    ┌─────────────────────┐
                    │  send_quote_request  │
                    │  POST /sapi/v1/     │
                    │  convert/getQuote    │
                    └────────┬────────────┘
                             │
                    returns quoteId, ratio,
                    fromAmount, toAmount,
                    validTimestamp
                             │
                             ▼
                    ┌─────────────────────┐
                    │    accept_quote      │
                    │  POST /sapi/v1/     │
                    │  convert/acceptQuote │
                    └────────┬────────────┘
                             │
                    returns orderId,
                    orderStatus (PROCESS)
                             │
                             ▼
                    ┌─────────────────────┐
                    │   order_status       │
                    │  GET /sapi/v1/      │
                    │  convert/orderStatus │
                    │  (poll until done)   │
                    └────────┬────────────┘
                             │
                    returns orderStatus
                    (SUCCESS / FAIL),
                    fromAmount, toAmount,
                    ratio
                             │
                             ▼
                    ExchangeOrderResult
```

### Step 1 — Request a Quote

Call `send_quote_request(fromAsset, toAsset, fromAmount=..., validTime="10s")`.

The response includes:
- `quoteId` — unique identifier, required to accept the quote
- `ratio` — the conversion rate (toAsset per fromAsset)
- `fromAmount` / `toAmount` — the exact amounts that will be exchanged
- `validTimestamp` — millisecond timestamp when the quote expires (typically 10s)

You must specify either `fromAmount` (how much you want to spend) or `toAmount` (how much you want to receive), not both.

> **Important:** A `quoteId` is only returned if you have sufficient funds. If balance is too low, the API returns an error.

### Step 2 — Accept the Quote

Call `accept_quote(quoteId)` before the quote expires.

The response returns an `orderId` and an initial `orderStatus` of `"PROCESS"`.

### Step 3 — Poll for Completion

Call `order_status(orderId=...)` in a loop until `orderStatus` reaches a terminal state:

| Status | Meaning |
|---|---|
| `PROCESS` | Order is being processed |
| `ACCEPT_SUCCESS` | Quote accepted, conversion in progress |
| `SUCCESS` | Conversion completed |
| `FAIL` | Conversion failed |

The final `SUCCESS` response includes `fromAmount`, `toAmount`, `fromAsset`, `toAsset`, and `ratio` — all reflecting the actual executed conversion.

---

## Architecture

### Class Hierarchy

```
BaseExchangeAdapter (ABC)          ← exchange_protocol.py
    │
    ├── BinanceAdapter             ← binance_adapter.py (spot market orders)
    │
    └── BinanceSwapAdapter         ← binance_swap_adapter.py (Convert API)
```

`BinanceSwapAdapter` implements the same `BaseExchangeAdapter` interface (`market_sell`, `market_buy`, `get_balance`, etc.), so it can be used as a drop-in replacement anywhere the protocol is expected.

### Data Models

Three Pydantic models represent the Convert API responses:

| Model | Purpose |
|---|---|
| `ConvertQuoteResult` | Wraps the `send_quote_request` response (quoteId, ratio, amounts, expiry) |
| `ConvertAcceptResult` | Wraps the `accept_quote` response (orderId, initial status) |
| `ConvertOrderStatus` | Wraps the `order_status` response (final amounts, ratio, terminal status) |

All models store the `raw_response` dict for debugging and have a `from_binance_response()` class method for construction.

### Exception Types

| Exception | When Raised |
|---|---|
| `ExchangeConnectionError` | API call fails (network, auth, unknown error) |
| `ExchangeBelowMinimumError` | Amount is below the Convert pair minimum |
| `ExchangeQuoteExpiredError` | Quote expired before `accept_quote` was called |
| `ExchangeError` | Order reached `FAIL` status, or polling timed out |

---

## Key Differences from BinanceAdapter

| Aspect | BinanceAdapter (spot) | BinanceSwapAdapter (convert) |
|---|---|---|
| **API endpoint** | `POST /api/v3/order` | `/sapi/v1/convert/*` (3 calls) |
| **Testnet support** | ✅ Yes | ❌ No — mainnet only |
| **Quantity rounding** | Required (HIVE → whole numbers) | Not needed (API handles precision) |
| **Fees** | Explicit (BNB or trade asset), tracked as `fee_msats` | Hidden in rate, `fee_msats = 0` |
| **Price guarantee** | None (market order, fills at book price) | Quoted rate is guaranteed if accepted in time |
| **Minimum detection** | LOT_SIZE + MIN_NOTIONAL from exchange_info | `fromAssetMinAmount` / `toAssetMinAmount` from convert pairs |
| **`exchange_name`** | `"binance"` or `"binance_testnet"` | `"binance_convert"` |
| **`client_order_id`** | Supported (max 36 chars) | Not supported by Convert API (ignored) |

### Fee Handling

The `BinanceAdapter` extracts commission from order fills and converts it to msats. The `BinanceSwapAdapter` always reports zero fees — the cost is embedded in the conversion ratio. Monitor the ratio relative to the spot mid-price to gauge the effective fee.

### Trade Quote Construction

Both adapters build a `QuoteResponse` (trade_quote) reflecting the actual execution rate. The swap adapter derives the trade rate from the Convert `ratio` field rather than from spot fill prices.

---

## Testnet Limitation

The Binance Convert API does **not** have a testnet/sandbox environment. The `BinanceSwapAdapter` constructor accepts `testnet=True` for interface compatibility but forces `testnet=False` internally and logs a warning:

```python
adapter = BinanceSwapAdapter(testnet=True)
# WARNING: Binance Convert API does not support testnet.
# Using mainnet credentials instead.
```

All API calls go to the live Binance endpoint using mainnet credentials from config.

---

## Interactive Testing Script

A script is provided at `scripts/test_binance_swap.py` for manual live testing:

```bash
python scripts/test_binance_swap.py --config devhive.config.yaml
```

### Menu Options

| Option | Description |
|---|---|
| **1 — Show balances** | Displays HIVE, BTC, HBD, SATS balances |
| **2 — Show minimums** | Queries `list_all_convert_pairs` for HIVE/BTC min amounts |
| **3 — Request quote only** | Gets a quote (sell or buy direction) without executing — lets you inspect the rate |
| **4 — Sell HIVE for BTC** | Requests a quote, shows it, asks for confirmation, then executes |
| **5 — Buy HIVE with BTC** | Same flow in the buy direction |
| **6 — Round trip** | Sells 10 HIVE then buys 10 HIVE back, showing the spread cost |

Every trade operation shows the quoted rate and requires explicit `y` confirmation before executing. The round-trip option shows the net HIVE and sats change to quantify the effective spread cost.

---

## Switching the Rebalance System to Use Convert

To use the swap adapter for rebalancing instead of the spot adapter, update the factory function in `exchange_protocol.py`:

```python
def get_exchange_adapter(exchange_name: str | None = None) -> BaseExchangeAdapter:
    # ... existing config reading ...

    if provider_name == "binance":
        from v4vapp_backend_v2.conversion.binance_swap_adapter import BinanceSwapAdapter
        return BinanceSwapAdapter(testnet=testnet)
```

Or register it as a separate provider name (e.g., `"binance_convert"`) and set `default_exchange: binance_convert` in the config YAML to switch without code changes.

Since `BinanceSwapAdapter` implements the same `market_sell()` / `market_buy()` interface, the rebalance orchestrator (`exchange_process.py`) requires no changes.

---

## Convert API Rate Limits

| Endpoint | Weight | Type |
|---|---|---|
| `list_all_convert_pairs` | 3000 | IP |
| `send_quote_request` | 200 | UID |
| `accept_quote` | 500 | UID |
| `order_status` | 100 | UID |
| `get_convert_trade_history` | 3000 | UID |

The quote request + accept + status poll sequence costs ~800 UID weight per trade. The `list_all_convert_pairs` call is heavy (3000 IP) — the adapter calls it in `get_min_order_requirements()`, so avoid calling that in a tight loop.

---

## Files Reference

| File | Role |
|---|---|
| `src/v4vapp_backend_v2/conversion/binance_swap_adapter.py` | `BinanceSwapAdapter` — Convert API adapter |
| `src/v4vapp_backend_v2/conversion/binance_adapter.py` | `BinanceAdapter` — original spot market order adapter |
| `src/v4vapp_backend_v2/conversion/exchange_protocol.py` | `BaseExchangeAdapter`, `ExchangeProtocol`, factory, shared models |
| `src/v4vapp_backend_v2/helpers/binance_extras.py` | Low-level Binance SDK wrappers (used by both adapters for balances/prices) |
| `scripts/test_binance_swap.py` | Interactive live testing script |

---

## Binance API Documentation References

- [Convert API Introduction](https://developers.binance.com/docs/convert/Introduction)
- [List All Convert Pairs](https://developers.binance.com/docs/convert/market-data/List-all-convert-pairs)
- [Send Quote Request](https://developers.binance.com/docs/convert/trade/Send-quote-request)
- [Accept Quote](https://developers.binance.com/docs/convert/trade/Accept-Quote)
- [Order Status](https://developers.binance.com/docs/convert/trade/Order-Status)
- [Get Convert Trade History](https://developers.binance.com/docs/convert/trade/Get-Convert-Trade-History)
