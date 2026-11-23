# Magi / Altera as a full exchange path, with Binance routing and arb watch

| Field | Value |
|---|---|
| **Date** | 2026-11-23 |
| **Status** | Draft |
| **Primary code** | `src/v4vapp_backend_v2/conversion/magi_adapter.py`, `exchange_protocol.py`, `exchange_process.py`, `exchange_rebalance.py` |
| **Related** | [exchange_rebalance.md](../exchange_rebalance.md), [binance_convert_swap_adapter.md](../binance_convert_swap_adapter.md), [magisats.md](../magisats.md) |

Customer Hive ↔ Lightning conversion already rebalances on Binance Convert (`BinanceSwapAdapter`). Magi / Altera is a native Hive DEX that can swap HIVE or **HBD** directly into MagiSats. This plan makes Magi a complete `ExchangeProtocol` venue, routes each rebalance to Magi or Binance by live quote, and records the spread even when Magi is not used.

Customer conversion is unchanged. Only the hedge trade after the customer fill is affected.

---

## Goal

1. Fill in `MagiAdapter` so it can quote, check minimums, read balances, and execute HIVE/HBD ↔ MagiSats swaps.
2. Keep Binance Convert as the default Lightning/BTC venue.
3. On each executable rebalance, quote both venues and take Magi when it wins by a configured edge and the size fits the pool.
4. Log Magi vs Binance vs USDT-triangle quotes on every attempt (arb watch), with a later optional auto-arb path.
5. Hold tradable Magi inventory on a dedicated Hive exchange account, not on the customer-facing server Magi address.

---

## How Altera swap works

Altera ([alterawallet.com](https://alterawallet.com/)) is the Magi wallet UI. Swaps are non-custodial Hive `custom_json` `id=vsc.call`. This repo already parses them as `VSCSwapPayload`.

| Role | Contract |
|---|---|
| Magi BTC ledger (MagiSats `transfer`) | `vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d` |
| DEX **router** (`execute` / swap) | `vsc1Brvi4YZHLkocYNAFd7Gf1JpsPjzNnv4i45` |

HBD is the hub. There is no HIVE/BTC pool. Live pools (indexer `dex_pool_registry`):

| Pair | Pool |
|---|---|
| HBD / HIVE | `vsc1BoaniA5HW56GuQy6pVdoZfMcVaaDfnC8kp` |
| BTC / HBD | `vsc1BVb95YKRHAEy24XgRSaW4L6d9vB88AdwjM` |
| plus HBD/ETH, HBD/USDC, others | unused for v4vapp rebalance |

HIVE→BTC is two hops (HIVE→HBD→BTC). HBD→BTC is one hop.

A swap signed by Hive account `alice`:

```json
{
  "net_id": "vsc-mainnet",
  "caller": "hive:alice",
  "contract_id": "vsc1Brvi4YZHLkocYNAFd7Gf1JpsPjzNnv4i45",
  "action": "execute",
  "payload": "{\"type\":\"swap\",\"version\":\"1.0.0\",\"asset_in\":\"HBD\",\"asset_out\":\"BTC\",\"amount_in\":\"15000\",\"min_amount_out\":\"23000\",\"recipient\":\"hive:alice\"}",
  "rc_limit": 10000,
  "intents": [{"type": "transfer.allow", "args": {"limit": "15.000", "token": "hbd"}}]
}
```

Units are integers with no decimal point: HIVE/HBD × 10³, BTC in sats. `transfer.allow` pulls **native** Hive/HBD from the signer. BTC out is MagiSats on `hive:<recipient>`.

Omit `destination_chain`. That field unwraps to L1 Bitcoin. MagiSats stay on Magi.

Quote is local from pool reserves (Altera wallet function `ly`). Indexer: `https://indexer.magi.milohpr.com/v1/graphql` (`dex_pool_liquidity`, `dex_pool_swap_events`). Confirmation: `dex_pool_swap_events` by `indexer_tx_hash`, plus `btc_mapping_transfer_events` when BTC moves.

### AMM (must match Altera)

Constants from Altera JS: `fee_bps = 8`, `L2 = 20000`, `pg = 10000`, `F2 = 625`, mode `OA = "off"`.

```
gross     = Y * dx / (X + dx)                         # constant product
baseFee   = max(gross * 8 / 10000, 1) * L2 / pg       # ~16 bps of gross
clpFee    = max(dx² * Y / (X+dx)², 1) * F2 / pg       # impact
expected  = max(gross - baseFee - clpFee, 0)
minOut    = expected * (10000 - slippage_bps) / 10000
```

Abort when implied fee bps > 300 (3%), or when `dx * 2 > X` (more than half the input reserve).

Two-hop: run hop 1 with slippage 0, feed `expected` into hop 2 with the user slippage.

---

## Inventory model

Two BTC books stay separate:

| Book | Where | What it hedges |
|---|---|---|
| Binance / Lightning | Binance Convert + LND node | Customer Hive ↔ Keepsats / Lightning |
| MagiSats | Magi BTC on a Hive account | Customer MagiSats in/out, and Magi DEX fills |

Selling HIVE on Magi **does not** refill the Lightning node. It mints MagiSats. Use Magi as the hedge when the customer flow is MagiSats, or when MagiSats inventory is the intended BTC book. Use Binance when the node needs BTC.

MagiSats ↔ Lightning already exists (`#magisats` outbound, Magi inbound `#magioutbound`). That bridge is the only way to move value between the two BTC books. Price it into arb (v4vapp Magi fee + LN routing). Do not treat a Magi HIVE→BTC fill as a Binance BTC refill.

### Which Hive account signs

Altera has no deposit address. The signer **is** the wallet.

Do **not** trade from production `v4vapp` (server). That Magi address is the customer hot wallet (`#magisats` in and out). The ledger can split Exchange Holdings from VSC Liability; the chain cannot.

Use a **new** dedicated Hive account, same pattern as `bdhivesteem` / `devexc`. Do not reuse `v4vapp.vsc`.

| Env | Magi exchange account | Server (untouched) |
|---|---|---|
| production | `v4vapp.magi` | `v4vapp` |
| dev | `devmagi.v4vapp` | `devser.v4vapp` |

Give it `role: exchange`, list it first in `exchange_config.magi_vsc.hive_accounts`, and watch it.

Flow:

```
treasury / server  --Hive transfer HBD/HIVE-->  magi exchange account
magi exchange account  --vsc.call execute-->  MagiSats on hive:<exchange>
hive:<exchange>  --VSC transfer, when server Magi is low-->  hive:<server>
```

Hive legs already book `TREASURY_TO_EXCHANGE` / `SERVER_TO_EXCHANGE` / `EXCHANGE_TO_TREASURY` via `process_hive.py` once the account is in `magi_vsc.hive_accounts`. MagiSats refill of the server is a VSC `transfer` (existing Magi send path), not a DEX swap.

A tiny signed test from the server account is allowed in phase 1 only, with a hard size cap. Production path is the exchange account.

---

## What exists today

| Piece | State |
|---|---|
| `MagiAdapter` | Stub. BTC balance for `server_id` works. HIVE/HBD return 0. Price, minimums, `market_sell` / `market_buy` raise. |
| `get_exchange_adapter("magi_vsc")` | Wired. `exchange_name = "MagiSwap"`. |
| Config `exchange_config.magi_vsc` | Present. Devhive `mainnet.no_trade: true`. |
| `VSCSwapPayload` / `VSCCall` execute | Parse only. Nothing sends an execute call. |
| `send_magi_transfer_custom_json` | Transfer action only. Checks Magi BTC balance and `.sats` / `.parent_id`. Unusable as-is for swaps. |
| `process_magi_btc_transfer_event` | Handles `action == "transfer"` only. A pool→address BTC credit with no transfer payload must **not** be booked as `magisats_inbound`. |
| Rebalance | `rebalance_queue_task` → `add_pending_rebalance` → `execute_rebalance_trade` on **one** adapter from `default_exchange`. Always HIVE/BTC. HBD is converted to HIVE equivalent first. |
| Binance | Convert API on mainnet. Spot HIVEBTC book is empty. Min override 50 HIVE. |

---

## Design

### Quote object

Add a venue-neutral quote used by Magi, Binance, and the arb log:

```python
class VenueQuote(BaseModel):
    venue: str                    # "MagiSwap" | "binance_convert" | "fair_usdt"
    base_asset: str
    quote_asset: str
    side: str                     # BUY | SELL
    amount_in: Decimal            # human units (HIVE/HBD, or BTC)
    amount_out: Decimal           # human units
    sats_per_base: Decimal        # BTC-out in sats / 1 HIVE or 1 HBD
    fee_bps: int
    min_amount_out: Decimal | None
    pool_impact_pct: Decimal | None
    raw: dict
    fetched_at: datetime
```

Compare venues on `sats_per_base` for sells (higher wins) and on base-per-sats for buys (more HIVE/HBD per sat wins).

Fair-value reference (not executable):

- HIVE: `HIVEUSDT / BTCUSDT` from `AllQuotes`
- HBD: Hive internal market HBD/HIVE × that HIVE sats rate

Binance Convert has no HBD pair. For HBD customer flow, Binance comparison is HBD→HIVE (Hive DEX or the conversion already stored as HIVE equivalent) then HIVE→BTC Convert.

### MagiAdapter

Implement the full `BaseExchangeAdapter` surface. Keep methods sync to match the protocol; reuse the existing thread-executor pattern from `get_balance`.

| Method | Behaviour |
|---|---|
| `exchange_name` | `"MagiSwap"` |
| `get_balance("BTC"/"SATS"/"MSATS")` | Magi BTC of **exchange Hive account**, not `server_id` |
| `get_balance("HIVE"/"HBD")` | Native Hive `Account` balances of that same account |
| `get_current_price` | Spot from reserves (`Y/X` oriented), two-hop chained for HIVE/BTC |
| `get_min_order_requirements` | Soft floor (20 HIVE or 1 HBD) + notional from 3% fee abort; hard cap 10% of input reserve (config) |
| `market_sell` / `market_buy` | Quote `ly`, refuse on abort rules, broadcast execute, wait for `dex_pool_swap_events`, return `ExchangeOrderResult` |

Payload builder lives next to Magi send, not inside the adapter:

- `send_magi_swap_custom_json(vsc_call)` — no Magi BTC pre-check; `payload` serialised as a **JSON string**; `intents` required; `active=True`.
- Wait helper polls indexer `dex_pool_swap_events(where: {indexer_tx_hash: {$like: trx_id%}})` then, for BTC out, `find_magi_docs` for the credit to `hive:<exchange>`.

`VSCSwapPayload` already has `extra="allow"`. Add optional `beneficiary` / `ref_bps` if we take the 25 bps referral. Default: no beneficiary, or `hive:v4vapp`.

Recipient for MagiSats: `hive:<exchange_account>`. Never a `bc1…` address on this path.

### Router

Do not flip `default_exchange` globally. Customer queue still accumulates on one `PendingRebalance` row keyed by `(base, quote, direction, exchange)`. Options:

**A — Accumulate on Binance, choose venue at execute (recommended for v1)**

Keep pending rows as they are (`exchange=binance_convert`). When `can_execute`, `execute_rebalance_trade` asks a router:

```
binance_quote = BinanceSwapAdapter.send_quote_request(...)   # do not accept yet
magi_quote    = MagiAdapter.preview_swap(...)                # ly() only
fair          = AllQuotes triangle

log VenueQuote × 3

if magi enabled
   and magi_quote.sats_per_base >= binance_quote.sats_per_base * (1 + min_edge)
   and size <= magi_max_fraction * magi_input_reserve
   and magi_quote.fee_bps <= 300
   and magi exchange account has the input asset:
     fill on Magi
else:
     accept the Binance quote (or request a fresh one if the 10s quote expired)
```

Pending row still says `binance_convert`. Fill `ExchangeOrderResult.exchange` is the venue that actually traded (`MagiSwap` or `binance_convert`). Ledger `Exchange Holdings` sub follows that name.

**B — Separate pending rows per venue**

Cleaner inventory, more operational complexity. Defer until Magi volume is real.

v1 is A. Magi `no_trade: true` makes the router always take Binance but still log Magi quotes.

### HBD

Magi can trade HBD/BTC in one hop. Binance cannot.

v1 still queues HBD customer flow as HIVE equivalent (current `rebalance_queue_task(..., base_asset="HIVE")`) so Binance fallback works.

v2: for Magi-eligible sells, pass `base_asset="HBD"` when the customer paid HBD **and** Magi is enabled. Router then quotes Magi HBD→BTC against Binance (HBD→HIVE on Hive DEX + HIVE→BTC Convert). If Magi loses or is disabled, convert HBD→HIVE on Hive internal market (existing `internal_market_trade.py`) and sell HIVE on Binance.

### Accounting

Reuse `exchange_accounting()`. Magi fill:

- `EXCHANGE_CONVERSION` both legs on `Exchange Holdings / MagiSwap` (HIVE or HBD vs MSATS)
- `EXCHANGE_FEES` if `fee_msats > 0` (`gross − expected` from `ly`, or fill `amount_in/out` vs quote)

`order_id` = Hive `trx_id`. `client_order_id` = triggering `short_id`.

Do **not** let `process_magi_btc_transfer_event` book DEX BTC credits as `magisats_inbound`. Guard:

- skip when `from_addr` is `contract:vsc1Brvi…` or a `dex_pool_registry` pool
- skip when `to_addr` is the Magi exchange account (not `server_id`)
- skip when the matching custom_json `action == "execute"`

Server Magi inbound/outbound for customers stays as today.

### Arb watch

Every router decision writes one document (Mongo `exchange_quotes` or Redis list + periodic Mongo):

| Field | Meaning |
|---|---|
| `short_id` / `group_id` | triggering customer op, or `arb_poll` |
| `side` | sell / buy |
| `base_asset` | HIVE or HBD |
| `qty` | size quoted |
| `magi_sats_per_base` | Magi `ly` |
| `binance_sats_per_base` | Convert `toAmount` |
| `fair_sats_per_base` | USDT triangle |
| `edge_bps_vs_binance` | Magi − Binance |
| `edge_bps_vs_fair` | Magi − fair |
| `chosen` | MagiSwap / binance_convert / none |
| `reason` | edge, size, no_trade, pool, balance |
| `magi_reserves` | BTC pool r0/r1, HIVE/HBD r0/r1 |
| `fetched_at` | |

A background task (hive_monitor or a small loop in `binance_monitor` / new `exchange_monitor`) polls the same quotes every N seconds (default 60) **without** a customer fill, so the spread is visible when flow is quiet. Notify Telegram when `|edge_bps_vs_binance|` ≥ `arb_notify_bps` (default 50) and size 20 HIVE would still pass Magi abort rules.

v1 is observe-only. Auto-arb (trade Magi and the opposite leg on Binance) is v3, and only after bridge costs and inventory are in the edge calc.

Snapshot at plan time (2026-11-23), 50 HIVE sell:

| Venue | sats / HIVE |
|---|---|
| Fair (HIVEUSDT/BTCUSDT) | 54.75 |
| Magi `ly` | 56.52 (~+3.2%) |
| Binance HIVEBTC last | 111, **empty book** — ignore |
| Magi BTC pool | 0.0235 BTC / 1,526 HBD |
| Magi BTC/HBD 24h | 4 swaps |

`hive:sbi-tokens` already fills this pool both ways. Assume the edge is competed and size-capped.

---

## Config

Extend `ExchangeProviderConfig` / `magi_vsc` (names indicative):

```yaml
exchange_config:
  default_exchange: binance          # customer queue still Binance-keyed
  magi_vsc:
    exchange_mode: mainnet
    hive_accounts:
      - v4vapp.magi                  # signer / MagiSats inventory
    mainnet:
      no_trade: true                 # quotes only until go-live
    router:
      enabled: true
      min_edge_bps: 50               # Magi must beat Binance by this
      max_pool_fraction: 0.10        # of Magi input reserve
      slippage_bps: 50
      min_hive: 20
      min_hbd: 1
      fee_abort_bps: 300
      arb_poll_seconds: 60
      arb_notify_bps: 50
      auto_arb: false
```

`no_trade: true` → quote and log, never broadcast execute.

---

## Phases

### Phase 0 — Accounts and config

- Create Hive account `v4vapp.magi` (prod) and `devmagi.v4vapp` (dev). Store active + memo keys in config like other exchange accounts.
- Fund a small HBD/HIVE float from treasury.
- Add `role: exchange`, `watch_users`, `magi_vsc.hive_accounts: [v4vapp.magi]`.
- Keep `no_trade: true`.
- MagiAdapter `get_balance` reads `v4vapp.magi` / `devmagi.v4vapp` for BTC and Hive.

**Done when:** the account exists on Hive, keys are in config, admin balance sheet can show MagiSats and HIVE/HBD on it; no swaps sent.

### Phase 1 — Quote engine (no broadcast)

New module `src/v4vapp_backend_v2/conversion/magi_quote.py`:

- Fetch `dex_pool_registry` + `dex_pool_liquidity` (indexer), cache ~5s.
- `Ph` / `oV` / `ly` / two-hop `iV` ported with Decimal/int, covered by tests against Altera constants.
- `preview_swap(asset_in, asset_out, amount_in, slippage_bps) -> VenueQuote`.
- Binance Convert `send_quote_request` already exists; wrap it as `VenueQuote`.
- Fair USDT triangle from `AllQuotes`.

Tests: golden vectors for 1/10/50/100 HIVE and 1/10/50 HBD; abort on 50% reserve and fee > 3%; HIVE routes via HBD; HBD is one hop.

**Done when:** a script prints Magi vs Binance vs fair for a size, no chain writes. Put it next to `scripts/test_binance_swap.py` as `scripts/test_magi_swap_quote.py`.

### Phase 2 — MagiAdapter protocol

- Implement remaining adapter methods on top of `magi_quote`.
- `market_sell` / `market_buy` still raise while `no_trade`, **or** they run quote + abort checks and return without sending (for unit tests).
- Tests extend `tests/conversions/test_magi_adapter.py`.

**Done when:** `get_exchange_adapter("magi_vsc")` can price and size-check HIVE/BTC and HBD/BTC.

### Phase 3 — Execute and confirm

- `send_magi_swap_custom_json` + wait on `dex_pool_swap_events`.
- Payload `action=execute`, JSON-string payload, `transfer.allow` intent in Hive amount units (`15.000` not `15000`).
- `ExchangeOrderResult` from fill `amount_in` / `amount_out`.
- Guard `process_magi_btc_transfer_event` so DEX credits to the exchange account are ignored.
- Manual live test: 1–5 HBD sell on **dev** exchange account, `nobroadcast` then tiny mainnet with `no_trade: false` on that env only.

**Done when:** one live fill is in `rebalance_results`, `EXCHANGE_CONVERSION` on `MagiSwap`, MagiSats on `hive:<exchange>`, server Magi balance unchanged.

### Phase 4 — Router in `execute_rebalance_trade`

- Quote both venues at execute time; log `exchange_quotes`.
- Choose Magi only if `no_trade` is false and edge/size/balance/fee gates pass.
- Binance path unchanged when Magi loses or is disabled.
- If Magi send fails after broadcast, do **not** automatically fire Binance for the same qty (double hedge). Leave pending row, alert, operator retry.

**Done when:** a 50 HIVE customer sell that Magi would have won is either filled on Magi (go-live) or logged as `chosen=binance_convert reason=no_trade` (shadow).

### Phase 5 — Arb watch poll

- 60s poll of 20 HIVE and 10 HBD sell+buy quotes.
- Telegram when `|edge| ≥ arb_notify_bps` and Magi abort rules pass.
- Admin: simple table on quote sources or a new Magi DEX page (reserves, last quotes, last fills). Minimum for v1 is Graylog/Mongo; UI is optional.

**Done when:** a day of quiet operation has a time series of Magi−Binance bps without any Magi trade.

### Phase 6 — HBD native + inventory refill

- Customer HBD sells: quote Magi HBD/BTC vs Binance HIVE path.
- MagiSats server refill: VSC transfer exchange → server when server Magi BTC < configured floor (needed for `#magisats` outbound).
- Hive surplus: existing auto_rebalance / treasury transfer onto the Magi exchange account.

**Done when:** an HBD customer deposit can hedge on Magi HBD/BTC without a Hive internal-market hop.

### Phase 7 — Auto-arb (optional, later)

Only with `auto_arb: true`, a documented inventory budget, and edge **after** Magi CLP + Binance spread + MagiSats↔LN bridge fee.

Pattern: Magi sell HIVE/HBD for MagiSats, buy HIVE on Binance (or the reverse). Size limited to `max_pool_fraction`. One in-flight arb at a time (Mongo claim, same concern as [net_rebalance_plan.md](net_rebalance_plan.md)).

Out of v1.

---

## File map

| File | Change |
|---|---|
| `conversion/magi_quote.py` | **New.** Indexer fetch, `ly`, routing, `VenueQuote` |
| `conversion/magi_adapter.py` | Full protocol; balances from exchange Hive account |
| `conversion/exchange_protocol.py` | Factory already has `magi_vsc`; maybe `preview_swap` on protocol if Binance gets one too |
| `conversion/exchange_process.py` | Router inside execute path; quote log |
| `conversion/exchange_rebalance.py` | Pass size into router; do not change pending key in v1 |
| `hive_models/magi_json_data.py` | Optional beneficiary/ref_bps; helper to dump execute payload as JSON string |
| `process/hive_notification.py` | `send_magi_swap_custom_json` |
| `process/process_magi.py` | Ignore DEX fills / execute calls for inbound |
| `magi/magi_balances.py` | GraphQL for `dex_pool_*` (or a sibling `magi_dex.py`) |
| `config/setup.py` | `magi_vsc.router` fields |
| `config/*.yaml` | `hive_accounts`, `no_trade`, router knobs |
| `scripts/test_magi_swap_quote.py` | **New.** Live quote compare |
| `tests/conversions/test_magi_quote.py` | **New.** AMM vectors |
| `tests/conversions/test_magi_adapter.py` | Price, mins, balances, no-trade |
| `tests/conversions/test_exchange_router.py` | **New.** Edge / size / no_trade choices |

---

## Safety

- Magi `no_trade` defaults true in every env until phase 3 is signed off.
- Hard cap `max_pool_fraction` (10%) and `fee_abort_bps` (300). CLP already punishes 1 HIVE; floor 20 HIVE / 1 HBD.
- Never land DEX BTC on `hive:<server_id>`.
- Never set `destination_chain: BTC` on the rebalance path.
- Magi send failure does not auto-fire Binance.
- Hive `active` key for the exchange account only; server keys stay on customer Magi transfers.
- BTC pool at plan time is ~0.023 BTC. A 100 HBD sell already moves the price ~12%. Treat Magi as a small-ticket venue.
- `sbi-tokens` and others arb this pool. Our quote can go stale between `ly` and Hive inclusion (~3s+ Magi consensus). `min_amount_out` is the protection; a fail is logged, pending stays.

---

## Tests

- `ly` integer vectors vs the formula above (gross, baseFee, clpFee, expected, minOut).
- Routing: HIVE-BTC uses two pools; HBD-BTC one; unknown pair returns empty.
- Abort: `dx*2 > X`; fee bps > 300.
- Adapter: HIVE/HBD from Hive account mock; BTC from Magi balance mock; `server_id` is not used.
- Router: Magi wins when edge ≥ `min_edge_bps`; Binance when `no_trade`, insufficient Magi HBD, or size > fraction.
- `process_magi`: execute custom_json + BTC credit to exchange account creates **zero** MAGI_INBOUND entries.

Live: quote script on mainnet indexer (read-only). One tiny HBD sell on the dedicated account after phase 3.

---

## Rollout

1. Shadow: `no_trade: true`, arb poll on, customer flow 100% Binance. Watch `exchange_quotes` for a week.
2. Dev: `no_trade: false`, cap 5 HBD / 20 HIVE, dedicated account.
3. Prod: same caps, Magi only when edge ≥ 50 bps. Raise caps only if the BTC pool grows.
4. HBD native (phase 6) after a few successful HIVE Magi fills.
5. Auto-arb stays off until inventory and bridge costs are in the log.

---

## Out of scope

- Replacing Binance as the Lightning BTC venue.
- L1 Bitcoin unwrap (`destination_chain: BTC`).
- ETH/USDC/lassecash pools.
- Changing customer `#magisats` / Keepsats flows.
- Switching `default_exchange` to `magi_vsc`.
- Netting buy vs sell ([net_rebalance_plan.md](net_rebalance_plan.md)) — independent; do not block on it.

---

## Decisions

1. **Magi exchange account:** new `v4vapp.exc` (prod) and but probably no `devexc.v4vapp` (dev). Not `v4vapp.vsc`.

## Open choices

1. Referral: omit, or `beneficiary: hive:v4vapp` at 25 bps.
2. Shadow week length before first prod fill.
3. Whether MagiSats from Magi sells are allowed to sit as Magi inventory, or must be bridged to Lightning before counting as a Hive→Keepsats hedge.
