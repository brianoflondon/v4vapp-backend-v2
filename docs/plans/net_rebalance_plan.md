# Net buy and sell rebalances before trading

| Field | Value |
|---|---|
| **Date** | 2026-11-19 |
| **Status** | Advice |
| **Primary code** | `src/v4vapp_backend_v2/conversion/exchange_rebalance.py`, `src/v4vapp_backend_v2/conversion/exchange_process.py` |

Netting is already written and tested. The live path never calls it. Each new customer order is added to one side, and that side trades alone once it clears 50 HIVE (or 0.5 DASH) and the sats minimum.

## The rule

Keep the two `pending_rebalances` rows, one buy and one sell, per pair. After each new order is added:

`net = sell quantity − buy quantity`

Trade only when `abs(net)` is at least the quantity minimum and the sats value of that net is at least the notional minimum. Sell the net when sells are larger. Buy the net when buys are larger. The smaller side is an internal offset and never goes to Binance.

| Sell | Buy | Net | Trade |
| --- | --- | --- | --- |
| 60 HIVE | 40 HIVE | 20 sell | No. 20 is under 50. |
| 90 HIVE | 30 HIVE | 60 sell | Sell 60. Buy row goes to zero. |
| 20 HIVE | 80 HIVE | 60 buy | Buy 60. Sell row goes to zero. |

HIVE/BTC and DASH/BTC stay separate. An HBD customer order is already stored as its HIVE equivalent, so it nets in HIVE.

## Where to switch it

`get_net_position` and `execute_net_rebalance` in `exchange_rebalance.py` already do this, including clearing the offset side and leaving a remainder on the side that was traded. `tests/conversions/test_exchange_rebalance.py` covers balanced, below-minimum, buy, and sell.

The live call is `rebalance_queue_task` in `exchange_process.py`. It waits 10 seconds, then calls `add_pending_rebalance`, which checks only the side it just increased. Hive-to-Keepsats, Keepsats-to-Hive, and Dash inbound all go through that task.

The change is: add the new quantity to its own row and save it, then run the net check instead of that side’s own `can_execute`. `execute_net_rebalance` does not add the new quantity, so it cannot replace `add_pending_rebalance` by itself.

## What has to be closed first

**One trade at a time, across processes.** Hive and Dash monitors are separate processes and share the same two rows. Two orders can both read “net 60” and both send a trade. An in-process lock is not enough. The pair needs a Mongo claim so only one execution proceeds, and the loser re-reads the rows.

**Save the new order before the trade.** If Binance rejects the order, the new quantity must stay on its row. Today a below-minimum error returns without saving that add.

**Pass the triggering short id into the net order.** `execute_net_rebalance` does not set `client_order_id`. The exchange ledger uses that id. The fill should still be booked as one `EXCHANGE_CONVERSION` for the quantity Binance actually filled. The offset is not a trade, and customer ledgers are already written when the customer order is processed.

**The testnet target buy shares the HIVE/BTC buy row.** `binance_monitor.py` calls `add_pending_rebalance` directly to top the treasury up to a target. If only the customer queue nets, that monitor can still buy the whole buy row and ignore the sells. If `add_pending_rebalance` itself nets, a target top-up is reduced by customer sells. Those should stay separate, or the monitor should stop writing into the customer buy row.

**Remainder rows lose their order list.** After a fill, `reset_after_execution` clears `transaction_ids` even when some quantity is left. The quantity math for a partial fill is already right (offset cleared, unfilled net left behind). The admin list for that leftover would be empty unless the ids are kept until the quantity hits zero.

## Admin widget

The buy and sell lists can stay. The progress that decides a trade should be `|sell − buy|` against the 50 HIVE / 0.5 DASH floor, with the direction of that net. Per-side percentages would no longer mean “this side is about to trade.”
