# Trading Profit & Loss Report

This page describes the new **Trading P&L** analysis that is produced from the
ledger by examining activity in the *Exchange Holdings* account.  The feature
was added to provide a quick evaluation of how much profit (or loss) the system
has realised from spot trades executed on external exchanges (currently Binance
convert/trade).  It can be accessed via the admin interface:

`Admin → Financial Reports → Trading P&L (Exchange)`

There is a separate section for each `sub` field of the *Exchange Holdings*
account.  In a normal deployment the `sub` value corresponds to the external
exchange adapter that generated the ledger entry (e.g. `binance_convert`).

The output is a simple text table, with the following parts:

1. **Counts and totals table** (top).  Shows number of sell/buy trades along
   with gross HIVE quantities and cashflow in satoshis.  HIVE values are printed
   to three decimal places; SATS values are rounded to integers.  Data is
   aligned in columns for easier scanning.
2. **Performance summary** (below the line).  Contains net inventory change,
   net cash generation, last-known price used for marking the remaining
   inventory, inventory valuation and the total trading P&L expressed in
   satoshis.  All numeric values are right‑aligned.
3. **Per-sub breakdown** (see full report output for details).

A full JSON version of the report is also available (use the **Toggle Raw Data**
button then **Download JSON**).  The JSON schema is minimal:

```json
{
  "as_of_date":"2026-02-19T...",
  "by_sub":{
    "binance_convert":{
      "summary":{...},
      "performance":{...},
      "sub":"binance_convert"
    },
    "another_sub":{...}
  },
  "totals":{...}
}
```

## How the calculation works

The P&L logic is encapsulated in
`src/v4vapp_backend_v2/accounting/trading_pnl.py` (see the code for full
comments).  At a high level:

1. Determine the list of subs to analyse.  If none is provided the code will
   query `list_all_accounts()` and collect all unique sub‑values belonging to
   accounts named **Exchange Holdings**.
2. For each sub, call `one_account_balance()` with an `AssetAccount(name="Exchange Holdings", sub=sub)`
   to obtain a `LedgerAccountDetails` object containing the ledger rows for
   that account/sub combination.
3. Extract the `hive` unit balance lines and filter to entries whose
   `ledger_type == LedgerType.EXCHANGE_CONVERSION.value` (`"exc_conv"`).
   These correspond to individual trades performed by the exchange adapter.
4. Iterate over the filtered entries and for each line:
   * read the `description` to detect *SELL* vs *BUY*.
   * parse `conv_signed` to obtain `hive` and `sats` amounts.  Absolute values
     are used because the same trade appears twice (debit/credit) in the
     combined balance view.
   * track running totals of HIVE sold/bought and SATS received/spent.
   * remember the last non-zero price value (`sats_hive`).
5. After processing all trades, the per-sub performance metrics are computed:
   * `net_hive_inventory_change = total_hive_bought - total_hive_sold`
   * `net_sats_cashflow = total_sats_received - total_sats_spent`
   * `inventory_valuation_sats = net_hive_inventory_change * last_price`
   * `total_trading_pnl_sats = net_sats_cashflow + inventory_valuation_sats`
6. Grand totals are derived by summing the sub-reports, using the first
   non-zero `last_price` found.

The code handles missing data gracefully (empty trade list → zero totals).  It
was designed to be fast even on large ledgers because the heavy lifting is done
within the MongoDB aggregation pipeline used by `one_account_balance`.

## Admin UI integration

The admin router (`financial_reports.py`) provides a GET handler at
`/admin/financial-reports/trading-pnl` that calls
`generate_trading_pnl_report()` and renders the results with
`trading_pnl.html`.  The page includes controls to download or print the
formatted text, and to fetch the JSON blob.

CSV export or charts are not currently implemented but could be added by
extending the template and JavaScript.

## Testing

Two new tests were introduced:

* `test_trading_pnl.py` – verifies the computation against the sample JSON
  dataset included in `tests/data/trading_peformance/exchange_holdings_sample.json`.
  It also checks that the printout formats numbers as expected.
* `test_trading_pnl_async.py` – ensures the report generator accepts a `sub`
  argument by monkeypatching `one_account_balance`.

To run the tests:

```bash
uv run pytest tests/accounting/test_trading_pnl*
```

## Notes & future work

* The report uses the last-known trade price to mark remaining inventory.  If
  more accurate market prices are desired (e.g. external feed), the code can be
  extended to accept a price argument or fetch from `crypto_prices.py`.
* The report currently only covers HIVE↔SAT trades.  Support for other pairs
  could be added by examining different ledger units or introducing a
  secondary report.
* The new download button mechanism is generic and may be reused elsewhere in
  the admin UI.

---
Document created Feb‑2026 by GitHub Copilot (via Raptor mini) for the
v4vapp-backend-v2 project.
