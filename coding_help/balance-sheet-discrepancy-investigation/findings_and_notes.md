# Balance Sheet Discrepancy Investigation — Findings & Notes

**Created:** 2025-12-16T08:20:00Z
**Author:** GitHub Copilot

## Executive Summary ✅

- Issue: the **Balance Sheet** totals for **Exchange Holdings (sub: binance_testnet)** were small negative amounts (SATS = -2, USD ≈ -0.002) while the **account card** shows very large HIVE/SATS movement lines (e.g., 305 HIVE / 33,247 SATS). This looked like a discrepancy.
- Conclusion: **not a material accounting error** — this is a presentation/aggregation issue. Underlying ledger entries are consistent: exchange trades (`exc_conv`) post gross (both sides) in different units and net to zero; exchange fees (`exc_fee`) produce the small residual.

---

## Key Evidence (from `v4vapp-dev.ledger.json`) 🔎

- Trades (ledger_type: `exc_conv`) for `binance_testnet`:
  - Sum of HIVE movements (debit/credit): **305.022875 HIVE** / **-305.022875 HIVE** → **net 0 HIVE**
  - Sum of SATS movements (debit/credit): **33,247.493375 SATS** / **-33,247.493375 SATS** → **net 0 SATS**
- Fees (ledger_type: `exc_fee`) credited to Exchange Holdings:
  - **-2,493.375 msats** = **-2.493375 sats** (displayed as **-2 SATS**)
  - USD equivalent (sum of `conv usd`): **-0.00215 USD** (displayed as **-0.002 USD**)
- Balance Sheet shows the net amounts (fees) after netting intra-account trades → matches ledger when netted and rounded.

---

## Accounting Assessment (materiality & risk) 🔍

- **Materiality:** Not material at this snapshot — residuals are tiny and arise from rounding and fee postings.
- **Root cause category:** Presentation/aggregation and rounding, not ledger posting error.
- **Risks:** accumulated small rounding/rounding-of-conversions over long periods could become material if frequent — implement monitoring.

---

## Notes for the Balance Sheet (how it should behave) 🧾

- **Netting rule:** The balance sheet should present **net balances per account** in the chosen reporting units. Internal conversion entries (exc_conv) that post both sides (HIVE and SATS) should net to zero and not double-count.
- **Primary unit & conversion:** Use msats (smallest integer unit) internally for lightning/sats arithmetic, and convert to display units at the end (SATS/HIVE/USD) with explicit rounding rules.
- **Rounding policy:** Convert and sum in smallest units; only round at presentation. Document rounding scale (e.g., display SATS to integer, HIVE/HBD to 3 decimal places, USD to 3 decimal places for micro-amounts).
- **Reporting note to add:** Add a line or tooltip in the Balance Sheet: _"Unit lines represent values converted into each unit; they are not additive across columns. Intra-account exchange trades show gross sides and net to zero in totals — only fees and real net flows remain."_

---

## Notes for Account Cards (detail view) 📇

- Account cards should show transaction-level detail (gross flows) and allow both:
  - a **Unit view** (e.g., HIVE or SATS) showing per-transaction values in that unit, and
  - a **Net view** showing the net position of the account (single-line totals) in a selected currency or canonical unit.
- **Important UI clarity:** Add a short line on the account card: _"HIVE and SATS sections are separate views of the same transactions and are not additive; check the net summary for account total."_
- Consider adding a toggle to show `Net (canonical)` vs `Gross (per-unit)`.

---

## Tests & Monitoring suggestions ✅

- Add unit tests asserting:
  - `sum(conv debit hives) + sum(conv credit hives) == 0` for `exc_conv` grouped by exchange/account/date-range.
  - `exc_fee` results in **Expense (debit)** and **Asset (credit)** with matching msats/hive/USD conversions.
- Add a daily job to compute and alert if residual absolute > configured thresholds (e.g., > 100 sats or > $10).

---

## Proposed short-term fixes (low effort / high impact) 🔧

1. **Add wording** to the Balance Sheet and Account Cards explaining that per-unit lines are not additive and trades appear on both sides. (UI / report text change)
2. **Enforce internal aggregation in smallest units** (msats/hundred-millionths for HIVE if needed) and round only for display.
3. **Add tests** verifying exc_conv netting and exc_fee treatment.
4. **Add monitoring/alert** for unexpectedly large residuals.

---

## Suggested next steps (for code review) ▶️

- Inspect the report-generation code that sums account balances for the Balance Sheet and the code that formats account cards. Look for:
  - where conv fields (`conv_signed`) are used vs transaction units (`debit_amount_signed`/`credit_amount_signed`)
  - rounding and unit selection logic
  - any places where per-unit values are summed across columns (incorrectly) instead of being netted by account
- Implement a small display note and add tests as described above.

---

## Files / Paths

- This note: `coding_help/balance-sheet-discrepancy-investigation/findings_and_notes.md`
- Example ledger used: `coding_help/balance-sheet-discrepancy-investigation/v4vapp-dev.ledger.json`
- Suggested code candidates to inspect (search for these symbols): `exc_conv`, `exc_fee`, `conv_signed`, functions that create the Balance Sheet output (likely under `src/report` or `src/accounting`).

---

If you want I can:

- (A) open and annotate the exact functions that perform the Balance Sheet aggregation and the account-card rendering, or
- (B) prepare a small PR that adds the display note and a unit test asserting `exc_conv` netting.

What's your preference?
