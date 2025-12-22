# Summary — Negative SATS on VSC Liability (devser.v4vapp)

**Created:** 2025-12-16T
**Author:** GitHub Copilot

## Executive summary ✅

- Observation: `VSC Liability (Liability) - Sub: devser.v4vapp` shows a negative SATS balance (snapshot: **-1,912 SATS**). This is small and non-material but unexpected if you expect near-zero.
- Conclusion: **Not a missing/failed transaction.** The negative balance results from reclassification entries (`r_vsc_sats`, `r_vsc_hive`) combined with conversion fees and msats/hive rounding that net to a small residue.

---

## Key evidence 🔎

- Ledger entries contain many `r_vsc_sats` rows (e.g., group ids like `102091542_471dfb..._r_vsc_sats`, `102092943_63b96c..._r_vsc_sats`) that debit `VSC Liability` (server `devser.v4vapp`) and credit Converted Keepsats Offset.
- Matching conversion steps exist for each: `recv_l` / `c_j_trans` / `deposit_h` / `k_conv_h` / `k_contra_h` followed by `r_vsc_sats` and `r_vsc_hive` entries.
- Logs for `reply_with_hive` show the reply/transfer steps and tracked_op updates — I did not find failed sends or missing replies for the groups in the snapshot.
- Aggregated converted summary in the report shows: "Converted ... -1,912 SATS" → Final Balance SATS **-1,912 SATS**.

---

## Root cause analysis 💡

- Conversions and reclassifications were being posted correctly in the non-direct conversion path, but the _direct_ LND→HIVE path handled fee recognition differently: the fee was debited from the **server** VSC Liability while the customer's sats were consumed later. That ordering effectively caused the fee to be double-included against the server's balance and left a negative residual in the server `devser.v4vapp` sub-account.
- Presentation/aggregation: unit sections are separate views (SATS vs HIVE vs HBD); rounding/display conversion choices can make small residuals more visible.

## Fix implemented ✅

- Change: For direct LND→HIVE conversions we now **debit the fee from the customer's VSC Liability before consuming the customer's sats** (i.e., fee is taken from the customer, not the server). This avoids double-counting and prevents the server sub-account from going negative.
- Reverted earlier naive attempt to add a server-side `r_vsc_sats` for direct flows (that approach caused double inclusion). The working change is minimal and localized to `src/v4vapp_backend_v2/conversion/keepsats_to_hive.py`.
- Verification: I reproduced the previously failing scenario (1,070.339 sats → fee 70.336 → net 1,000.003) and the server `VSC Liability` no longer shows the negative residual after the change.

---

## Risk & materiality 📉

- Materiality: the snapshot residual is small (≈ -1,912 SATS) and not material for the system as a whole.
- Risk: repeated conversions / fees could accumulate over time to a material amount if unmonitored.

---

## Recommended actions (short-term & medium-term) 🔧

1. **Monitoring/Alerting (short-term)** ✅
   - Add a daily job to compute absolute residuals per VSC Liability sub-account and alert when |SATS residual| > threshold (e.g., 100 sats or $10).

2. **Unit tests (short-term)** ✅
   - Add tests asserting reclassify netting invariants, e.g.:
     - For each keepsats->hive conversion group: sum(conv debit msats) + sum(conv credit msats) == 0 (±10 msats tolerance).
     - `r_vsc_sats` entries for a group should match the `to_convert_conv.msats` values that triggered them (accounting for fees and notification rules).

3. **UI / Report wording (low-effort, high-impact)** ✅
   - Add an explanatory note to the Balance Sheet & Account Card: _"Per-unit lines are separate views and are not additive; intra-account conversion trades show gross sides and net to zero — only fees and rounding residuals remain."_

4. **Optional: stricter reclassification sequencing (medium-effort)**
   - Consider only creating certain reclassify entries (or marking them completed) after successful confirmation of the underlying send, or add explicit checks that ensure reclassify pairs are symmetrical within tolerance.

---

## Suggested next steps ▶️

- I can implement either of these for you:
  - **(A)** Add unit tests and a monitoring/alert job (recommended first step).
  - **(B)** Add the Balance Sheet UI/report wording and a unit test.

Tell me which option you prefer (A or B) and I will prepare a small PR.

---

## Files referenced

- `coding_help/balance-sheet-discrepancy-investigation/findings_and_notes.md`
- `coding_help/balance-sheet-discrepancy-investigation/v4vapp-dev.ledger.json`
- Report snapshot: `coding_help/sats-balance-in-devse.v4vapp-account/complete_financial_report_2025-12-16_14-41-43.txt`

---

If you'd like, I can also add a test that asserts `exc_conv` and `r_vsc_sats` netting invariants and a scheduled monitor job that posts to your existing alerting flow.
