# Balance adjustments using the `v4v-app` Hive account ✅

Short version
- Send a Hive/HBD transfer *from* or *to* the `v4v-app` account with the memo containing **Balance adjustment**.
- The backend will treat that transfer as an on‑chain-only balance tweak — **no normal ledger entry (P&L) will be created**.

Why / when to use
- Use when you need to correct the *on‑chain* balance of the main server account quickly (top‑up or pull funds) without creating P&L accounting entries.
- Typical scenarios: operational top‑ups, tiny manual fixes, or moving funds between operator wallets for runtime fixes.

How it works (implementation notes)
- Detection is implemented in `src/v4vapp_backend_v2/process/process_hive.py`.
  - Trigger conditions:
    - Either `from_account` **or** `to_account` equals `v4v-app` (exact match), AND
    - the transfer memo contains the substring `Balance adjustment` (case‑sensitive).
  - When matched the code logs the event and intentionally does NOT create a normal ledger entry.
    - Constants: `BALANCE_ADJUSTMENT_ACCOUNT = "v4v-app"`, `BALANCE_ADJUSTMENT_MEMO = "Balance adjustment"`.

Step‑by‑step (recommended safe workflow) 🔧
1. Inspect and confirm the mismatch
   - Verify on‑chain balance for your server account (e.g. `devser.v4vapp`) and the ledger.
2. Make the on‑chain transfer
   - Example (CLI or wallet):
     - Top‑up server from operator wallet:
       `hive transfer v4v-app devser.v4vapp "10.000 HIVE" "Balance adjustment — top‑up for reconciling X"`
     - Pull from server to operator wallet:
       `hive transfer devser.v4vapp v4v-app "5.000 HIVE" "Balance adjustment — move to ops wallet"`
   - Notes:
     - Memo must include `Balance adjustment` (substring match; case‑sensitive).
     - Any extra text after/before the phrase is allowed.
3. Verify the change
   - On‑chain: confirm the Hive transaction ID on a block explorer.
   - Backend logs: look for a log like **"Balance adjustment transfer detected: ..."** from `process_hive.py`.
   - Ledger: no automatic ledger entry will be created (this is expected).
4. If the ledger also needs updating
   - Either create the appropriate accounting entry manually, or run the opening/adjustment tooling (examples below).

When NOT to use
- Don’t use this if you expect the transfer to be recorded in the ledger automatically.
- Don’t use for customer‑facing transactions or routine income/expense — this bypasses normal accounting.

Tools to reconcile ledger after on‑chain change
- Lightning/exchange opening‑balance helpers create proper `funding`/`adjustment` entries:
  - `reset_lightning_opening_balance()` — adjusts Lightning node opening balance ledger entries
  - `reset_exchange_opening_balance()` — adjusts Exchange Holdings opening/adjustment entries
  - See `src/v4vapp_backend_v2/helpers/opening_balances.py` for details.

Safety & best practices ⚠️
- Only authorized operators should use `v4v-app` + `Balance adjustment`.
- Test with small amounts first.
- Always include an audit reason in the memo (e.g. ticket/PR id, operator initials).
- Verify both the on‑chain transaction and your bookkeeping after performing the change.
- The memo check is case‑sensitive; use exactly `Balance adjustment` (can be part of a longer memo).

Troubleshooting
- Transfer fails with “Not enough to pay …”: ensure the sending account has sufficient HIVE/HBD.
- No ledger change after transfer: expected. Use opening‑balance helpers or create a manual ledger entry if you need the ledger updated.
- If nothing is detected by the backend: confirm account name is `v4v-app` (exact) and memo contains `Balance adjustment`.

References
- Detection & behaviour: `src/v4vapp_backend_v2/process/process_hive.py`
- Opening/adjustment helpers: `src/v4vapp_backend_v2/helpers/opening_balances.py`

Example audit memo (recommended):
- `Balance adjustment — top‑up server for reconciliation #INC-1234 (ops@org)`

If you want, I can add a short admin script or a unit test to validate the `Balance adjustment` flow. ⏭️