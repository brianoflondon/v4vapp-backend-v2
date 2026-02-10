# Expense accounts (Hive/HBD payments) 🔧

This page documents how "expense accounts" are configured and how Hive/HBD transfers to those accounts are handled by the application.

---

## Overview 💡

- Expense accounts are Hive accounts that are used to record expenses (HIVE or HBD) in the ledger.
- When a transfer is made *to* an expense account, the transfer is recorded as an expense using the rules defined on the expense account.
- Lightning-based expense recording (Lightning payments recorded as expenses) is planned separately (TBD).

---

## Configuration (YAML) 🔧

Define an expense account under `hive.hive_accs` with `role: expense` and an `expense_rule` block. The config must use the field name `expense_account_name` (note: spelling matters — typos cause validation errors).

Example (devhive.config.yaml):

```yaml
hive:
  hive_accs:
    v4vapp.bol:
      role: expense
      expense_rule:
        from_type: treasury
        expense_account_name: "Testing Expenses"   # <- required
        description: "Sample expense rule"
        ledger_type: expense
```

Notes:
- `expense_account_name` is required by the Pydantic model (`HiveAccountRulesConfig`).
- `from_type` should be a member of `HiveRoles` (e.g. `treasury`).
- `ledger_type` is used to map to the ledger entry type (default: `expense`).

If the YAML contains a typo (for example `excpense_account_name`) the config validator will raise an error at startup.

---

## Behavior in code (how transfers are turned into expense ledger entries) 🧾

Reference: `src/v4vapp_backend_v2/process/process_hive.py`

When a transfer is processed the code checks whether the transfer destination is an expense account:

- It consults `InternalConfig().config.hive.expense_account_names` to see if the `to_account` is registered as an expense account.
- If it is, the code looks up the account object (`hive_config.hive_accs[to_account]`) and checks that `expense_rule` is defined. If missing, a `LedgerEntryCreationException` is raised.
- Using the rule, the code builds the ledger entry as follows:
  - Debit: ExpenseAccount(name=<rule.expense_account_name>, sub=<expense_account.name>)
  - Credit: AssetAccount(name="Treasury Hive", sub=<hive_transfer.from_account>)
  - Description: "<rule.description> - <base_description>"
  - user_memo: processed with `lightning_memo`
  - ledger_type: `LedgerType(rule.ledger_type)`

Errors raised at this stage prevent the ledger entry from being saved.

---

## Current limitations & notes ⚠️

- The current implementation expects a single `expense_rule` per expense account (`HiveAccountConfig.expense_rule` is singular). If you need multiple rules per account we should change the model to a list and adapt the processing logic.
- The code currently credits the Treasury (asset) account using the `from_account` as the sub-account; it does not strictly enforce `from_type` yet — make sure the rule's `from_type` aligns with the actual source account in your config or update the code to validate `from_type` before recording.
- Lightning-based expense recording is still to be implemented (TODO #249).

---

---
