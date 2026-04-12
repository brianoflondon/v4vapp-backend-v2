# Using Hive Transfers to Interact with *Keepsats*

## Background

The v4v.app backend monitors the Hive blockchain for transfers directed at the **server account** (e.g. `v4vapp` on production, `devser.v4vapp` on dev). When such a transfer is detected, the system inspects the **memo** field to decide what to do. This document describes every memo pattern the system recognises and the behaviour each one triggers.

For interacting with *Keepsats* via `custom_json` operations instead of transfers, see [custom_json.md](custom_json.md).

> **Important:** Only transfers **to the server account** trigger special processing. Transfers between other accounts, or transfers *from* the server account, are recorded but do not invoke any conversion or payment logic.

---

## Memo Flags Reference

| Flag / Pattern | Example Memo | Effect |
|---|---|---|
| `#sats` or `#keepsats` | `"deposit check \| #sats"` | Convert the full Hive/HBD amount into *Keepsats* (sats) |
| Lightning invoice | `"lnbc5u1p...abc memo text"` | Pay the Lightning invoice using the deposited Hive/HBD |
| Lightning address | `"bob@getalby.com tip"` | Resolve the address and pay the resulting invoice |
| `#paywithsats:NNNN` | `"lnbc5u1p...abc #paywithsats:5000"` | Hold NNNN sats from sender's *Keepsats* balance and use them to pay the Lightning invoice/address |
| `account #paywithsats:NNNN` | `"bob #paywithsats:4000"` | Transfer NNNN sats from sender's *Keepsats* to `account`'s *Keepsats* |
| `#HBD` | `"lnbc5u1p...abc #HBD"` | Use the HBD exchange rate when calculating the conversion (can combine with other flags) |
| `#v4vapp` | `"lnbc5u1p...abc #v4vapp"` | App identifier; stripped from public memos before broadcast |
| `#balance_request` | `"#balance_request"` | Query your current *Keepsats* balance; the deposited Hive is returned with the balance in the memo |
| `#balance_request private` | `"#balance_request private"` | Same as above but the reply memo is encrypted using Hive's encrypted memo system |
| No recognised flag | `"hello world"` | Defaults to a *Keepsats* deposit (same as `#sats`) |

Flags are **case‑insensitive** and can appear anywhere in the memo. Multiple flags can be combined (e.g. `"lnbc... #paywithsats:5000 #HBD #v4vapp"`).

---

## 1. Deposit Hive/HBD as *Keepsats* (`#sats`)

The simplest interaction: send Hive or HBD to the server account with `#sats` in the memo. The entire amount is converted to Lightning satoshis at the current exchange rate and credited to the sender's *Keepsats* balance.

### Example

```
From:   v4vapp-test
To:     v4vapp          (server account)
Amount: 5.000 HIVE
Memo:   "deposit | #sats"
```

### What happens

1. The transfer is recorded and a `CUSTOMER_HIVE_IN` ledger entry is created.
2. `conversion_hive_to_keepsats()` converts the HIVE amount to msats using the live BTC/HIVE rate.
3. A fee is calculated and held.
4. Ledger entries are written: `HOLD_KEEPSATS`, `CONV_HIVE_TO_KEEPSATS`, `CONTRA_HIVE_TO_KEEPSATS`, `CONV_CUSTOMER`, `RELEASE_KEEPSATS`.
5. A `custom_json` notification is broadcast on‑chain crediting the customer's balance.
6. A separate `custom_json` fee notification is broadcast recording the fee.
7. If the deposited amount produced change (rounding), a small HIVE transfer is returned to the sender.

### Ledger entries (typical: 7)

| Type | Description |
|---|---|
| `CUSTOMER_HIVE_IN` | Initial deposit recorded |
| `HOLD_KEEPSATS` | Sats reserved during conversion |
| `CONV_HIVE_TO_KEEPSATS` | Conversion from Hive to sats |
| `CONTRA_HIVE_TO_KEEPSATS` | Offset entry for the conversion |
| `CONV_CUSTOMER` | Net customer conversion |
| `RELEASE_KEEPSATS` | Hold released on completion |
| `CUSTOM_JSON_FEE` | Fee charged |
| `FEE_INCOME` | Fee recognised as income |

A notification `custom_json` and optional change transfer are also broadcast.

> **Note:** If the memo contains no recognised flags at all, the system defaults to this behaviour — treating the transfer as a *Keepsats* deposit.

---

## 2. Pay a Lightning Invoice

Include a BOLT‑11 Lightning invoice (`lnbc...`, `lntb...`, or `lnbcrt...`) in the memo. The system converts the deposited Hive/HBD into sats and pays the invoice via LND.

### Example

```
From:   v4vapp-test
To:     v4vapp
Amount: 3.500 HIVE
Memo:   "lnbc277880n1p0d7u2epp5... coffee payment"
```

### What happens

1. The transfer is recorded (`CUSTOMER_HIVE_IN`).
2. The invoice is decoded; amount and expiry are validated.
3. Hive is converted to sats and the invoice is paid through LND.
4. On success: conversion, fee, and payment ledger entries are written; a notification `custom_json` confirms the payment.
5. On failure (expired, routing error, etc.): the full Hive amount is returned to the sender.

### Ledger entries (typical: 12 per invoice)

The entries follow the same pattern as a `#sats` deposit plus additional payment and fee entries for the Lightning leg.

---

## 3. Pay a Lightning Address

Include a Lightning address (`user@domain`) in the memo. The system resolves the address via LNURL, obtains an invoice, and pays it.

### Example

```
From:   v4vapp-test
To:     v4vapp
Amount: 2.000 HIVE
Memo:   "bob@getalby.com thanks for the podcast"
```

The `⚡` emoji or `lightning:` prefix before the address is also accepted and stripped automatically.

### What happens

The flow is identical to paying a Lightning invoice (section 2), except the system first resolves the Lightning address into a BOLT‑11 invoice before proceeding.

---

## 4. Pay with Existing *Keepsats* Balance (`#paywithsats`)

When a memo contains both a Lightning invoice/address **and** the `#paywithsats:NNNN` flag, the system uses sats from the sender's existing *Keepsats* balance instead of (or in addition to) converting the deposited Hive.

### Example

```
From:   v4vapp-test
To:     v4vapp
Amount: 0.001 HIVE          ← marker amount (not used for payment)
Memo:   "lnbc277880n1p0d7u2epp5... #paywithsats:5000"
```

### What happens

1. The transfer is recorded (`CUSTOMER_HIVE_IN`).
2. 5000 sats are **held** from the sender's *Keepsats* balance (`HOLD_KEEPSATS`).
3. The Lightning invoice is decoded and paid via LND using the held sats.
4. On success: the hold is consumed and conversion/payment ledger entries are written.
5. On failure: the hold is released (sats returned to sender's balance) and the Hive deposit is returned.

The Hive amount sent is typically a small marker (e.g. `0.001 HIVE`) since the actual payment comes from the *Keepsats* balance.

---

## 5. Internal *Keepsats* Transfer via Hive Transfer (`account #paywithsats:NNNN`)

When the memo starts with a **Hive account name** followed by `#paywithsats:NNNN` (and does *not* contain a Lightning invoice or address), the system treats it as an instruction to move sats between two *Keepsats* accounts.

### Example

```
From:   v4vapp-test
To:     v4vapp
Amount: 0.001 HIVE          ← marker amount
Memo:   "v4vapp.qrc #paywithsats:4000"
```

### What happens

1. The transfer is recorded (`CUSTOMER_HIVE_IN` for the 0.001 HIVE marker).
2. The system extracts the recipient (`v4vapp.qrc`) and amount (`4000` sats) from the memo.
3. A `KeepsatsTransfer` custom JSON is broadcast: sender's *Keepsats* balance is debited and recipient's balance is credited.
4. A `CUSTOM_JSON_TRANSFER` ledger entry records the movement.
5. An optional notification `custom_json` may be sent to the recipient.

### Ledger entries (typical: 3–4)

| Type | Description |
|---|---|
| `CUSTOMER_HIVE_IN` | Marker Hive deposit recorded |
| `CUSTOM_JSON_TRANSFER` | Sats moved from sender to recipient |
| (optional) notification | Recipient informed of incoming sats |

This is the lightest transfer path — no Lightning payment, no Hive conversion, no fee. It is equivalent to a `custom_json` keepsats transfer (see [custom_json.md § 1](custom_json.md#1-transfer-keepsats-on-chain-balance-move)) but initiated via a Hive transfer memo instead of a custom JSON operation.

---

## 6. The `#HBD` Flag

The `#HBD` flag can be combined with any of the patterns above. It tells the conversion layer to use the **HBD exchange rate** instead of the default HIVE rate when calculating how many sats the deposited amount is worth. If the flag is absent, HIVE is assumed.

```
Memo: "lnbc... #paywithsats:5000 #HBD"
Memo: "deposit | #sats #HBD"
```

When the transfer asset itself is already HBD (e.g. `5.000 HBD`), the flag is redundant but harmless.

---

## 7. Balance Request (`#balance_request`)

Send a small transfer to the server account with `#balance_request` (or `balance_request`) in the memo to query your current *Keepsats* balance. The system looks up your balance and replies with a Hive transfer back to you containing the result in the memo.

### Example

```
From:   v4vapp-test
To:     v4vapp
Amount: 0.001 HIVE
Memo:   "#balance_request"
```

### What happens

1. The transfer is recorded (`CUSTOMER_HIVE_IN` ledger entry for the small deposit).
2. The system queries the sender's *Keepsats* balance.
3. A Hive transfer is sent back to the sender with a JSON memo containing:
   - `msats` — balance in millisatoshis
   - `sats` — balance in satoshis
   - `reply_to` — the short ID of the original transfer
   - `original_memo` — the memo from the original transfer
4. If the sender has any outstanding Hive balance owed by the system, the return amount is adjusted to include it; otherwise the original deposit amount is returned.

### Reply memo example

```json
{
  "return_details_str": "Current balance is 12,345 sats | timestamp 2026-04-12T10:00:00+00:00 | ...",
  "msats": 12345000,
  "sats": "12345.000",
  "reply_to": "7856_799039_1",
  "original_memo": "#balance_request"
}
```

### Private balance requests

If the word **`private`** appears anywhere in the memo alongside `#balance_request`, the reply transfer memo is **encrypted** using Hive's built‑in encrypted memo system. This means only the sender (who holds the corresponding memo key) can decrypt and read the balance. Useful when you don't want your balance visible on‑chain.

```
Memo: "#balance_request private"
Memo: "private #balance_request"
```

Both forms work — the system checks for the presence of the word `private` anywhere in the memo.

### Ledger entries

| Type | Description |
|---|---|
| `CUSTOMER_HIVE_IN` | Initial deposit recorded |
| `CUSTOMER_HIVE_OUT` | Return transfer to sender (with balance info) |

The balance request is a **read‑only** query — no sats are moved, no conversion takes place, and no fees are charged.

---

## Error Handling

All error scenarios result in the deposited Hive being returned to the sender. The system never silently loses funds.

| Error | System Response |
|---|---|
| Lightning invoice expired | Full Hive returned to sender |
| Lightning payment routing failure | Full Hive returned to sender |
| Insufficient *Keepsats* balance (for `#paywithsats`) | Hold released, Hive returned |
| Invoice amount exceeds configured maximum | Hive returned with error notification |
| Invoice amount below configured minimum | Hive returned with error notification |
| Account flagged as suspicious | Transfer redirected to `v4vapp.sus` |
| Unrecognised memo with Lightning decode failure | Treated as `#sats` deposit (fallback) |

Error notifications are sent back to the sender as `custom_json` operations. See [custom_json.md § System‑generated notifications](custom_json.md#system-generated-notifications) for the notification format.

---

## Decision Tree

The system processes transfer memos in this order of priority:

```
Transfer arrives at server account
│
├─ Is this a fee memo?
│  └─ YES → Skip (internal bookkeeping)
│
├─ Does the operation already have replies?
│  └─ YES → Skip (already processed)
│
├─ Is this a #balance_request?
│  └─ YES → Look up balance, reply with Hive transfer
│           (encrypt memo if "private" in memo)
│
├─ Is this a #sats / #keepsats deposit?
│  └─ YES → Convert full amount to Keepsats
│
├─ Does memo contain a Lightning invoice or address?
│  ├─ YES + #paywithsats → Hold sats, pay invoice, release on success
│  └─ YES (no paywithsats) → Convert Hive to sats, pay invoice
│
├─ Does memo match "account #paywithsats:NNNN"?
│  └─ YES → Internal keepsats transfer to named account
│
└─ None of the above
   └─ Default to #sats deposit (convert all to Keepsats)
```

---

## Relationship to Custom JSON Operations

Hive transfers and custom JSON operations are two parallel ways to interact with *Keepsats*:

| Action | Via Hive Transfer | Via Custom JSON |
|---|---|---|
| Query Keepsats balance | `0.001 HIVE` + `#balance_request` memo | N/A (transfers only) |
| Deposit Hive/HBD as sats | `amount HIVE` + `#sats` memo | N/A (transfers only) |
| Pay Lightning invoice | Invoice in memo | Invoice in `memo` field of `KeepsatsTransfer` |
| Pay Lightning address | Address in memo | Address in `memo` field of `KeepsatsTransfer` |
| Transfer sats to another user | `0.001 HIVE` + `"bob #paywithsats:500"` | `KeepsatsTransfer` with `to_account: "bob"` |
| Convert sats back to Hive/HBD | N/A (use custom JSON) | `KeepsatsTransfer` to server account |

See [custom_json.md](custom_json.md) for the custom JSON interface.
