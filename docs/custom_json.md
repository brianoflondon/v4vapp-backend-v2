
# Using Custom Json on Hive to Interact with *Keepsats*

## Background

There are two main ways to deposit *Keepsats* on the v4v.app system: send a Hive or HBD transfer to the `server` Hive account with `#sats` anywhere in the memo, or receive lightning on a Lightning address on the system (e.g. `brianoflondon@sats.v4v.app`).

## Paying a Lightning Invoice with *Keepsats*

### Using the Web

The web‑based app on [v4v.app](https://v4v.app) allows you to spend *Keepsats* directly.

## Custom Json

External users can interact with the v4v.app backend by submitting a Hive `custom_json` operation. The operation must be signed with the **active key** of the `from_account` and the JSON payload must include `from_account` matching that authority. This requirement protects against replay attacks and ensures the sender consents to the action.

There are three distinct behaviours the system recognises based on the fields you supply:

1. **Keepsats transfer between two v4v accounts**
2. **Send sats to a Lightning address**
3. **Pay a Lightning invoice**

All of the custom‑json operations use the same `id` value: `v4vapp_transfer`.

### 1. Transfer Keepsats (on‑chain balance move)

Use this when you simply want to move sats from one v4v user to another.

```json
{
  "id": "v4vapp_transfer",
  "json": {
    "from_account": "alice",
    "to_account": "bob",
    "sats": 500000,
    "memo": "thanks for the coffee"
  }
}
```

* `from_account` – must match the active authority of the transaction.
* `to_account` – the Hive account that will receive the sats. This account must already be registered with the v4v system (i.e. has a keepsats balance).
* `sats` or `msats` – the amount to transfer from sender to receiver. `msats` are milisatoshis, 1/1000ths of a sat. If you specify `msats` then `sats` value will be ignored. All internal accounting is actually in `msats`.
* `memo` – optional free‑form text stored with the transfer.

The backend debits `from_account`'s keepsats balance and credits `to_account` accordingly.

### 2. Send to a Lightning Address

This lets an external user withdraw sats from their keepsats balance and push them to any Lightning address. You may also include the optional `invoice_message` field (see `KeepsatsTransfer` model) – if present it will be forwarded to the remote service when the address is resolved and eventually delivered as a memo to the recipient on the Lightning network.

```json
{
  "id": "v4vapp_transfer",
  "json": {
    "from_account": "alice",
    "memo": "bob@sats.v4v.app",
    "sats": 200000,
    "invoice_message": "coffee and conversation"
  }
}
```

* `memo` – a Lightning address (for example `user@sats.v4v.app`).
* `sats` (or `msats`) – the amount to send. The system will attempt to route the payment for exactly this amount.
* `invoice_message` – optional text included when resolving the address; delivered to the recipient if the address resolution results in an invoice.

The `to_account` field is ignored for this type of request – sats are pulled from the sender's balance and forwarded on‑chain via LND. The `from_account` requirement still applies.

### 3. Pay a Lightning Invoice

When the `memo` field contains a **BOLT‑11 invoice**, the system will try to settle that invoice on behalf of the `from_account`. The invoice itself encodes the amount, so you **must not** supply `sats` in the JSON unless you want to impose an upper limit on the invoice value (see below).

```json
{
  "id": "v4vapp_transfer",
  "json": {
    "from_account": "alice",
    "memo": "lnbc1...",
    "sats": 100000            # optional limit, invoice must be <= this
  }
}
```

* `memo` – full Lightning invoice string. The amount is fixed inside the invoice.
* `sats`/`msats` – optional. If provided, the invoice amount cannot exceed this value; otherwise the request is rejected.

The backend pays the invoice using the sender's keepsats balance.

---

### Additional notes

* If both a Lightning address and an invoice are supplied in `memo`, the system treats it as an invoice and ignores the address.
* For any request that sends sats outside the v4v ledger (cases 2 and 3 above), the `to_account` field is not required and is ignored if present.
* All custom JSON operations must be broadcast as a Hive transaction signed by the active key of `from_account`. Unsigned or improperly signed operations are rejected.
* Amounts are denominated in satoshis. Use `msats` if you require millisecond precision.

By structuring your `custom_json` payload according to one of the patterns above you can automate deposits and withdrawals without ever touching the web interface.

---

## System‑generated notifications

Any time the backend needs to inform you about the status of a previous operation it will send back a **notification** custom JSON. These are not submitted by users – they are sent from the server account and carry details of errors, refunds, conversion results, or other informational messages.

* **ID** – `PREFIX_notification` (e.g. `v4vapp_notification` on production).
* **from_account** – always the server’s Hive account name.
* **to_account** – the original requesting `from_account`.
* **memo** – human‑readable message describing what happened. Errors such as “Insufficient Keepsats balance” are common.
* **msats** – zero for pure notifications; if the notification accompanies a transfer it will contain the amount that was moved.
* **parent_id** – group id of the original operation, useful for tracing.
* **notification** – boolean flag set to `true`.

Example of a failure notice returned when a user’s invoice payment could not be completed:

```json
{
  "id": "v4vapp_notification",
  "json": {
    "from_account": "v4vapp",
    "to_account": "alice",
    "memo": "Insufficient Keepsats balance | § XYZ123",
    "msats": 0,
    "parent_id": "XYZ123",
    "notification": true
  }
}
```

You can monitor Hive operations for notifications if you wish to automate logging or alerts. They are purely informational – the system does **not** attempt to act on them further but they will be stored in the ledger under the `CUSTOM_JSON_TRANSFER`/`CUSTOM_JSON_FEE` types depending on context.

Notifications are also generated for successful replies when the backend performs a conversion or other return and chooses to send a result via custom json rather than a Hive transfer. These replies follow the same format, with a non‑zero `msats` when sats are being returned.
