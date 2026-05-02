# Magisats — Lightning ↔ Magi (VSC) BTC Bridge

## Background

**Magisats** is the name for Bitcoin satoshis that travel via the **Magi** layer — a VSC (Virtual Smart Contracts) contract deployed on the Hive blockchain.  In the Magi system every Hive account is addressed as `hive:<accountname>`, and BTC balances are maintained by a smart contract (`vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d`) on `vsc-mainnet`.

The v4vapp backend acts as a bridge in **both directions**:

| Direction | Trigger | Outcome |
|---|---|---|
| **Outbound** (Lightning → Magi) | A Lightning invoice is paid with `#magisats` + `#v4vapp` in the memo | Server forwards the sats to the customer's Magi (`hive:accountname`) address via a VSC `transfer` call |
| **Inbound** (Magi → Keepsats / Lightning) | A third party sends sats to the server's Magi address; memo contains `#magioutbound` | Server receives the sats, credits Exchange Holdings, then optionally forwards them onward via Lightning (`pay_with_sats`) |

For background on *Keepsats* and the standard Lightning deposit flow, see [hive_transfers.md](hive_transfers.md) and [custom_json.md](custom_json.md).

---

## Invoice Memo Tag — `#magisats`

A Lightning invoice is identified as a Magisats request by the presence of **both** `#magisats` and `#v4vapp` in the memo field.

```
regex: ^\s*\S+.*#magisats(?:\s+(\d+))?.*#v4vapp
```

The first non-whitespace token before the tags is treated as the **destination account** (`cust_id`), e.g.:

```
alice #magisats #v4vapp
```

The optional integer after `#magisats` is reserved for a future explicit amount parameter.

Detection happens in `Invoice.model_post_init()` via `MAGISATS_TAG` from `helpers/regex_constants.py`, which sets `invoice.is_magisats = True`.

---

## Flow 1: Outbound — External Lightning → Magi VSC

### Summary

A user pays a Lightning invoice whose memo identifies a Hive account + `#magisats #v4vapp`. The server:

1. Receives the Lightning payment.
2. Creates an initial `DEPOSIT_LIGHTNING` accounting entry (via the standard invoice handler).
3. Calls `forward_magisats()` instead of the normal Keepsats deposit path.
4. Broadcasts a VSC `transfer` `custom_json` on Hive sending the net sats (after fee) to `hive:<cust_id>`.
5. When the Magi indexer confirms the on-chain transfer, `process_magi_btc_transfer_event()` → `magisats_outbound()` writes the final accounting entries.

### Accounting entries

**Step A — Lightning deposit (standard invoice handler)**

| # | Type | Debit | Credit | Amount |
|---|---|---|---|---|
| 1 | `DEPOSIT_LIGHTNING` | External Lightning Payments (umbrel) | VSC Liability (`server_id`) | full received msats |

**Step B — Outbound accounting (`magisats_outbound`), recorded when the Magi indexer confirms the VSC transfer**

| # | Type | Debit | Credit | Amount |
|---|---|---|---|---|
| 2 | `MAGI_OUTBOUND` | VSC Liability (`server_id`) | Exchange Holdings (`exchange_name`) | `amount_sent_msats` (net forwarded) |
| 3 | `FEE_INCOME` | VSC Liability (`server_id`) | Fee Income Magisats (`exchange_name`) | `net_fee_msats` (received − sent) |

After step B the VSC Liability returns to zero.  Exchange Holdings holds the forwarded sats, and revenue increases by the fee.

> **Fee derivation:** `net_fee_msats = original_invoice.value_msat − amount_sent_msats`.  This is cross-checked against the fee embedded in the VSC payload (`vsc_payload.msats_fee`) and must be ≥ that value.

**Step C — On-chain notification (non-accounting)**

A `KeepsatsTransfer` `custom_json` is broadcast on Hive from the server to the customer (`msats=0`, `notification=True`) to inform wallets / clients of the transfer completion.

### Overwatch flow: `external_to_magisats`

```
1. invoice op          — #MAGISATS-tagged Lightning invoice           (primary)
2. deposit_l ledger    — DEPOSIT_LIGHTNING                            (primary)
3. custom_json op      — VSC vsc.call transfer sent by server         (vsc_send)
4. magi_btc_transfer   — Magi indexer confirms VSC transfer           (magi_receive)
5. magi_outbound       — MAGI_OUTBOUND ledger                         (magi_receive)
6. fee_income          — FEE_INCOME ledger                            (magi_receive)
7. custom_json op      — KeepsatsTransfer notification (OPTIONAL)     (magi_notification)
```

### Code path

```
lnd_monitor_v2  ──►  process_invoice()         [invoice.is_magisats == True]
                         └──►  forward_magisats(invoice)
                                   └──►  send_magi_transaction(vsc_payload)
                                             └──►  send_magi_transfer_custom_json(vsc_call)

stream_magi     ──►  process_tracked_events()  [MagiBTCTransferEvent, vsc_call.caller == server]
                         └──►  process_magi_btc_transfer_event(magi_transfer)
                                   └──►  magisats_outbound(magi_transfer, vsc_call)
```

Relevant source files:

- [process/process_magi.py](../src/v4vapp_backend_v2/process/process_magi.py) — `forward_magisats`, `magisats_outbound`
- [process/process_invoice.py](../src/v4vapp_backend_v2/process/process_invoice.py) — `invoice.is_magisats` branch
- [magi/magi_general.py](../src/v4vapp_backend_v2/magi/magi_general.py) — `send_magi_transaction`
- [magi/stream_magi.py](../src/v4vapp_backend_v2/magi/stream_magi.py) — WebSocket subscription to Magi indexer

---

## Flow 2: Inbound — Magi VSC → Keepsats / Lightning

### Summary

A third party sends sats to the server's Magi address (`hive:<server_id>`).  The Magi stream indexer delivers a `MagiBTCTransferEvent` to the server.  The server:

1. Records Exchange Holdings increasing (sats arrived from Magi) and a VSC Liability to the customer.
2. Deducts a fee from the VSC Liability.
3. If the transfer memo contains `#magioutbound`, triggers a follow-on Lightning payment to the customer via `follow_on_transfer()`.

### The `#magioutbound` flag

The `#magioutbound` string in the Magi transfer memo enables the outbound Lightning leg:

```python
@property
def pay_with_sats(self) -> bool:
    return not self.do_not_pay and self.amount > 0


@property
def do_not_pay(self) -> bool:
    # Only pay onward if the memo explicitly contains #magioutbound
    if self.d_memo and "#magioutbound" in self.memo.lower():
        return False
    return True
```

Without `#magioutbound`, received sats remain as a VSC Liability (credited to the customer's Keepsats balance) with no Lightning payment.

### Customer ID resolution

`cust_id` is resolved in order:

1. Parsed from the memo via `ProcessedMemo` (if the memo contains a Hive account name).
2. Falls back to `magi_transfer.cust_id` (derived from `from_addr` / `to_addr`, excluding `server_id`).

### Accounting entries (`magisats_inbound`)

| # | Type | Debit | Credit | Amount |
|---|---|---|---|---|
| 1 | `MAGI_INBOUND` | Exchange Holdings (`exchange_name`) | VSC Liability (`cust_id`) | `amount_sent_msats` (full received) |
| 2 | `FEE_INCOME` | VSC Liability (`cust_id`) | Fee Income Magisats (`exchange_name`) | `net_fee_msats` (from `magi_transfer.conv.msats_fee`) |

After these entries:

- **Exchange Holdings** increases by `amount_sent_msats` (the sats now reside in the server's Magi wallet).
- **VSC Liability** to the customer is `net_to_customer_msats = amount_sent_msats − net_fee_msats`.
- **Revenue** increases by `net_fee_msats`.

If `pay_with_sats` is True, `follow_on_transfer()` is called to pay the customer's Lightning invoice or address and the VSC Liability is subsequently cleared by those entries.

### Code path

```
stream_magi  ──►  process_tracked_events()   [MagiBTCTransferEvent, payload.to == server magi addr]
                      └──►  process_magi_btc_transfer_event(magi_transfer)
                                └──►  magisats_inbound(magi_transfer, vsc_call)
                                          └──►  follow_on_transfer(magi_transfer)  [if pay_with_sats]
```

Relevant source files:

- [process/process_magi.py](../src/v4vapp_backend_v2/process/process_magi.py) — `magisats_inbound`
- [process/process_transfer.py](../src/v4vapp_backend_v2/process/process_transfer.py) — `follow_on_transfer` (handles `MagiSatsInboundFollowOnTransferError`)
- [magi/magi_classes.py](../src/v4vapp_backend_v2/magi/magi_classes.py) — `MagiBTCTransferEvent`, `pay_with_sats`, `do_not_pay`

---

## Magi Address Format

Hive accounts are represented in the Magi / VSC system with a `hive:` prefix:

| Hive account | Magi address |
|---|---|
| `alice` | `hive:alice` |
| `devser.v4vapp` | `hive:devser.v4vapp` |

The `AccName` helper provides this via `.magi_prefix`:

```python
AccName("alice").magi_prefix  # → "hive:alice"
```

---

## VSC Call Structure

When the server forwards sats outbound it broadcasts a `custom_json` with `id = "vsc.call"`:

```json
{
  "net_id": "vsc-mainnet",
  "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
  "action": "transfer",
  "caller": "hive:devser.v4vapp",
  "payload": {
    "amount": "500",
    "to": "hive:alice",
    "parent_id": "<invoice_group_id>",
    "msats_fee": "60000",
    "memo": "Received 500 sats via v4v.app ⚡"
  },
  "rc_limit": 2000,
  "intents": []
}
```

`parent_id` links the VSC transfer back to the originating Lightning invoice so `magisats_outbound` can retrieve the original received amount and compute the true fee.

---

## API Endpoint — Fee Calculator for Inbound Magi Transfers

```
GET /v2/crypto/to_keepsats/?keepsats=<N>
```

Use this endpoint **before** sending Magi sats to the server's Magi address. Given the number of sats you want to **receive** in your Keepsats balance (`keepsats`), it tells you exactly how many Magi sats you need to **send** so that after the conversion fee and the Lightning routing fee estimate are deducted, you end up with the requested amount — plus any change that remains will stay in your balance.

### Query parameter

| Parameter | Type | Description |
|---|---|---|
| `keepsats` | `int` | The number of sats you want credited to your Keepsats balance after all fees |

### Response fields

| Field | Type | Description |
|---|---|---|
| `receive_sats` | `int` | Echo of the requested `keepsats` amount |
| `fee_sats` | `int` | Server conversion fee in whole sats |
| `fee_msats` | `int` | Server conversion fee in millisatoshis (precise) |
| `forwarding_fee_estimate_sats` | `int` | Estimated Lightning routing fee in whole sats |
| `forwarding_fee_estimate_msats` | `int` | Estimated Lightning routing fee in millisatoshis |
| `total_to_send_sats` | `int` | **Total Magi sats you must send** (`receive_sats + fee_sats + forwarding_fee_estimate_sats`, rounded up) |

### How to use it

1. Call the endpoint with the amount you want to receive.
2. Send **`total_to_send_sats`** to the server's Magi address.
3. The server deducts the conversion fee and routing fee from what you sent.
4. Your Keepsats balance is credited with `receive_sats` (any fractional remainder becomes change in your balance).

### Example

```
GET /v2/crypto/to_keepsats/?keepsats=633333333
```

```json
{
  "receive_sats": 633333333,
  "fee_sats": 633333,
  "fee_msats": 633333333,
  "forwarding_fee_estimate_sats": 1267933,
  "forwarding_fee_estimate_msats": 1267933000,
  "total_to_send_sats": 635234599
}
```

In this example you would send **635,234,599 Magi sats** to receive **633,333,333 Keepsats** after the server fee (633,333 sats) and the routing fee estimate (1,267,933 sats) are covered.

> **Note:** The forwarding fee is an *estimate*. The actual routing fee charged by the Lightning network may be slightly lower, in which case the difference remains in your Keepsats balance as change.

---

## Ledger Types Summary

| `LedgerType` | Used in | Meaning |
|---|---|---|
| `DEPOSIT_LIGHTNING` | Flow 1, Step A | Full received Lightning amount recorded |
| `MAGI_OUTBOUND` | Flow 1, Step B | Net forwarded sats cleared from VSC Liability to Exchange Holdings |
| `FEE_INCOME` | Both flows | Server fee recognised as revenue |
| `MAGI_INBOUND` | Flow 2 | Sats received from Magi into Exchange Holdings; VSC Liability created for customer |

---

## Error Handling

- **`AssertionError`** in `process_magi_btc_transfer_event`: logged and an empty list returned. Common causes: amount mismatch between VSC payload and Magi transfer event, missing `parent_id`, negative fee.
- **`MagiSatsInboundFollowOnTransferError`**: raised in `process_transfer.py` if the follow-on Lightning payment fails for an inbound Magi transfer; caught and logged without unwinding the accounting entries already written.
- **`MagiBTCBalanceError`**: raised in `magi_balances.py` when the Magi balance API is unreachable.
- Self-transfers (server sends to itself) are detected and skipped with a warning log.
