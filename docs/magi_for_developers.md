# Magi for Developers — Integrating with v4v.app via VSC

This document describes how external developers can interact with the v4v.app service
using the **Magi** layer — VSC (Virtual Smart Contracts) running on the Hive blockchain.
It covers both on-chain interaction patterns (sending `vsc.call` custom_json operations)
and the REST API fee-calculator endpoints.

---

## Overview

**Magi** is a VSC smart-contract system layered on top of the Hive blockchain.
Bitcoin (in satoshis) can be moved to and from regular Lightning Network payments via
the v4v.app server acting as a bridge.

| Direction | What you do | What the server does |
|---|---|---|
| **Outbound** (Lightning → Magi) | Pay a Lightning invoice with `#magisats #v4vapp` in the memo | Forwards net sats (after fee) to your Magi/VSC address via an on-chain VSC `transfer` |
| **Inbound** (Magi → Keepsats / Lightning) | Send sats to the server's Magi address with a memo | Credits your Keepsats balance; optionally pays a Lightning invoice or address onward |

---

## Magi / VSC Address Format

Every Hive account has a corresponding Magi address formed with a `hive:` prefix:

| Hive account | Magi address |
|---|---|
| `alice` | `hive:alice` |
| `v4vapp` | `hive:v4vapp` |

EVM (Ethereum-style) addresses are also supported:

| EVM address | Magi address |
|---|---|
| `0xAbCd...1234` | `did:pkh:eip155:1:0xabcd...1234` |

---

## The v4v.app Server Account — `hive:v4vapp`

The Hive account that operates the v4v.app bridge is **`v4vapp`**.  Its Magi address is
therefore **`hive:v4vapp`**.

| Purpose | Address |
|---|---|
| Send sats **to** v4v.app (inbound / deposit) | `hive:v4vapp` |
| Sats received **from** v4v.app (outbound / forwarding) | originate from `hive:v4vapp` |

All `vsc.call` transfer payloads targeting v4v.app must set `payload.to = "hive:v4vapp"`.

When the server sends you sats after a `#magisats` Lightning payment is settled, the
on-chain VSC `transfer` will show `hive:v4vapp` as the `caller`.

> **Verification tip:** Always check that the `caller` field in an inbound VSC transfer
> is `hive:v4vapp` before treating it as a v4v.app payout.

---

## On-chain VSC `transfer` Call Structure

To send sats via Magi, broadcast a Hive `custom_json` with `id = "vsc.call"`.
The JSON body must follow the `VSCCall` schema:

```json
{
  "net_id": "vsc-mainnet",
  "caller": "hive:your-account",
  "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
  "action": "transfer",
  "payload": {
    "amount": "1000",
    "to": "hive:v4vapp",
    "memo": "<your memo here>"
  },
  "rc_limit": 2000
}
```

### Field reference

| Field | Type | Required | Description |
|---|---|---|---|
| `net_id` | string | yes | Always `"vsc-mainnet"` for production |
| `caller` | string | yes | Your Hive Magi address, e.g. `"hive:alice"` |
| `contract_id` | string | yes | Always `"vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d"` (the Magi BTC contract) |
| `action` | string | yes | Always `"transfer"` |
| `payload.amount` | string | yes | Integer sats to send, as a string e.g. `"1000"` |
| `payload.to` | string | yes | Recipient Magi address e.g. `"hive:v4vapp"` |
| `payload.memo` | string | no | Optional memo — controls routing behaviour (see below) |
| `rc_limit` | int | no | Resource Credit limit; `2000` is sufficient for most transfers |

> **Note:** `amount` must be a string (not a raw integer), though the server's
> Pydantic model will coerce numeric values for tolerance.  Use a string to be safe.

---

## Inbound Flow: Sending Sats to v4v.app

Send sats to the server's Magi address (e.g. `hive:v4vapp`).
The memo field controls what happens next.

### Memo tags

| Tag | Effect |
|---|---|
| *(no special tag)* | Sats are credited to your **Keepsats** balance (VSC Liability for your account) |
| `#magioutbound` | Triggers a follow-on Lightning payment to the address/invoice in the memo |
| `#paywithsats:<N>` | Caps the follow-on Lightning payment at *N* sats; any remainder goes back to you |
| `#v4vapp` | Identifies v4v.app as the service; include in every memo |

### Example 1: Deposit to Keepsats balance

Sends 200 sats to the server; the server credits `hive:v4vapp-test` with (200 − fee) sats
in their Keepsats balance.

```json
{
  "net_id": "vsc-mainnet",
  "caller": "hive:v4vapp-test",
  "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
  "action": "transfer",
  "payload": {
    "amount": "200",
    "to": "hive:v4vapp",
    "memo": "v4vapp.qrc | deposit to keepsats | #v4vapp"
  },
  "rc_limit": 2000
}
```

### Example 2: Pay a Lightning address

Sends 1000 sats to the server and requests the server pay `brianoflondon@walletofsatoshi.com`.
The Lightning address is the first token before the `|` separator.

```json
{
  "net_id": "vsc-mainnet",
  "caller": "hive:v4vapp-test",
  "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
  "action": "transfer",
  "payload": {
    "amount": "1000",
    "to": "hive:v4vapp",
    "memo": "brianoflondon@walletofsatoshi.com | test payment | #v4vapp #magioutbound"
  },
  "rc_limit": 2000
}
```

### Example 3: Pay a Lightning invoice

Sends 1300 sats and pays a specific BOLT-11 invoice for 1000 sats.
The `#paywithsats:1300` tag caps the outbound payment to 1300 sats; any remainder
after the fee and the invoice amount is returned to the sender.

```json
{
  "net_id": "vsc-mainnet",
  "caller": "hive:v4vapp-test",
  "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
  "action": "transfer",
  "payload": {
    "amount": "1300",
    "to": "hive:v4vapp",
    "memo": "lnbc10u1p... | #v4vapp #magioutbound #paywithsats:1300"
  },
  "rc_limit": 2000
}
```

> **Tip:** Use the `fee_in` API endpoint (see below) to calculate how many sats you need to
> send for a given receive amount.

### Customer ID resolution

The server determines which Keepsats account to credit in the following order:

1. The first recognisable Hive account name parsed from the memo by `ProcessedMemo`.
2. Falls back to the `from_addr` of the Magi transfer (the sender).

To credit a different account than the sender, put that account name first in the memo:

```
v4vapp.qrc | your message | #v4vapp
```

---

## Outbound Flow: Receiving Sats from v4v.app in Your Magi Wallet

To receive sats in your Magi wallet, pay a Lightning invoice that has **both**
`#magisats` and `#v4vapp` in its memo field.

### Invoice memo format

```
<hive-account> #magisats #v4vapp
```

Example invoice memo:

```
alice #magisats #v4vapp
```

The server:

1. Receives the Lightning payment.
2. Deducts the service fee.
3. Broadcasts a VSC `transfer` on Hive sending the net sats to `hive:alice`.

Use the `fee_out` API endpoint to calculate the service fee before generating the invoice.

---

## Fee Calculator API Endpoints

The v4v.app API provides two fee calculator endpoints under the `magisats` router.
These are stateless and do **not** require authentication.

> **Base URL (production):** `https://api.v4v.app/v2/magisats/`

### `GET /fee_out/` — Fee for outbound (Lightning → Magi) transfers

Calculates the fees for receiving sats in your Magi wallet by paying a Lightning invoice.
Includes both the v4v.app service fee **and** an estimated Lightning routing/forwarding fee.

Also reachable as `GET /fee/` (alias).

#### Query parameters

Provide **exactly one** of:

| Parameter | Type | Description |
|---|---|---|
| `sats` | `int` | The number of sats you want to **receive** in your Magi wallet |
| `reverse_sats` | `int` | The total sats you want to **send** in the Lightning payment; the API reverse-calculates the receive amount |

#### Response fields

| Field | Type | Description |
|---|---|---|
| `receive_sats` | `int` | Sats that will arrive in your Magi wallet |
| `fee_sats` | `int` | v4v.app service fee in whole sats |
| `fee_msats` | `int` | v4v.app service fee in millisatoshis (precise) |
| `forwarding_fee_estimate_sats` | `int` | Estimated Lightning routing fee in whole sats |
| `forwarding_fee_estimate_msats` | `int` | Estimated Lightning routing fee in millisatoshis |
| `total_to_send_sats` | `int` | **Total sats to include in the Lightning invoice** (rounded up) |

#### Example — forward calculation (sats to receive)

```bash
curl -X 'GET' \
  'https://api.v4v.app/v2/magisats/fee_out?sats=2222' \
  -H 'accept: application/json'
```

```json
{
  "receive_sats": 2222,
  "fee_sats": 6,
  "fee_msats": 6222,
  "forwarding_fee_estimate_sats": 2,
  "forwarding_fee_estimate_msats": 2272,
  "total_to_send_sats": 2231
}
```

Here you would generate a Lightning invoice for **2231 sats**.  When paid, the server
deducts the fee (6 sats) and routing estimate (2 sats) and forwards **2222 sats** to your
Magi wallet.

> The forwarding fee is an *estimate*.  The actual network routing fee may be lower,
> in which case the difference is credited to your Keepsats balance.

#### Example — reverse calculation (total to send is known)

```bash
curl -X 'GET' \
  'https://api.v4v.app/v2/magisats/fee_out?reverse_sats=2231' \
  -H 'accept: application/json'
```

```json
{
  "receive_sats": 2222,
  "fee_sats": 6,
  "fee_msats": 6222,
  "forwarding_fee_estimate_sats": 2,
  "forwarding_fee_estimate_msats": 2272,
  "total_to_send_sats": 2231
}
```

---

### `GET /fee_in/` — Fee for inbound (Magi → Keepsats) transfers

Calculates the fees for depositing sats from Magi into your v4v.app Keepsats balance.
This endpoint returns **only** the v4v.app service fee; it does **not** include a
Lightning routing/forwarding fee estimate because no Lightning payment is made for a
pure Keepsats deposit.

#### Query parameters

Same as `fee_out`:

| Parameter | Type | Description |
|---|---|---|
| `sats` | `int` | The number of sats you want to receive in your Keepsats balance |
| `reverse_sats` | `int` | The total Magi sats you want to send; the API reverse-calculates the receive amount |

#### Response fields

Identical to `fee_out`. `forwarding_fee_estimate_sats` and `forwarding_fee_estimate_msats`
will always be `0`.

#### Example

```bash
curl -X 'GET' \
  'https://api.v4v.app/v2/magisats/fee_in?sats=2222' \
  -H 'accept: application/json'
```

```json
{
  "receive_sats": 2222,
  "fee_sats": 6,
  "fee_msats": 6222,
  "forwarding_fee_estimate_sats": 0,
  "forwarding_fee_estimate_msats": 0,
  "total_to_send_sats": 2229
}
```

Here you would send **2229 Magi sats** to the server's Magi address to have **2222 sats**
credited to your Keepsats balance after the service fee (6 sats) is deducted.

#### Use with `#paywithsats`

When using the `#paywithsats:<N>` memo tag, call `fee_in` with `reverse_sats=<N>` to
find out how many sats will end up in your Keepsats balance after the fee:

```bash
curl -X 'GET' \
  'https://api.v4v.app/v2/magisats/fee_in?reverse_sats=1300' \
  -H 'accept: application/json'
```

The `receive_sats` in the response is the maximum net amount available for the follow-on
Lightning payment after the service fee is deducted from your `#paywithsats:1300` cap.

---

## Fee Structure

The service fee is composed of two parts (configured in `V4VConfig`):

```
fee = (conv_fee_percent + MARGIN_SPREAD) × amount_msats + conv_fee_sats × 1_000
```

The Lightning routing fee estimate uses the `lnd_config` settings:

```
forwarding_fee_estimate = lightning_fee_base_msats + amount_msats × lightning_fee_estimate_ppm / 1_000_000
```

Typical values (subject to change):

| Parameter | Typical value |
|---|---|
| `conv_fee_percent` | ~0.1 % |
| `conv_fee_sats` (flat) | 4 sats |
| `lightning_fee_base_msats` | 50 000 msats (50 sats) |
| `lightning_fee_estimate_ppm` | 1 000 ppm |

---

## Error Responses

The fee endpoints return `HTTP 400` with a JSON detail message for invalid inputs:

| Condition | Detail |
|---|---|
| Both `sats` and `reverse_sats` provided | `"Provide exactly one of sats or reverse_sats"` |
| Neither provided | `"Provide exactly one of sats or reverse_sats"` |
| `sats <= 0` | `"Invalid sats amount"` |
| `reverse_sats <= 0` | `"Invalid reverse_sats amount"` |
| `reverse_sats` cannot be matched | `"reverse_sats cannot be matched with the current fee structure"` |

---

## OpenAPI / Swagger UI

When the API server is running, interactive documentation is available at:

```
https://api.v4v.app/docs
```

The Magisats fee endpoints appear under the **v2/magisats** tag.

---

## Quick-reference: Hive broadcast (Python / beem)

```python
from nectar.hive import Hive
import json

hive = Hive(keys=["<your-active-key>"])

vsc_call = {
    "net_id": "vsc-mainnet",
    "caller": "hive:your-account",
    "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
    "action": "transfer",
    "payload": {
        "amount": "1000",
        "to": "hive:v4vapp",
        "memo": "your message #v4vapp #magioutbound",
    },
    "rc_limit": 2000,
}

hive.custom_json(
    id="vsc.call",
    json_data=json.dumps(vsc_call),
    required_auths=["your-account"],
)
```

---

## Related documentation

- [magisats.md](magisats.md) — Internal architecture, accounting flows, and code paths
- [custom_json.md](custom_json.md) — General `custom_json` operations and Keepsats transfers
- [hive_transfers.md](hive_transfers.md) — Hive HIVE/HBD → Lightning deposit flow
- [fixed_quote.md](fixed_quote.md) — Fixed-quote Lightning invoices (used in the outbound flow)
