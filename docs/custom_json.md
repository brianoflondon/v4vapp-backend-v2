
# Using Custom Json on Hive to Interact with *Keepsats*

## Background

There are two main ways to deposit *Keepsats* on the v4v.app system:

1. Send a Hive or HBD Transfer to the `server` Hive Account with `#sats` anywhere in the memo.
2. Receive Lightning on a Lightning address on the system eg `brianoflondon@sats.v4v.app`



## Paying a Lightning Invoice with *Keepsats*

### Using the Web

The web based app on [v4v.app](https://v4v.app) allows you to spend *Keepsats* directly.

## Custom Json

To initiate a payment with a `custom_json` you need to create a Hive `custom_json` with the following characteristics.

The `from_account` in the `custom_json` data must match the Active Authority on the transaction. This is to prevent replay attacks where a malicious actor could try to replay a transaction from another account.

### Send Lightning


* ID: `v4vapp_transfer`

* Json containing the following fields:
  * `from_account` - the sending account, this needs to match the Active Authority on the transaction
  * `to_account` - the server's account (`@v4vapp` on the live system) corresponds to the server's Hive account.
  * `memo` : either a lightning invoice or a lightning address, this can have extra information in it that will be passed along if possible.
  * Optional `sats` or `msats` : if the memo is a lightning address or a zero value invoice, this is the amount of sats to send. This will also put an upper limit on the invoice amount. If the invoice is larger than this amount, it will be rejected.
  * `invoice_message` - Used specifically for invoice messages, when requesting an invoice from a foreign service, this comment will be included in the generated invoice and the receiver will see it.

### Convert to Hive/HBD

* Json containing the following fields:
  * `from_account` - the sending account, this needs to match the Active Authority on the transaction
  * `to_account` - the server's account (`@v4vapp` on the live system) corresponds to the server's Hive account.
  * `memo` : memo must not be a lightning invoice. if it has `#HBD` it will return HBD otherwise it will be Hive.
  * One of:
  * `sats` or `msats` [Optional]: the amount of sats/msats to be converted to Hive/HBD
  * `hive` or `hbd` [Optional]: the amount of Hive/HBD to be converted to sats/msats
  * `invoice_message` - Used specifically for invoice messages, when requesting an invoice from a foreign service, this comment will be included in the generated invoice and the receiver will see it.

### Transfer Keepsats

* ID: `v4vapp_transfer`

* Json containing the following fields:
  * `from_account` - the sending account, this needs to match the Active Authority on the transaction
  * `to_account` - the receiving account, this is the Hive account that will receive the sats
  * `sats` or `msats` - the amount of sats to transfer, this will be deducted from the sender's balance and added to the receiver's balance.
  * `memo` - an optional memo that can be included with the transfer, this can contain any additional information you want to pass along with the transfer.
