
# Using Custom Json on Hive to Interact with *Keepsats*

## Background

There are two main ways to deposit *Keepsats* on the v4v.app system:

1. Send a Hive or HBD Transfer to the `server` Hive Account with `#sats` anywhere in the memo.
2. Receive Lightning on a Lightning address on the system eg `brianoflondon@sats.v4v.app`



## Paying a Lightning Invoice with *Keepsats*

### Using the Web

The web based app on [v4v.app](https://v4v.app) allows you to spend *Keepsats* directly.

### Custom Json

To initiate a payment with a `custom_json` you need to create a Hive `custom_json` with the following characteristics.

* ID: `v4vapp_transfer`
* Json containing the following fields:
* `from_account` - the sending account
* `memo` - either a lightning invoice or a lightning address

If the Lightning
