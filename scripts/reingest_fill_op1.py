"""One-off repair for the dropped puncakbukit fill on trx 74fa4174.

Already applied on 8 Nov 2026 at 06:36:09 UTC from this machine, against
production database ``v4vapp-backend``, while production db-monitor was
still 2.17.1. Running it again should raise ``LedgerEntryDuplicateException``
and write nothing. Answer ``N`` at the prompt unless you intend a replay.

What broke
----------
Backend 2.17.1 booked the HBD to HIVE rebalance in block 111471342 out of
``op_in_trx`` order. Hive monitor was at the head and logged all seven
``fill_order`` virtual ops between 02:21:53.080 and 02:21:53.258 UTC. The
db-monitor then handled each hive_ops insert in its own task. Op 2 took the
``v4vapp`` customer lock first. ``find_existing_hive_ledger_entry`` treated
"a higher op_in_trx is already booked" as proof that an earlier op was a
duplicate, and dropped op 1 forever. The warning was:

    virtual already has later ledger op(s) [2] booked, but incoming
    op_in_trx is 1 … Dropping: this transaction was already processed
    past this operation.

That check is the wrong duplicate key for a market fill. A fill is
bookkeeping for a match that already happened on Hive. It does not send a
Hive transfer or a Lightning payment. The duplicate key has to be the fill
itself: the same slot, or the same order ids, owners, and amount/NAI already
stored under another index of this block, transaction, and realm. A later
index only proves that a different fill arrived first. Transfers and other
Hive ops that can cause a payout still keep the later-index stop.

The code change is on branch ``fix/virtual-op-out-of-order-drop``, in
``process_hive.find_existing_hive_ledger_entry``. Tests are
``tests/process/test_hive_ledger_replay.py``. Deploy that db-monitor before
relying on the new gate. This script was the manual repair and does not
depend on the new monitor being live. It imports ``process_hive_op`` from
whatever tree you run.

The exchange
------------
Chain time 8 Nov 2026 02:21:45 UTC (04:21:45 Asia/Jerusalem).
Account v4vapp sold 39.695 HBD for 905.122 HIVE.
Limit order 2548986856, created in the same transaction as real op 1.
Hive trx 74fa4174a4bd8f69d4a5af41dadc62962722dc75.
Short-id prefix 1342_74fa41. Realm is not part of the short id, so real op 1
and virtual op 1 both display as 1342_74fa41_1. The group_id keeps the realm.

    op  counterparty   HBD      HIVE     ledger before this repair
    1   puncakbukit     0.335    7.665    dropped
    2   honeybot        0.585   13.370    fill_or_n booked
    3   quicktrades     1.090   24.899    fill_or_n booked
    4   econo           0.500   11.417    fill_or_n booked
    5   droida          1.074   24.517    fill_or_n booked
    6   dtake           0.936   21.340    fill_or_n booked
    7   konvik-hbd     35.175  801.914    fill_or_n booked

"Remaining HIVE" in those log lines is ``min_to_receive`` (896.114 HIVE)
minus only the current fill. It is not a stored running total, and it is
not what this repair fixes. The order was fully filled: all 39.695 HBD was
consumed. Op 7 logging "Remaining 94.200 HIVE" is 896.114 - 801.914.

Only virtual op 1 was missing. Ops 2-7 were left in place. Deleting them
and replaying the whole transaction was the riskier path.

The customer payment that triggered the rebalance was already correct and
must not be rewritten: jthomasewsky, 31.264 HBD, parent 1322_05f756_1,
Lightning payout 39,162 sats plus a 52-sat fee. Nothing in this script
selects that transaction.

Why the first repair attempt did nothing
-----------------------------------------
An earlier version of this script unset ``process_time`` on the virtual
hive_ops row, then called ``process_tracked_event``. Two things went wrong.

``process_tracked_event`` reloads the hive_op by group_id and returns
immediately when ``process_time`` is set. Clearing it on the in-memory dict
does not matter.

Clearing it in Mongo woke production db-monitor. ``$unset`` arrives as
``removedFields``. ``ignore_changes`` only looks at ``updatedFields``, and
an empty updated-field set is processed. At 06:23:59 UTC the old monitor
loaded ``…_1_virtual``, hit the later-index rule, dropped op 1 again, logged
a success-style "checkmark 0.63 s" line, and wrote ``process_time`` back.
The local script then reloaded that timestamp and exited. The ledger stayed
at 6 net rows: 39.360 HBD and 897.457 HIVE.

This version never writes ``hive_ops``. ``process_hive_op`` does not look
at ``process_time``. Production db-monitor can stay up. Do not stop it for
this script, and do not unset ``process_time`` while the old monitor is
running.

What a successful run writes
----------------------------
One new ledger row:

    111471342_74fa4174a4bd8f69d4a5af41dadc62962722dc75_1_virtual_fill_or_n
    ledger_type fill_or_n, debit 7.665 HIVE, credit 0.335 HBD, v4vapp

And an in-place ``reversed`` timestamp on the existing escrow row, not a
second document and not a chain transaction:

    111471342_74fa4174a4bd8f69d4a5af41dadc62962722dc75_1_real
    ledger_type limit_or, 39.695 HBD, Customer Deposits Hive to Escrow Hive

The reversal runs because virtual op 1 shares short id 1342_74fa41_1 with
the real ``limit_order_create``. The lookup is by that short id. Ops 2-7
cannot find the escrow row, which is why it was still open after they were
booked. The log says "for order 1794096544". That number is puncakbukit's
open order id. The document updated is still the v4vapp limit order above,
order 2548986856.

After the run the seven ``fill_or_n`` rows sum to 39.695 HBD and 905.122
HIVE, and the main-site sanity check cleared. Confirmed 8 Nov 2026 06:36:09 UTC.

How to run
----------
From this repo, on the fix branch, with the production-from-home config:

    .venv/bin/python scripts/reingest_fill_op1.py

The only prompt is ``Book this fill only? [y/N]``. Answer ``y`` only when
the preview is virtual op 1, open owner puncakbukit, 335 HBD-units and
7665 HIVE-units (0.335 HBD and 7.665 HIVE). Any other line, answer ``N``.

The script reads Hive open orders. It does not broadcast. It does not touch
payments, invoices, or short id 1322_05f756.
"""

import asyncio

from v4vapp_backend_v2.actions.tracked_any import tracked_any_filter
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.process.process_hive import process_hive_op

TRX = "74fa4174a4bd8f69d4a5af41dadc62962722dc75"
BLOCK = 111471342
NET_GROUP_ID = f"{BLOCK}_{TRX}_1_virtual_fill_or_n"
CREATE_GROUP_ID = f"{BLOCK}_{TRX}_1_real"


async def main() -> None:
    InternalConfig(config_filename="production.fromhome.config.yaml")
    await DBConn().setup_database()
    db = InternalConfig.db
    doc = await db["hive_ops"].find_one(
        {"block_num": BLOCK, "trx_id": TRX, "op_in_trx": 1, "realm": "virtual"}
    )
    if doc is None:
        raise SystemExit("virtual op 1 not found")
    print(
        doc.get("group_id"),
        doc.get("open_owner"),
        doc.get("current_pays"),
        doc.get("open_pays"),
    )
    if input("Book this fill only? [y/N] ").strip() != "y":
        return

    # Book the fill only. Do not unset process_time and do not call
    # process_tracked_event: both wake the old db-monitor or no-op on it.
    entries = await process_hive_op(tracked_any_filter(doc))
    for entry in entries:
        print(entry.group_id, entry.ledger_type, entry.debit_amount, entry.debit_unit)

    net = await db["ledger"].find_one({"group_id": NET_GROUP_ID}, {"group_id": 1, "ledger_type": 1})
    create = await db["ledger"].find_one(
        {"group_id": CREATE_GROUP_ID},
        {"group_id": 1, "reversed": 1, "ledger_type": 1},
    )
    print("net row:", net)
    print("limit order row:", create)


if __name__ == "__main__":
    asyncio.run(main())
