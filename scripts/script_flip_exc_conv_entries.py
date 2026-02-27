"""
Ad-hoc utility for correcting the direction of historical exchange conversion
ledger entries.  A bug in earlier versions of `exchange_accounting()` flipped the
credit/debit sides, which meant sales increased an asset instead of decreasing it.

This script scans the ledger collection for entries whose `ledger_type` is
`EXCHANGE_CONVERSION` and swaps the two sides.  It is intentionally as simple as
possible; the expectation is that you will review the code below and then run it
against a copy of the database or under supervision.

Usage
-----
    cd v4vapp-backend-v2
    uv run python scripts/flip_exc_conv_entries.py

Note that the script writes the modified documents back into Mongo with
`upsert=True`.  It does **not** perform any other validation or backups; take a
snapshot of your database first.
"""

import asyncio
from typing import Any

from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn


async def flip_entries() -> None:
    # ensure the singleton config is initialized so ``InternalConfig.db`` is set
    ic = InternalConfig(config_filename="devhive.config.yaml")
    db_conn = DBConn()
    await db_conn.setup_database()

    query: dict[str, Any] = {"ledger_type": LedgerType.EXCHANGE_CONVERSION.value}
    cursor = LedgerEntry.collection().find(query)
    total = 0
    async for raw in cursor:
        entry = LedgerEntry.model_validate(raw)

        # swap all of the fields that describe the two sides of the T-account
        entry.debit, entry.credit = entry.credit, entry.debit
        entry.debit_unit, entry.credit_unit = entry.credit_unit, entry.debit_unit
        entry.debit_amount, entry.credit_amount = entry.credit_amount, entry.debit_amount
        entry.debit_conv, entry.credit_conv = entry.credit_conv, entry.debit_conv

        # the signed getters compute their own value from the above fields,
        # so there is no need to touch ``debit_amount_signed`` etc.

        # save the modified document back to the database; ``upsert`` ensures
        # our change is applied even if another process has mutated the same
        # entry (very unlikely for this one-off script).
        await entry.save(upsert=True)
        total += 1

    print(f"flipped {total} EXCHANGE_CONVERSION entries")


if __name__ == "__main__":
    asyncio.run(flip_entries())
