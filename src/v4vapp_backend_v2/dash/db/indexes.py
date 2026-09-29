from typing import Any

from pymongo import ASCENDING, IndexModel
from pymongo.asynchronous.database import AsyncDatabase

from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_PAYOUTS, COL_WALLET_STATE

# Idempotent create_index names. No TTL — Dash addresses can receive late funds.
INVOICE_INDEXES = [
    IndexModel([("external_id", ASCENDING)], name="external_id", unique=True),
    IndexModel([("address", ASCENDING)], name="address", unique=True),
    IndexModel([("state", ASCENDING), ("expires_at", ASCENDING)], name="state_expires"),
    IndexModel([("cust_id", ASCENDING)], name="cust_id"),
    IndexModel(
        [("cust_id", ASCENDING), ("state", ASCENDING), ("settled_at", ASCENDING)],
        name="cust_state_settled",
    ),
    IndexModel([("created_at", ASCENDING)], name="created_at"),
    IndexModel(
        [("lightning_invoice", ASCENDING)],
        name="lightning_invoice",
        unique=True,
        sparse=True,
    ),
]

WALLET_STATE_INDEXES: list[IndexModel] = []

PAYOUT_INDEXES = [
    IndexModel([("external_id", ASCENDING)], name="external_id", unique=True),
    IndexModel([("txid", ASCENDING)], name="txid", unique=True, sparse=True),
    IndexModel([("state", ASCENDING)], name="state"),
]

COLLECTION_INDEXES: dict[str, list[IndexModel]] = {
    COL_INVOICES: INVOICE_INDEXES,
    COL_WALLET_STATE: WALLET_STATE_INDEXES,
    COL_PAYOUTS: PAYOUT_INDEXES,
}


async def ensure_indexes(db: AsyncDatabase[dict[str, Any]]) -> dict[str, list[str]]:
    created: dict[str, list[str]] = {}
    for name, models in COLLECTION_INDEXES.items():
        coll = db[name]
        if models:
            created[name] = await coll.create_indexes(models)
        else:
            existing = await db.list_collection_names()
            if name not in existing:
                await db.create_collection(name)
            created[name] = []
    return created
