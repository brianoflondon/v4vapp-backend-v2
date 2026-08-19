from typing import Any

import pytest

from v4vapp_backend_v2.dash.collections import COL_WALLET_STATE
from v4vapp_backend_v2.dash.db.wallet_state import (
    WalletStateMismatch,
    allocate_receive_index,
    ensure_wallet_state,
)

XPUB = "xpub-test-not-real"
FP = "73c5da0a"


class _Coll:
    def __init__(self) -> None:
        self.docs: dict[str, dict[str, Any]] = {}

    async def find_one(self, query: dict[str, Any]) -> dict[str, Any] | None:
        return self.docs.get(query["_id"])

    async def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs[doc["_id"]] = doc

    async def find_one_and_update(
        self,
        query: dict[str, Any],
        update: dict[str, Any],
        return_document: Any = None,
    ) -> dict[str, Any] | None:
        doc = self.docs.get(query["_id"])
        if doc is None:
            return None
        before = dict(doc)
        for key, value in update.get("$inc", {}).items():
            doc[key] = int(doc.get(key, 0)) + int(value)
        doc.update(update.get("$set", {}))
        return before


class _Db:
    def __init__(self) -> None:
        self.coll = _Coll()

    def __getitem__(self, name: str) -> _Coll:
        assert name == COL_WALLET_STATE
        return self.coll


@pytest.mark.asyncio
async def test_inserts_on_first_boot() -> None:
    db = _Db()
    doc = await ensure_wallet_state(
        db,  # type: ignore[arg-type]
        network="regtest",
        account_xpub=XPUB,
        fingerprint=FP,
        descriptor_range_end=100000,
    )
    assert doc["next_receive_index"] == 0
    assert doc["_id"] == "regtest"
    again = await ensure_wallet_state(
        db,  # type: ignore[arg-type]
        network="regtest",
        account_xpub=XPUB,
        fingerprint=FP.upper(),
        descriptor_range_end=100000,
    )
    assert again["account_xpub"] == XPUB


@pytest.mark.asyncio
async def test_mismatch_aborts() -> None:
    db = _Db()
    await ensure_wallet_state(
        db,  # type: ignore[arg-type]
        network="regtest",
        account_xpub=XPUB,
        fingerprint=FP,
        descriptor_range_end=100000,
    )
    with pytest.raises(WalletStateMismatch):
        await ensure_wallet_state(
            db,  # type: ignore[arg-type]
            network="regtest",
            account_xpub="xpub-OTHER",
            fingerprint=FP,
            descriptor_range_end=100000,
        )


@pytest.mark.asyncio
async def test_allocate_increments() -> None:
    db = _Db()
    await ensure_wallet_state(
        db,  # type: ignore[arg-type]
        network="mainnet",
        account_xpub=XPUB,
        fingerprint=FP,
        descriptor_range_end=100000,
    )
    first = await allocate_receive_index(db, "mainnet")  # type: ignore[arg-type]
    second = await allocate_receive_index(db, "mainnet")  # type: ignore[arg-type]
    assert first == 0
    assert second == 1
    assert db.coll.docs["mainnet"]["next_receive_index"] == 2
