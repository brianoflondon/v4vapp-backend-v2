from datetime import UTC, datetime
from typing import Any

import pytest
from bson import ObjectId

from tests.dash.test_hd import TESTNET_ADDRS, TESTNET_XPUB
from tests.dash.test_invoices import _Db, _EmptyDashd
from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_WALLET_STATE
from v4vapp_backend_v2.dash.dashd.rpc import DashdError
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.wallet.allocate import allocate_empty_receive

FP = "73c5da0a"


def _wallet_db() -> _Db:
    db = _Db()
    db[COL_WALLET_STATE].docs.append(
        {
            "_id": "testnet",
            "network": "testnet",
            "fingerprint": FP,
            "account_xpub": TESTNET_XPUB,
            "next_receive_index": 0,
            "next_change_index": 0,
            "descriptor_range_end": 100000,
            "updated_at": datetime.now(UTC),
        }
    )
    return db


async def _alloc(db: _Db, dashd: Any, *, range_end: int = 100000):
    return await allocate_empty_receive(
        db,
        dashd,
        network="testnet",  # type: ignore[arg-type]
        account_xpub=TESTNET_XPUB,
        range_end=range_end,
    )


@pytest.mark.asyncio
async def test_first_index_when_empty() -> None:
    index, address, der = await _alloc(_wallet_db(), _EmptyDashd())
    assert index == 0
    assert address == TESTNET_ADDRS[0]
    assert der.path == "m/44'/1'/0'/0/0"


@pytest.mark.asyncio
async def test_skips_index_with_utxo() -> None:
    dirty = TESTNET_ADDRS[0]
    dashd = _EmptyDashd(
        utxos_by_address={
            dirty: [{"txid": "aa" * 32, "vout": 0, "address": dirty, "amount": 0.1306}]
        }
    )
    index, address, der = await _alloc(_wallet_db(), dashd)
    assert index == 1
    assert address == TESTNET_ADDRS[1]
    assert der.index == 1
    assert dashd.listunspent_addrs == [[TESTNET_ADDRS[0]], [TESTNET_ADDRS[1]]]


@pytest.mark.asyncio
async def test_skips_address_already_on_an_invoice() -> None:
    db = _wallet_db()
    db[COL_INVOICES].docs.append(
        {
            "_id": ObjectId(),
            "address": TESTNET_ADDRS[0],
            "state": "SETTLED",
        }
    )
    index, address, _der = await _alloc(db, _EmptyDashd())
    assert index == 1
    assert address == TESTNET_ADDRS[1]


@pytest.mark.asyncio
async def test_skips_address_that_previously_received() -> None:
    dashd = _EmptyDashd(received_by_address={TESTNET_ADDRS[0]: 1.0})
    index, address, _der = await _alloc(_wallet_db(), dashd)
    assert index == 1
    assert address == TESTNET_ADDRS[1]


@pytest.mark.asyncio
async def test_listunspent_failure_is_unavailable() -> None:
    class _Down(_EmptyDashd):
        async def listunspent(self, *args: object, **kwargs: object) -> list[dict[str, Any]]:
            raise DashdError("dashd RPC listunspent ConnectError: down")

    with pytest.raises(ApiError) as exc:
        await _alloc(_wallet_db(), _Down())
    assert exc.value.status_code == 503
    assert exc.value.code == "wallet_unavailable"


@pytest.mark.asyncio
async def test_gives_up_after_max_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "v4vapp_backend_v2.dash.wallet.allocate.MAX_EMPTY_RECEIVE_SKIPS",
        2,
    )

    class _AllDirty(_EmptyDashd):
        async def listunspent(
            self,
            minconf: int = 0,
            maxconf: int = 9999999,
            addresses: list[str] | None = None,
            include_unsafe: bool = True,
        ) -> list[dict[str, Any]]:
            addr = (addresses or ["?"])[0]
            return [{"txid": "bb" * 32, "vout": 0, "address": addr, "amount": 1}]

    with pytest.raises(ApiError) as exc:
        await _alloc(_wallet_db(), _AllDirty())
    assert exc.value.status_code == 503
    assert exc.value.code == "index_exhausted"
    assert "skipping 2 indexes" in exc.value.message


@pytest.mark.asyncio
async def test_range_exhausted_before_empty() -> None:
    db = _wallet_db()
    db[COL_WALLET_STATE].docs[0]["next_receive_index"] = 5
    with pytest.raises(ApiError) as exc:
        await _alloc(db, _EmptyDashd(), range_end=5)
    assert exc.value.code == "index_exhausted"
