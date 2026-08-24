from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests.dash.test_hd import MASTER_FINGERPRINT, TEST_MNEMONIC, TESTNET_ADDRS, TESTNET_XPUB
from tests.dash.test_invoices import _Db
from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.collections import COL_INVOICES, COL_PAYOUTS, COL_WALLET_STATE
from v4vapp_backend_v2.dash.errors import register_dash_exception_handlers
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.routers import router as dash_router
from v4vapp_backend_v2.dash.wallet.hd import wif_from_mnemonic


class _FakeDashd:
    def __init__(self, utxos: list[dict[str, Any]]) -> None:
        self.utxos = utxos
        self.sent: list[str] = []
        self.locked: list[Any] = []

    async def listunspent(self, *args: object) -> list[dict[str, Any]]:
        return self.utxos

    async def getblockchaininfo(self) -> dict[str, Any]:
        return {
            "chain": "test",
            "blocks": 100,
            "headers": 100,
            "initialblockdownload": False,
            "pruned": False,
        }

    async def getwalletinfo(self) -> dict[str, Any]:
        return {"txcount": 3, "private_keys_enabled": False, "descriptors": True}

    async def validateaddress(self, address: str) -> dict[str, Any]:
        return {"isvalid": address.startswith("y"), "address": address}

    async def estimatesmartfee(self, _n: int = 1) -> dict[str, Any]:
        return {"errors": ["Insufficient data"]}

    async def lockunspent(self, unlock: bool, outputs: list[dict[str, Any]]) -> bool:
        self.locked.append((unlock, outputs))
        return True

    async def createrawtransaction(self, inputs: list, outputs: dict) -> str:
        assert inputs and outputs
        return "00" * 32

    async def signrawtransactionwithkey(self, raw_hex: str, keys: list[str]) -> dict[str, Any]:
        assert keys
        return {"hex": "11" * 32, "complete": True}

    async def sendrawtransaction(self, raw_hex: str) -> str:
        self.sent.append(raw_hex)
        return "b" * 64

    async def gettransaction(self, txid: str) -> dict[str, Any]:
        return {"txid": txid, "instantlock": True, "chainlock": False, "confirmations": 0}

    async def aclose(self) -> None:
        return None


def _conn(*, payouts: bool, mnemonic_file: str = "") -> DashConnectionConfig:
    return DashConnectionConfig(
        rpc_url="http://127.0.0.1:19998",
        rpc_user="u",
        rpc_password="p",
        network="testnet",
        xpub=TESTNET_XPUB,
        master_fingerprint=MASTER_FINGERPRINT,
        settle_policy="instantsend_or_chainlock",
        routing_fee_sats=300,
        payouts_enabled=payouts,
        mnemonic_file=mnemonic_file,
    )


@pytest.fixture
def wallet_client() -> tuple[TestClient, _Db, _FakeDashd]:
    db = _Db()
    db[COL_WALLET_STATE].docs.append(
        {
            "_id": "testnet",
            "network": "testnet",
            "fingerprint": MASTER_FINGERPRINT,
            "account_xpub": TESTNET_XPUB,
            "next_receive_index": 1,
            "next_change_index": 0,
            "descriptor_range_end": 100000,
            "updated_at": datetime.now(UTC),
        }
    )
    addr = TESTNET_ADDRS[0]
    db[COL_INVOICES].docs.append(
        {
            "address": addr,
            "state": DashInvoiceState.SETTLED.value,
            "derivation": {
                "account": 0,
                "change": 0,
                "index": 0,
                "path": "m/44'/1'/0'/0/0",
            },
        }
    )
    fake = _FakeDashd(
        [
            {
                "txid": "aa" * 32,
                "vout": 0,
                "address": addr,
                "amount": "0.50000000",
                "confirmations": 6,
                "instantlock": True,
                "chainlock": False,
            }
        ]
    )
    app = FastAPI()
    register_dash_exception_handlers(app)
    app.include_router(dash_router)
    app.state.dash_db = db
    app.state.dash_conn = _conn(payouts=False)
    app.state.dashd = fake
    return TestClient(app), db, fake


def test_wallet_balance_and_hd(wallet_client: tuple[TestClient, _Db, _FakeDashd]) -> None:
    client, _db, _fake = wallet_client
    res = client.get("/v2/dash/wallet")
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["network"] == "testnet"
    assert body["payouts_enabled"] is False
    assert body["can_sign"] is False
    assert body["balance"]["duffs_total"] == 50_000_000
    assert body["balance"]["duffs_spendable"] == 50_000_000
    assert body["hd"]["next_receive_index"] == 1
    assert body["utxos"] is None
    listed = client.get("/v2/dash/wallet?include_utxos=true")
    assert listed.json()["utxos"][0]["spendable"] is True


def test_wallet_hides_open_invoice_utxo(
    wallet_client: tuple[TestClient, _Db, _FakeDashd],
) -> None:
    client, db, _fake = wallet_client
    db[COL_INVOICES].docs[0]["state"] = DashInvoiceState.OPEN.value
    body = client.get("/v2/dash/wallet").json()
    assert body["balance"]["duffs_spendable"] == 0
    assert body["balance"]["duffs_incoming"] == 50_000_000


def test_payout_broadcast(
    wallet_client: tuple[TestClient, _Db, _FakeDashd], tmp_path: Path
) -> None:
    client, db, fake = wallet_client
    mfile = tmp_path / "seed.txt"
    mfile.write_text(TEST_MNEMONIC)
    client.app.state.dash_conn = _conn(payouts=True, mnemonic_file=str(mfile))  # type: ignore[union-attr]
    dest = TESTNET_ADDRS[1]
    res = client.post(
        "/v2/dash/payouts",
        json={"external_id": "payout:1", "address": dest, "duffs": 1_000_000},
    )
    assert res.status_code == 201, res.text
    body = res.json()
    assert body["state"] == "BROADCAST"
    assert body["txid"] == "b" * 64
    assert body["address"] == dest
    assert fake.sent
    stored = db[COL_PAYOUTS].docs[0]
    assert stored["external_id"] == "payout:1"

    again = client.post(
        "/v2/dash/payouts",
        json={"external_id": "payout:1", "address": dest, "duffs": 1_000_000},
    )
    assert again.status_code == 200
    got = client.get(f"/v2/dash/payouts/{body['payout_id']}")
    assert got.status_code == 200
    by_ext = client.get("/v2/dash/payouts/by-external/payout:1")
    assert by_ext.json()["txid"] == body["txid"]


def test_payout_rejects_both_amounts(
    wallet_client: tuple[TestClient, _Db, _FakeDashd],
) -> None:
    client, _db, _fake = wallet_client
    res = client.post(
        "/v2/dash/payouts",
        json={
            "external_id": "payout:bad",
            "address": TESTNET_ADDRS[1],
            "duffs": 1,
            "sats": 1,
        },
    )
    assert res.status_code == 422


def test_wif_roundtrip_abandon_mnemonic() -> None:
    wif = wif_from_mnemonic(TEST_MNEMONIC, "testnet", 0, change=0)
    assert wif.startswith(("c", "9"))


def test_load_mnemonic_skips_comments(tmp_path: Path) -> None:
    from v4vapp_backend_v2.dash.keys import load_mnemonic

    path = tmp_path / "seed.mnemonic"
    path.write_text("# testnet only\n\n" + TEST_MNEMONIC + "\n")
    conn = _conn(payouts=True, mnemonic_file=str(path))
    assert load_mnemonic(conn) == TEST_MNEMONIC


def test_check_dash_spend_keys_match(tmp_path: Path) -> None:
    from v4vapp_backend_v2.dash.keys import check_dash_spend_keys

    path = tmp_path / "seed.mnemonic"
    path.write_text(TEST_MNEMONIC)
    conn = _conn(payouts=True, mnemonic_file=str(path))
    check = check_dash_spend_keys(conn)
    assert check.can_sign is True
    assert check.fingerprint == MASTER_FINGERPRINT
    assert check.problem is None


def test_check_dash_spend_keys_mismatch(tmp_path: Path) -> None:
    from v4vapp_backend_v2.dash.keys import check_dash_spend_keys

    path = tmp_path / "seed.mnemonic"
    path.write_text(
        "legal winner thank year wave sausage worth useful legal winner thank yellow"
    )
    conn = _conn(payouts=True, mnemonic_file=str(path))
    check = check_dash_spend_keys(conn)
    assert check.can_sign is False
    assert check.problem is not None
