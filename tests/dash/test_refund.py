from decimal import Decimal

import pytest

from v4vapp_backend_v2.dash.amounts import duffs_from_sats
from v4vapp_backend_v2.dash.dashd.rpc import DashdError
from v4vapp_backend_v2.dash.refund import (
    REFUND_FEE_SATS,
    first_funding_txid,
    funding_sender_address,
    refund_duffs,
    refund_payout_external_id,
)

OUR = "yOurInvoiceAddr"
CHANGE = "yjXVUzCj8b6HuHtYhkhyPbCXGzcahfHptb"
# scriptSig pubkey from inbound 5e47794d… (testnet P2PKH)
LIVE_PUBKEY = "038ff9a3005721cc9136dde514e7d960ef3e1ac379d8f2c9709d8e6f19c22734e1"
LIVE_INPUT_ADDR = "ySyQo85gphdTuDb1zZoa3KSCxx5nbh35NM"


class _FakeDashd:
    def __init__(
        self,
        txs: dict[str, dict] | None = None,
        *,
        wallet: dict[str, dict] | None = None,
        decoded: dict[str, dict] | None = None,
    ) -> None:
        self.txs = txs or {}
        self.wallet = wallet or {}
        self.decoded = decoded or {}
        self.raw_calls: list[tuple[str, bool, str | None]] = []

    async def gettransaction(self, txid: str) -> dict:
        if txid in self.wallet:
            return self.wallet[txid]
        raise DashdError(f"Invalid or non-wallet transaction id {txid}")

    async def decoderawtransaction(self, raw_hex: str) -> dict:
        if raw_hex in self.decoded:
            return self.decoded[raw_hex]
        raise DashdError("decoderawtransaction failed")

    async def getrawtransaction(
        self, txid: str, verbose: bool = True, blockhash: str | None = None
    ) -> dict:
        self.raw_calls.append((txid, verbose, blockhash))
        if txid not in self.txs:
            raise DashdError(
                "No such mempool transaction. Use -txindex or provide a block hash"
            )
        return self.txs[txid]


def _invoice() -> dict:
    return {
        "_id": "abc",
        "address": OUR,
        "duffs_received": Decimal(80_000_000),
        "quote": {
            "btc_usd": "83000",
            "dash_usd": "32.5",
            "dash_btc": "0.00039157",
        },
        "txids": [
            {"txid": "paytx", "vout": 0, "duffs": 80_000_000},
        ],
    }


def _p2pkh_vout(address: str, value: float = 0.04) -> dict:
    return {
        "value": value,
        "scriptPubKey": {"address": address, "type": "pubkeyhash"},
    }


def test_refund_duffs_is_received_minus_100_sats() -> None:
    doc = _invoice()
    fee = duffs_from_sats(REFUND_FEE_SATS, Decimal("0.00039157"))
    assert refund_duffs(doc) == Decimal(80_000_000) - fee
    assert refund_duffs(doc) < Decimal(80_000_000)


def test_refund_payout_external_id() -> None:
    assert refund_payout_external_id("6a95ab7c") == "dash-refund:6a95ab7c"


def test_first_funding_txid_picks_largest() -> None:
    doc = {
        "txids": [
            {"txid": "small", "duffs": 100},
            {"txid": "big", "duffs": 900},
        ]
    }
    assert first_funding_txid(doc) == "big"


@pytest.mark.asyncio
async def test_funding_sender_prefers_unique_change_from_wallet_hex() -> None:
    decoded = {
        "vin": [
            {
                "txid": "prev",
                "vout": 1,
                "scriptSig": {"asm": f"30aa[ALL] {LIVE_PUBKEY}"},
            }
        ],
        "vout": [_p2pkh_vout(OUR, 0.04475383), _p2pkh_vout(CHANGE, 0.80602753)],
    }
    dashd = _FakeDashd(
        wallet={"paytx": {"hex": "aabb", "blockhash": "00" * 32}},
        decoded={"aabb": decoded},
    )
    addr = await funding_sender_address(dashd, txid="paytx", our_address=OUR)
    assert addr == CHANGE
    assert dashd.raw_calls == []


@pytest.mark.asyncio
async def test_funding_sender_p2pkh_scriptsig_when_no_change() -> None:
    decoded = {
        "vin": [{"scriptSig": {"asm": f"30aa[ALL] {LIVE_PUBKEY}"}}],
        "vout": [_p2pkh_vout(OUR)],
    }
    dashd = _FakeDashd(
        wallet={"paytx": {"hex": "ccdd"}},
        decoded={"ccdd": decoded},
    )
    addr = await funding_sender_address(dashd, txid="paytx", our_address=OUR)
    assert addr == LIVE_INPUT_ADDR
    assert dashd.raw_calls == []


@pytest.mark.asyncio
async def test_funding_sender_skips_change_when_multiple_other_outputs() -> None:
    decoded = {
        "vin": [{"scriptSig": {"asm": f"30aa[ALL] {LIVE_PUBKEY}"}}],
        "vout": [
            _p2pkh_vout(OUR),
            _p2pkh_vout("yOtherOne", 0.1),
            _p2pkh_vout("yOtherTwo", 0.2),
        ],
    }
    dashd = _FakeDashd(
        wallet={"paytx": {"hex": "eeff"}},
        decoded={"eeff": decoded},
    )
    # Not unique change; fall through to the single P2PKH scriptSig.
    assert await funding_sender_address(dashd, txid="paytx", our_address=OUR) == LIVE_INPUT_ADDR


@pytest.mark.asyncio
async def test_funding_sender_unique_prevout_last_resort() -> None:
    dashd = _FakeDashd(
        {
            "paytx": {"vin": [{"txid": "prev", "vout": 1}], "vout": [_p2pkh_vout(OUR)]},
            "prev": {
                "vout": [
                    {"value": 0.01, "scriptPubKey": {"addresses": ["yIgnore"]}},
                    {"value": 0.9, "scriptPubKey": {"address": "yBigPayer"}},
                ]
            },
        }
    )
    addr = await funding_sender_address(dashd, txid="paytx", our_address=OUR)
    assert addr == "yBigPayer"


@pytest.mark.asyncio
async def test_funding_sender_skips_mixed_prevouts() -> None:
    dashd = _FakeDashd(
        {
            "paytx": {
                "vin": [
                    {"txid": "prev_small", "vout": 0},
                    {"txid": "prev_big", "vout": 1},
                ],
                "vout": [_p2pkh_vout(OUR)],
            },
            "prev_small": {
                "vout": [{"value": 0.1, "scriptPubKey": {"address": "ySmallPayer"}}]
            },
            "prev_big": {
                "vout": [
                    {"value": 0.01, "scriptPubKey": {"addresses": ["yIgnore"]}},
                    {"value": 0.9, "scriptPubKey": {"address": "yBigPayer"}},
                ]
            },
        }
    )
    assert await funding_sender_address(dashd, txid="paytx", our_address=OUR) is None


@pytest.mark.asyncio
async def test_funding_sender_skips_our_invoice_address() -> None:
    dashd = _FakeDashd(
        {
            "paytx": {"vin": [{"txid": "prev", "vout": 0}], "vout": [_p2pkh_vout(OUR)]},
            "prev": {
                "vout": [{"value": 1, "scriptPubKey": {"address": OUR}}],
            },
        }
    )
    assert await funding_sender_address(dashd, txid="paytx", our_address=OUR) is None


@pytest.mark.asyncio
async def test_funding_sender_logs_when_tx_cannot_load() -> None:
    dashd = _FakeDashd()
    assert await funding_sender_address(dashd, txid="missing", our_address=OUR) is None
