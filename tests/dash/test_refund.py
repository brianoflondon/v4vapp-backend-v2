from decimal import Decimal

import pytest

from v4vapp_backend_v2.dash.amounts import duffs_from_sats
from v4vapp_backend_v2.dash.refund import (
    REFUND_FEE_SATS,
    first_funding_txid,
    funding_sender_address,
    refund_duffs,
    refund_payout_external_id,
)


class _FakeDashd:
    def __init__(self, txs: dict[str, dict]) -> None:
        self.txs = txs

    async def getrawtransaction(self, txid: str, verbose: bool = True) -> dict:
        return self.txs[txid]


def _invoice() -> dict:
    return {
        "_id": "abc",
        "address": "yOurInvoiceAddr",
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
async def test_funding_sender_address_uses_largest_vin() -> None:
    dashd = _FakeDashd(
        {
            "paytx": {
                "vin": [
                    {"txid": "prev_small", "vout": 0},
                    {"txid": "prev_big", "vout": 1},
                ]
            },
            "prev_small": {
                "vout": [
                    {
                        "value": 0.1,
                        "scriptPubKey": {"address": "ySmallPayer"},
                    }
                ]
            },
            "prev_big": {
                "vout": [
                    {"value": 0.01, "scriptPubKey": {"addresses": ["yIgnore"]}},
                    {
                        "value": 0.9,
                        "scriptPubKey": {"address": "yBigPayer"},
                    },
                ]
            },
        }
    )
    addr = await funding_sender_address(dashd, txid="paytx", our_address="yOurInvoiceAddr")
    assert addr == "yBigPayer"


@pytest.mark.asyncio
async def test_funding_sender_skips_our_invoice_address() -> None:
    dashd = _FakeDashd(
        {
            "paytx": {"vin": [{"txid": "prev", "vout": 0}]},
            "prev": {
                "vout": [
                    {
                        "value": 1,
                        "scriptPubKey": {"address": "yOurInvoiceAddr"},
                    }
                ]
            },
        }
    )
    assert await funding_sender_address(dashd, txid="paytx", our_address="yOurInvoiceAddr") is None
