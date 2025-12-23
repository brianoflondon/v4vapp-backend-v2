from decimal import Decimal

import pytest

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.models.lnd_balance_models import (
    ChannelBalance,
    WalletBalance,
    fetch_balances_from_default,
    protobuf_channel_to_pydantic,
    protobuf_wallet_to_pydantic,
)


def test_wallet_balance_conversion_simple():
    pkt = lnrpc.WalletBalanceResponse(
        total_balance=12345,
        confirmed_balance=12000,
        unconfirmed_balance=345,
        locked_balance=0,
        reserved_balance_anchor_chan=50,
    )
    # attach an account map
    pkt.account_balance["main"].confirmed_balance = 12000
    pkt.account_balance["main"].unconfirmed_balance = 345

    model = protobuf_wallet_to_pydantic(pkt)

    assert isinstance(model, WalletBalance)
    assert model.total_balance == Decimal(12345)
    assert model.account_balance["main"].confirmed_balance == Decimal(12000)
    assert model.total_sats == Decimal(12345)


def test_channel_balance_conversion_with_amounts():
    local = lnrpc.Amount(sat=5000, msat=5000000)
    remote = lnrpc.Amount(sat=3000, msat=3000000)
    pkt = lnrpc.ChannelBalanceResponse(
        balance=8000,
        pending_open_balance=0,
        local_balance=local,
        remote_balance=remote,
    )

    model = protobuf_channel_to_pydantic(pkt)
    assert isinstance(model, ChannelBalance)
    assert model.local_balance.sat == Decimal(5000)
    assert model.remote_balance.msat == Decimal(3000000)
    assert model.balance == Decimal(8000)
    assert model.local_sats == Decimal(5000)
    assert model.remote_msat == Decimal(3000000)


@pytest.mark.asyncio
async def test_fetch_balances_no_default(monkeypatch):
    # Ensure no default node configured
    cfg = InternalConfig().config.lnd_config
    monkeypatch.setattr(cfg, "default", "")

    wallet, chan = await fetch_balances_from_default()
    assert wallet is None and chan is None
