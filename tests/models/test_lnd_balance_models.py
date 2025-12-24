import asyncio
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

from v4vapp_backend_v2.models.lnd_balance_models import (
    ChannelBalance,
    NodeBalances,
    WalletBalance,
    fetch_balances,
)


class FakeClient:
    def __init__(self, delay=0.1):
        self._delay = delay
        self.lightning_stub = SimpleNamespace(WalletBalance="wallet", ChannelBalance="channel")
        self.entered = False
        self.exit_called = False

    async def call(self, stub, req):
        # simulate remote call
        await asyncio.sleep(self._delay)
        return {"stub": stub}

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exit_called = True


@pytest.mark.asyncio
async def test_fetch_balances_uses_external_client_and_runs_concurrently(monkeypatch):
    fake = FakeClient(delay=0.15)

    # stub the protobuf->pydantic converters to return simple models so we don't need proto messages
    async def fake_wallet(resp):
        return WalletBalance(total_balance=Decimal(100))

    async def fake_chan(resp):
        return ChannelBalance(
            balance=Decimal(50),
            local_balance=SimpleNamespace(sat=Decimal(30), msat=Decimal(30000)),
            remote_balance=SimpleNamespace(sat=Decimal(20), msat=Decimal(20000)),
        )

    # monkeypatch the converter functions (they are synchronous in module) to simple callables
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_wallet_to_pydantic",
        lambda resp: WalletBalance(total_balance=Decimal(100)),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_channel_to_pydantic",
        lambda resp: ChannelBalance(balance=Decimal(50)),
    )

    start = time.monotonic()
    nb = await fetch_balances(node="testnode", lnd_client=fake)
    duration = time.monotonic() - start

    # if calls ran concurrently, duration should be close to single delay, not sum
    # allow some scheduling overhead; concurrent behavior should be noticeably less than sequential sum
    assert duration < 0.5, f"Expected concurrent calls, took {duration}s"
    assert isinstance(nb, NodeBalances)
    assert nb.node == "testnode"
    assert nb.wallet.total_balance == Decimal(100)
    assert nb.channel.balance == Decimal(50)
    # ensure external client was not closed
    assert fake.exit_called is False


@pytest.mark.asyncio
async def test_fetch_balances_closes_internal_client(monkeypatch):
    # Replace LNDClient with a factory that returns our FakeClient instance
    created = FakeClient(delay=0.01)

    def fake_factory(node):
        return created

    monkeypatch.setattr("v4vapp_backend_v2.models.lnd_balance_models.LNDClient", fake_factory)
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_wallet_to_pydantic",
        lambda resp: WalletBalance(total_balance=Decimal(1)),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_channel_to_pydantic",
        lambda resp: ChannelBalance(balance=Decimal(1)),
    )

    nb = await fetch_balances(node="testnode", lnd_client=None)
    assert isinstance(nb, NodeBalances)
    # since we used internal client, its __aexit__ should have been called
    assert created.exit_called is True


@pytest.mark.asyncio
async def test_fetch_balances_propagates_error(monkeypatch):
    class BadClient(FakeClient):
        async def call(self, stub, req):
            await asyncio.sleep(0)
            raise RuntimeError("rpc failure")

    bad = BadClient()
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_wallet_to_pydantic",
        lambda resp: WalletBalance(total_balance=Decimal(1)),
    )
    monkeypatch.setattr(
        "v4vapp_backend_v2.models.lnd_balance_models.protobuf_channel_to_pydantic",
        lambda resp: ChannelBalance(balance=Decimal(1)),
    )

    with pytest.raises(ValueError):
        await fetch_balances(node="testnode", lnd_client=bad)
