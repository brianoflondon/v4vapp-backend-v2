import json

import httpx
import pytest

from v4vapp_backend_v2.dash.dashd.rpc import Dashd, DashdError, WalletDisabled


def _handler(request: httpx.Request) -> httpx.Response:
    payload = json.loads(request.read())
    method = payload["method"]
    path = request.url.path
    if method == "getblockchaininfo":
        assert path in ("", "/")
        return httpx.Response(
            200,
            json={
                "result": {"chain": "main", "blocks": 1, "initialblockdownload": True},
                "error": None,
            },
        )
    if method == "listunspent":
        assert path == "/wallet/watch"
        return httpx.Response(200, json={"result": [], "error": None})
    if method == "listwallets":
        return httpx.Response(
            200,
            json={"result": None, "error": {"code": -32601, "message": "Method not found"}},
        )
    return httpx.Response(200, json={"result": None, "error": {"code": -1, "message": method}})


@pytest.mark.asyncio
async def test_node_vs_wallet_paths() -> None:
    transport = httpx.MockTransport(_handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://dashd:9998")
    dashd = Dashd("http://dashd:9998", user="u", password="p", client=client)
    info = await dashd.getblockchaininfo()
    assert info["chain"] == "main"
    utxos = await dashd.listunspent(0, 9999999, ["Xabc"])
    assert utxos == []
    await dashd.aclose()


@pytest.mark.asyncio
async def test_wallet_disabled_mapping() -> None:
    transport = httpx.MockTransport(_handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://dashd:9998")
    dashd = Dashd("http://dashd:9998", user="u", password="p", client=client)
    with pytest.raises(WalletDisabled):
        await dashd.listwallets()
    await dashd.aclose()


@pytest.mark.asyncio
async def test_unauthorized() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="nope")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    dashd = Dashd("http://dashd:9998", user="u", password="p", client=client)
    with pytest.raises(DashdError):
        await dashd.getblockchaininfo()
    await dashd.aclose()
