from __future__ import annotations

from typing import Any

import httpx

from v4vapp_backend_v2.dash.amounts import rpc_dash_to_duffs

__all__ = ["Dashd", "DashdError", "WalletDisabled", "rpc_dash_to_duffs"]


class DashdError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class WalletDisabled(DashdError):
    """dashd was started with -disablewallet=1."""


def _wallet_url(node_url: str, wallet: str) -> str:
    return f"{node_url.rstrip('/')}/wallet/{wallet}"


class Dashd:
    """JSON-RPC client. Node methods hit rpc_url; wallet methods hit /wallet/<name>."""

    def __init__(
        self,
        url: str,
        *,
        user: str,
        password: str,
        wallet: str = "watch",
        timeout: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.node_url = url.rstrip("/")
        self.wallet_name = wallet
        self.wallet_url = _wallet_url(self.node_url, wallet)
        self._auth = (user, password)
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def _call(self, url: str, method: str, params: list[Any]) -> Any:
        payload = {
            "jsonrpc": "1.0",
            "id": "v4vapp-backend-v2",
            "method": method,
            "params": params,
        }
        try:
            response = await self._client.post(url, json=payload, auth=self._auth)
        except httpx.TransportError as exc:
            raise DashdError(f"dashd RPC {method} {type(exc).__name__}: {exc}") from exc
        if response.status_code == 401:
            raise DashdError("dashd RPC unauthorized", code=401)
        try:
            body = response.json()
        except ValueError as exc:
            raise DashdError(f"dashd returned non-JSON ({response.status_code})") from exc
        error = body.get("error")
        if error:
            message = error.get("message", str(error)) if isinstance(error, dict) else str(error)
            code = error.get("code") if isinstance(error, dict) else None
            lowered = message.lower()
            wallet_methods = {
                "listwallets",
                "createwallet",
                "getwalletinfo",
                "importdescriptors",
                "listdescriptors",
                "listunspent",
                "getreceivedbyaddress",
                "gettransaction",
                "lockunspent",
            }
            if method in wallet_methods and (
                code == -32601
                or "method not found" in lowered
                or "disablewallet" in lowered
                or ("wallet" in lowered and "disable" in lowered)
            ):
                raise WalletDisabled(message, code=code)
            raise DashdError(message, code=code)
        response.raise_for_status()
        return body.get("result")

    async def node(self, method: str, *params: Any) -> Any:
        return await self._call(self.node_url, method, list(params))

    async def wallet(self, method: str, *params: Any) -> Any:
        return await self._call(self.wallet_url, method, list(params))

    async def getblockchaininfo(self) -> dict[str, Any]:
        result = await self.node("getblockchaininfo")
        if not isinstance(result, dict):
            raise DashdError("getblockchaininfo returned a non-object")
        return result

    async def listwallets(self) -> list[str]:
        result = await self.node("listwallets")
        if not isinstance(result, list):
            raise DashdError("listwallets returned a non-list")
        return [str(name) for name in result]

    async def create_watch_wallet(self) -> Any:
        return await self.node(
            "createwallet",
            self.wallet_name,
            True,  # disable_private_keys
            True,  # blank
            "",  # passphrase
            False,  # avoid_reuse
            True,  # descriptors
            True,  # load_on_startup
        )

    async def getdescriptorinfo(self, desc: str) -> dict[str, Any]:
        result = await self.node("getdescriptorinfo", desc)
        if not isinstance(result, dict):
            raise DashdError("getdescriptorinfo returned a non-object")
        return result

    async def importdescriptors(self, requests: list[dict[str, Any]]) -> Any:
        return await self.wallet("importdescriptors", requests)

    async def listdescriptors(self) -> dict[str, Any]:
        result = await self.wallet("listdescriptors")
        if not isinstance(result, dict):
            raise DashdError("listdescriptors returned a non-object")
        return result

    async def getwalletinfo(self) -> dict[str, Any]:
        result = await self.wallet("getwalletinfo")
        if not isinstance(result, dict):
            raise DashdError("getwalletinfo returned a non-object")
        return result

    async def listunspent(
        self,
        minconf: int = 0,
        maxconf: int = 9999999,
        addresses: list[str] | None = None,
        include_unsafe: bool = True,
    ) -> list[dict[str, Any]]:
        result = await self.wallet(
            "listunspent", minconf, maxconf, addresses or [], include_unsafe
        )
        if not isinstance(result, list):
            raise DashdError("listunspent returned a non-list")
        return result

    async def getreceivedbyaddress(self, address: str, minconf: int = 0) -> Any:
        """Total DASH ever received on `address` (spent or unspent). Wallet must know it."""
        return await self.wallet("getreceivedbyaddress", address, minconf)

    async def gettransaction(self, txid: str) -> dict[str, Any]:
        result = await self.wallet("gettransaction", txid)
        if not isinstance(result, dict):
            raise DashdError("gettransaction returned a non-object")
        return result

    async def getrawtransaction(self, txid: str, verbose: bool = True) -> Any:
        return await self.node("getrawtransaction", txid, verbose)

    async def validateaddress(self, address: str) -> dict[str, Any]:
        result = await self.node("validateaddress", address)
        if not isinstance(result, dict):
            raise DashdError("validateaddress returned a non-object")
        return result

    async def estimatesmartfee(self, conf_target: int = 1) -> dict[str, Any]:
        result = await self.node("estimatesmartfee", conf_target)
        if not isinstance(result, dict):
            raise DashdError("estimatesmartfee returned a non-object")
        return result

    async def createrawtransaction(
        self, inputs: list[dict[str, Any]], outputs: dict[str, Any]
    ) -> str:
        result = await self.node("createrawtransaction", inputs, outputs)
        if not isinstance(result, str) or not result:
            raise DashdError("createrawtransaction returned a non-hex")
        return result

    async def signrawtransactionwithkey(self, raw_hex: str, keys: list[str]) -> dict[str, Any]:
        result = await self.node("signrawtransactionwithkey", raw_hex, keys)
        if not isinstance(result, dict):
            raise DashdError("signrawtransactionwithkey returned a non-object")
        return result

    async def sendrawtransaction(self, raw_hex: str) -> str:
        result = await self.node("sendrawtransaction", raw_hex)
        if not isinstance(result, str) or not result:
            raise DashdError("sendrawtransaction returned a non-txid")
        return result

    async def lockunspent(self, unlock: bool, outputs: list[dict[str, Any]]) -> bool:
        result = await self.wallet("lockunspent", unlock, outputs)
        return bool(result)
