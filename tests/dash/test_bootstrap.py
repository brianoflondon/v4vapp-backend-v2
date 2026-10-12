from typing import Any

import pytest

from v4vapp_backend_v2.dash.dashd.bootstrap import bootstrap_watch_wallet
from v4vapp_backend_v2.dash.dashd.rpc import WalletDisabled

XPUB = (
    "xpub6CYEjsU6zPM3sADS2ubu2aZeGxCm3C5KabkCpo4rkNbXGAH9M7rRUJ4E5CKiyUddm"
    "RzrSCopPzisTBrXkfCD4o577XKM9mzyZtP1Xdbizyk"
)
FP = "73c5da0a"


class _FakeDashd:
    wallet_name = "watch"

    def __init__(self) -> None:
        self.wallets: list[str] = []
        self.created = 0
        self.imports: list[list[dict[str, Any]]] = []
        self.listed: dict[str, Any] = {"descriptors": []}

    async def listwallets(self) -> list[str]:
        return list(self.wallets)

    async def create_watch_wallet(self) -> None:
        self.created += 1
        self.wallets.append(self.wallet_name)

    async def getdescriptorinfo(self, desc: str) -> dict[str, str]:
        return {"descriptor": desc + "#cksum", "checksum": "cksum"}

    async def listdescriptors(self) -> dict[str, Any]:
        return self.listed

    async def importdescriptors(self, requests: list[dict[str, Any]]) -> list[dict[str, bool]]:
        self.imports.append(requests)
        return [{"success": True} for _ in requests]


class _DisabledDashd(_FakeDashd):
    async def listwallets(self) -> list[str]:
        raise WalletDisabled("Method not found")


@pytest.mark.asyncio
async def test_creates_wallet_and_imports_both_descriptors() -> None:
    dashd = _FakeDashd()
    result = await bootstrap_watch_wallet(
        dashd,  # type: ignore[arg-type]
        network="mainnet",
        account_xpub=XPUB,
        fingerprint=FP,
        range_end=100000,
    )
    assert result["status"] == "ok"
    assert dashd.created == 1
    assert len(dashd.imports) == 1
    recv, change = dashd.imports[0]
    assert recv["internal"] is False
    assert change["internal"] is True
    assert recv["timestamp"] == "now"
    assert recv["range"] == [0, 100000]
    assert "watchonly" not in recv
    assert f"[{FP}/44h/5h/0h]" in recv["desc"]
    assert recv["desc"].endswith("#cksum")


@pytest.mark.asyncio
async def test_skips_import_when_descriptors_exist() -> None:
    dashd = _FakeDashd()
    dashd.wallets = ["watch"]
    recv = f"pkh([{FP}/44h/5h/0h]{XPUB}/0/*)#cksum"
    change = f"pkh([{FP}/44h/5h/0h]{XPUB}/1/*)#cksum"
    dashd.listed = {"descriptors": [{"desc": recv}, {"desc": change}]}
    result = await bootstrap_watch_wallet(
        dashd,  # type: ignore[arg-type]
        network="mainnet",
        account_xpub=XPUB,
        fingerprint=FP,
        range_end=100000,
    )
    assert result["imported"] is False
    assert dashd.imports == []


@pytest.mark.asyncio
async def test_wallet_disabled_is_not_fatal() -> None:
    result = await bootstrap_watch_wallet(
        _DisabledDashd(),  # type: ignore[arg-type]
        network="mainnet",
        account_xpub=XPUB,
        fingerprint=FP,
        range_end=100000,
    )
    assert result["status"] == "wallet_disabled"
