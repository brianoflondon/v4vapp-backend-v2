"""Golden vectors from the well-known BIP39 test mnemonic (not a secret)."""

from v4vapp_backend_v2.dash.wallet.derive_xpub import material_from_mnemonic
from v4vapp_backend_v2.dash.wallet.hd import derivation_path, derive_address, derive_receive

TEST_MNEMONIC = (
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
)

MAINNET_XPUB = (
    "xpub6CYEjsU6zPM3sADS2ubu2aZeGxCm3C5KabkCpo4rkNbXGAH9M7rRUJ4E5CKiyUddm"
    "RzrSCopPzisTBrXkfCD4o577XKM9mzyZtP1Xdbizyk"
)
TESTNET_XPUB = (
    "tpubDC5FSnBiZDMmhiuCmWAYsLwgLYrrT9rAqvTySfuCCrgsWz8wxMXUS9Tb9iVMvcRbv"
    "FcAHGkMD5Kx8koh4GquNGNTfohfk7pgjhaPCdXpoba"
)
MASTER_FINGERPRINT = "73c5da0a"

MAINNET_ADDRS = {
    0: "XoJA8qE3N2Y3jMLEtZ3vcN42qseZ8LvFf5",
    1: "XbctnEsgWTn5j1co3emZynemxSFPqkLRKZ",
    42: "XexNL9fN4SohEWsEkTc3h6KJ3LQp4hA3bE",
}
TESTNET_ADDRS = {
    0: "yRd4FhXfVGHXpsuZXPNkMrfD9GVj46pnjt",
    1: "yfd64jEpzzTLrHnR1wq3iiYXh68AiU8mcw",
    42: "ycf6zC2C6Zx6hB2LGsR5M8CfN2sV98aBgT",
}


def test_paths() -> None:
    assert derivation_path("mainnet", 42) == "m/44'/5'/0'/0/42"
    assert derivation_path("testnet", 42) == "m/44'/1'/0'/0/42"
    assert derivation_path("regtest", 42) == "m/44'/1'/0'/0/42"
    assert derivation_path("mainnet", 0, change=1) == "m/44'/5'/0'/1/0"


def test_material_from_mnemonic() -> None:
    main = material_from_mnemonic(TEST_MNEMONIC, "mainnet")
    assert main.master_fingerprint == MASTER_FINGERPRINT
    assert main.account_xpub == MAINNET_XPUB
    test = material_from_mnemonic(TEST_MNEMONIC, "testnet")
    assert test.account_xpub == TESTNET_XPUB
    reg = material_from_mnemonic(TEST_MNEMONIC, "regtest")
    assert reg.account_xpub == TESTNET_XPUB


def test_mainnet_receive_addresses() -> None:
    for index, expected in MAINNET_ADDRS.items():
        address, der = derive_receive(MAINNET_XPUB, "mainnet", index)
        assert address == expected
        assert address.startswith("X")
        assert der.path == f"m/44'/5'/0'/0/{index}"


def test_testnet_and_regtest_share_path() -> None:
    for network in ("testnet", "regtest"):
        for index, expected in TESTNET_ADDRS.items():
            address = derive_address(TESTNET_XPUB, network, index)
            assert address == expected
            assert address.startswith("y")


def test_xpub_has_no_private_key() -> None:
    from bip_utils import Bip32KeyError, Bip44, Bip44Coins

    acct = Bip44.FromExtendedKey(MAINNET_XPUB, Bip44Coins.DASH)
    try:
        acct.PrivateKey()
        raise AssertionError("account xpub must not expose a private key")
    except Bip32KeyError:
        pass
