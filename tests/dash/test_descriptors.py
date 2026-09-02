from v4vapp_backend_v2.dash.wallet.descriptors import (
    checksummed_descriptor,
    import_request,
    origin_path,
    raw_descriptor,
)

MAINNET_XPUB = (
    "xpub6CYEjsU6zPM3sADS2ubu2aZeGxCm3C5KabkCpo4rkNbXGAH9M7rRUJ4E5CKiyUddm"
    "RzrSCopPzisTBrXkfCD4o577XKM9mzyZtP1Xdbizyk"
)
FP = "73c5da0a"


def test_origin_paths() -> None:
    assert origin_path("mainnet") == "44h/5h/0h"
    assert origin_path("testnet") == "44h/1h/0h"
    assert origin_path("regtest") == "44h/1h/0h"


def test_raw_receive_and_change() -> None:
    recv = raw_descriptor(FP, MAINNET_XPUB, "mainnet", change=False)
    change = raw_descriptor(FP, MAINNET_XPUB, "mainnet", change=True)
    assert recv == f"pkh([{FP}/44h/5h/0h]{MAINNET_XPUB}/0/*)"
    assert change == f"pkh([{FP}/44h/5h/0h]{MAINNET_XPUB}/1/*)"
    assert "watchonly" not in recv


def test_import_request_has_official_fields_only() -> None:
    req = import_request("pkh(xpub)/0/*#cksum", internal=False, range_end=100000)
    assert req == {
        "desc": "pkh(xpub)/0/*#cksum",
        "timestamp": "now",
        "active": True,
        "internal": False,
        "range": [0, 100000],
    }
    assert "watchonly" not in req


def test_checksum_from_getdescriptorinfo() -> None:
    raw = "pkh(xpub/0/*)"
    assert checksummed_descriptor({"descriptor": raw + "#abcd1234"}, raw) == raw + "#abcd1234"
    assert checksummed_descriptor({"checksum": "zzzz9999"}, raw) == raw + "#zzzz9999"
