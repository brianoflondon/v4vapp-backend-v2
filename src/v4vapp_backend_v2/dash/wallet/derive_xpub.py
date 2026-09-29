"""Offline CLI: mnemonic → account xpub + master fingerprint. Never run on the server."""

from __future__ import annotations

import argparse
import json
import sys

from bip_utils import (
    Bip32Slip10Secp256k1,
    Bip39MnemonicValidator,
    Bip39SeedGenerator,
    Bip44,
)

from v4vapp_backend_v2.config.setup import DashNetwork
from v4vapp_backend_v2.dash.models.wallet import XpubMaterial
from v4vapp_backend_v2.dash.wallet.hd import coin_for_network, derive_receive


def material_from_mnemonic(mnemonic: str, network: DashNetwork) -> XpubMaterial:
    mnemonic = " ".join(mnemonic.split())
    if not Bip39MnemonicValidator().IsValid(mnemonic):
        raise ValueError("invalid BIP39 mnemonic")
    seed = Bip39SeedGenerator(mnemonic).Generate()
    fingerprint = Bip32Slip10Secp256k1.FromSeed(seed).FingerPrint().ToHex()
    account = Bip44.FromSeed(seed, coin_for_network(network)).Purpose().Coin().Account(0)
    return XpubMaterial(
        network=network,
        account_xpub=account.PublicKey().ToExtended(),
        master_fingerprint=fingerprint,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Derive a Dash account xpub and master fingerprint from a "
        "BIP39 mnemonic. Prints JSON and exits. Do not run this on the API host."
    )
    parser.add_argument(
        "--network",
        choices=("mainnet", "testnet", "regtest"),
        default="mainnet",
    )
    parser.add_argument(
        "--mnemonic",
        help="BIP39 mnemonic. If omitted, read a single line from stdin.",
    )
    parser.add_argument(
        "--addresses",
        type=int,
        default=3,
        help="Print the first N receive addresses to match a wallet app (default 3).",
    )
    args = parser.parse_args(argv)

    mnemonic = args.mnemonic if args.mnemonic is not None else sys.stdin.readline()
    if not mnemonic or not mnemonic.strip():
        print("error: empty mnemonic", file=sys.stderr)
        return 2
    try:
        material = material_from_mnemonic(mnemonic, args.network)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    payload = material.model_dump()
    addrs = []
    for index in range(max(args.addresses, 0)):
        address, der = derive_receive(material.account_xpub, args.network, index)
        addrs.append({"index": index, "path": der.path, "address": address})
    payload["receive_addresses"] = addrs
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
