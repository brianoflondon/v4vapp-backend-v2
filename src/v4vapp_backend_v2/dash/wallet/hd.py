from bip_utils import Bip39SeedGenerator, Bip44, Bip44Changes, Bip44Coins

from v4vapp_backend_v2.config.setup import DashNetwork
from v4vapp_backend_v2.dash.models.wallet import Derivation

COIN_TYPE: dict[DashNetwork, int] = {
    "mainnet": 5,
    "testnet": 1,
    "regtest": 1,
}


def coin_for_network(network: DashNetwork) -> Bip44Coins:
    if network == "mainnet":
        return Bip44Coins.DASH
    return Bip44Coins.DASH_TESTNET


def derivation_path(network: DashNetwork, index: int, *, change: int = 0) -> str:
    if index < 0:
        raise ValueError("index must be >= 0")
    if change not in (0, 1):
        raise ValueError("change must be 0 (receive) or 1 (change)")
    coin_type = COIN_TYPE[network]
    return f"m/44'/{coin_type}'/0'/{change}/{index}"


def derive_address(account_xpub: str, network: DashNetwork, index: int, *, change: int = 0) -> str:
    """Derive a P2PKH address from an account-level xpub. No private keys."""
    coin = coin_for_network(network)
    account = Bip44.FromExtendedKey(account_xpub, coin)
    chain = Bip44Changes.CHAIN_EXT if change == 0 else Bip44Changes.CHAIN_INT
    return account.Change(chain).AddressIndex(index).PublicKey().ToAddress()


def derive_receive(account_xpub: str, network: DashNetwork, index: int) -> tuple[str, Derivation]:
    address = derive_address(account_xpub, network, index, change=0)
    path = derivation_path(network, index, change=0)
    return address, Derivation(account=0, change=0, index=index, path=path)


def derive_change(account_xpub: str, network: DashNetwork, index: int) -> tuple[str, Derivation]:
    address = derive_address(account_xpub, network, index, change=1)
    path = derivation_path(network, index, change=1)
    return address, Derivation(account=0, change=1, index=index, path=path)


def derivation_at(network: DashNetwork, index: int, *, change: int) -> Derivation:
    return Derivation(
        account=0,
        change=change,
        index=index,
        path=derivation_path(network, index, change=change),
    )


def wif_from_mnemonic(mnemonic: str, network: DashNetwork, index: int, *, change: int = 0) -> str:
    """WIF for one BIP44 leaf. Caller must zero/discard the string after signing."""
    if change not in (0, 1):
        raise ValueError("change must be 0 (receive) or 1 (change)")
    seed = Bip39SeedGenerator(" ".join(mnemonic.split())).Generate()
    account = Bip44.FromSeed(seed, coin_for_network(network)).Purpose().Coin().Account(0)
    chain = Bip44Changes.CHAIN_EXT if change == 0 else Bip44Changes.CHAIN_INT
    return account.Change(chain).AddressIndex(index).PrivateKey().ToWif()
