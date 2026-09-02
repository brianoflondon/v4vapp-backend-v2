import json
from dataclasses import dataclass
from pathlib import Path

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.models.wallet import XpubMaterial
from v4vapp_backend_v2.dash.wallet.derive_xpub import material_from_mnemonic


def load_xpub_material(conn: DashConnectionConfig) -> XpubMaterial | None:
    if conn.xpub and conn.master_fingerprint:
        return XpubMaterial(
            network=conn.network,
            account_xpub=conn.xpub,
            master_fingerprint=conn.master_fingerprint,
        )
    if conn.xpub_file:
        path = Path(conn.xpub_file)
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            material = XpubMaterial.model_validate(data)
            if material.network != conn.network:
                raise ValueError(
                    f"xpub file network {material.network} != dash network {conn.network}"
                )
            return material
    return None


def load_mnemonic(conn: DashConnectionConfig) -> str | None:
    """BIP39 words from mnemonic_file. Blank lines and `#` comments are ignored."""
    if not conn.mnemonic_file:
        return None
    path = Path(conn.mnemonic_file)
    if not path.is_file():
        return None
    words: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        words.extend(line.split())
    text = " ".join(words)
    return text or None


def mnemonic_matches_xpub(mnemonic: str, conn: DashConnectionConfig) -> bool:
    """Refuse to sign if the seed does not match the watch-only xpub."""
    try:
        derived = material_from_mnemonic(mnemonic, conn.network)
    except ValueError:
        return False
    stored_fp = (conn.master_fingerprint or "").lower()
    fp_ok = not stored_fp or derived.master_fingerprint.lower() == stored_fp
    xpub_ok = not conn.xpub or derived.account_xpub == conn.xpub
    return fp_ok and xpub_ok


@dataclass(frozen=True)
class DashSpendCheck:
    fingerprint: str
    payouts_enabled: bool
    mnemonic_present: bool
    can_sign: bool
    problem: str | None = None


def check_dash_spend_keys(conn: DashConnectionConfig) -> DashSpendCheck:
    """Startup/payout gate. Never returns the mnemonic or xpub."""
    fp = (conn.master_fingerprint or "").lower()
    mnemonic = load_mnemonic(conn)
    if not conn.payouts_enabled:
        return DashSpendCheck(
            fingerprint=fp,
            payouts_enabled=False,
            mnemonic_present=bool(mnemonic),
            can_sign=False,
            problem=None,
        )
    if not conn.mnemonic_file:
        return DashSpendCheck(fp, True, False, False, "mnemonic_file is not set")
    if not Path(conn.mnemonic_file).is_file():
        return DashSpendCheck(
            fp, True, False, False, f"mnemonic_file not found: {conn.mnemonic_file}"
        )
    if not mnemonic:
        return DashSpendCheck(fp, True, False, False, "mnemonic_file has no seed words")
    try:
        derived = material_from_mnemonic(mnemonic, conn.network)
    except ValueError:
        return DashSpendCheck(fp, True, True, False, "mnemonic is not valid BIP39")
    derived_fp = derived.master_fingerprint.lower()
    if fp and derived_fp != fp:
        return DashSpendCheck(
            fp,
            True,
            True,
            False,
            f"mnemonic fingerprint {derived_fp} != config {fp}",
        )
    if conn.xpub and derived.account_xpub != conn.xpub:
        return DashSpendCheck(
            derived_fp or fp,
            True,
            True,
            False,
            "mnemonic does not derive the configured xpub",
        )
    return DashSpendCheck(derived_fp or fp, True, True, True, None)
