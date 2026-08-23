import json
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
    """BIP39 words from mnemonic_file. None if payouts are not configured to sign."""
    if not conn.mnemonic_file:
        return None
    path = Path(conn.mnemonic_file)
    if not path.is_file():
        return None
    text = " ".join(path.read_text(encoding="utf-8").split())
    return text or None


def mnemonic_matches_xpub(mnemonic: str, conn: DashConnectionConfig) -> bool:
    """Refuse to sign if the seed does not match the watch-only xpub."""
    derived = material_from_mnemonic(mnemonic, conn.network)
    stored_fp = (conn.master_fingerprint or "").lower()
    fp_ok = not stored_fp or derived.master_fingerprint.lower() == stored_fp
    xpub_ok = not conn.xpub or derived.account_xpub == conn.xpub
    return fp_ok and xpub_ok
