import json
from pathlib import Path

from v4vapp_backend_v2.config.setup import DashConnectionConfig
from v4vapp_backend_v2.dash.models.wallet import XpubMaterial


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
