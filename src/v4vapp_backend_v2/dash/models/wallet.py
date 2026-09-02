from pydantic import BaseModel, Field

from v4vapp_backend_v2.config.setup import DashNetwork


class Derivation(BaseModel):
    account: int = 0
    change: int
    index: int
    path: str


class XpubMaterial(BaseModel):
    network: DashNetwork
    account_xpub: str
    master_fingerprint: str = Field(min_length=8, max_length=8)
