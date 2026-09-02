from decimal import Decimal

from pydantic import BaseModel, Field


class Quote(BaseModel):
    source: str
    fetched_at: str
    btc_usd: Decimal
    dash_usd: Decimal
    dash_btc: Decimal
    sats_per_dash: Decimal
    ttl_s: int = 60
    duffs_quoted: Decimal = Field(gt=0)
    dash_quoted: str
