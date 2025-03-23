from datetime import datetime

from pydantic import BaseModel


class BlockMarker(BaseModel):
    type: str = "block_marker"
    block_num: int
    timestamp: datetime
    trx_id: str = "block_marker"
    op_in_trx: int = 0
