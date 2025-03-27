from datetime import datetime

from v4vapp_backend_v2.hive_models.op_base import OpBase, OpRealm


class BlockMarker(OpBase):
    type: str = "block_marker"
    block_num: int
    timestamp: datetime
    trx_id: str = "block_marker"
    op_in_trx: int = 0
    realm: OpRealm = OpRealm.MARKER
