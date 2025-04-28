from datetime import datetime, timezone
from typing import Any, override

from v4vapp_backend_v2.hive_models.op_base import OpBase


class BlockMarker(OpBase):
    def __init__(self, block_num: int, timestamp: datetime | None = None):
        super().__init__(
            block_num=block_num,
            trx_id="block_marker",
            type="block_marker",
            timestamp=timestamp or datetime.now(tz=timezone.utc),
        )

    @property
    @override
    def db_query(self) -> dict[str, Any]:
        ans = {"trx_id": self.trx_id, "realm": self.realm}
        return ans
