import json
from datetime import datetime
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field

from v4vapp_backend_v2.hive_models.op_base import OpBase

allowed_ids = ["v4vapp_transfer"]


class KeepsatsTransfer(BaseModel):
    from_account: str = Field(alias="hive_accname_from")
    to_account: str = Field(alias="hive_accname_to")
    sats: int
    memo: str = ""

    def __init__(self, **data: Any):
        print("KeepsatsTransfer data:", data)
        if data.get("memo", None) is None:
            data["memo"] = ""
        super().__init__(**data)


class CustomJsonKeepsats(OpBase):
    type: str
    cj_id: str = Field(alias="id")
    json_data: KeepsatsTransfer = Field(alias="json")
    required_auths: List[str]
    required_posting_auths: List[str]
    timestamp: datetime
    block_num: int
    trx_num: int

    def __init__(self, **data):
        if data["id"] not in allowed_ids:
            raise ValueError(f"Unknown operation ID: {data['id']}")
        if isinstance(data["json"], str):
            data["json"] = json.loads(data["json"])
        super().__init__(**data)
