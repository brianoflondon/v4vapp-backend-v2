import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Union

from pydantic import BaseModel, Field

from v4vapp_backend_v2.hive_models.op_base import OpBase

keepsats_ids = ["v4vapp_transfer"]


class KeepsatsTransfer(BaseModel):
    from_account: str = Field("", alias="hive_accname_from")
    to_account: str = Field("", alias="hive_accname_to")
    sats: int
    memo: str = ""

    def __init__(self, **data: Any):
        print("KeepsatsTransfer data:", data)
        if data.get("memo", None) is None:
            data["memo"] = ""
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} "
                f"sats via Keepsats to {self.memo}"
            )
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} "
            f"sats to {self.to_account} via KeepSats"
        )


CustomJsonData = Union[Dict[str, Any], KeepsatsTransfer]


def custom_json_filter(data: Dict[str, Any]) -> CustomJsonData:
    if data["id"] in keepsats_ids:
        if isinstance(data["json"], str):
            data["json"] = json.loads(data["json"])
            return KeepsatsTransfer.model_validate(data["json"])
    # if "pp_" in data["id"]:
    #     data["json"] = json.loads(data["json"])
    #     return data["json"]

    raise ValueError(f"Unknown operation ID: {data['id']}")


class CustomJson(OpBase):
    type: str
    cj_id: str = Field(alias="id")
    json_data: CustomJsonData = Field(alias="json")
    required_auths: List[str]
    required_posting_auths: List[str]
    timestamp: datetime
    block_num: int
    trx_num: int

    def __init__(self, **data):
        try:
            json_object = custom_json_filter(data)
            data["json"] = json_object
        except ValueError:
            raise

        super().__init__(**data)

    @property
    def log_str(self) -> str:
        # check if self.json_data has method log_str
        if hasattr(self.json_data, "log_str"):
            return self.json_data.log_str
        return json.dumps(self.json_data)
