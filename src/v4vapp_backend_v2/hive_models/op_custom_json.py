import json
from datetime import datetime
from typing import List

from pydantic import Field

from v4vapp_backend_v2.hive_models.custom_json_data import CustomJsonData, custom_json_test_data
from v4vapp_backend_v2.hive_models.op_base import OpBase


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
        SpecialJsonType = custom_json_test_data(data)
        if SpecialJsonType is not None:
            try:
                json_object = SpecialJsonType.model_validate(json.loads(data["json"]))
                data["json"] = json_object
            except ValueError as e:
                raise ValueError(
                    f"Invalid JSON data for operation ID {data['id']}: {data['json']} - {e}"
                )
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        # check if self.json_data has method log_str
        if hasattr(self.json_data, "log_str"):
            return f"{self.json_data.log_str} {self.link}"
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.cj_id[:19]:>20} | {self.op_in_trx:<3} | {self.link}"

    @property
    def notification_str(self) -> str:
        if hasattr(self.json_data, "notification_str"):
            return f"{self.json_data.notification_str} {self.markdown_link}"
        return f"{self.block_num:,} | {self.age:.2f} | {self.timestamp:%Y-%m-%d %H:%M:%S} {self.realm:<8} | {self.cj_id[:19]:>20} | {self.op_in_trx:<3} | {self.markdown_link}"
