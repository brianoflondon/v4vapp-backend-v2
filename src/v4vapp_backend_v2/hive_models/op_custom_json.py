import json
from datetime import datetime
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
        if data.get("memo", None) is None:
            data["memo"] = ""
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        if self.to_account == "":
            return f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {self.memo}"
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )


CustomJsonData = Union[Dict[str, Any], KeepsatsTransfer]


def custom_json_filter(data: Dict[str, Any]) -> CustomJsonData:
    """
    Filters and processes a JSON object based on its operation ID.

    This function checks if the provided data's "id" is in the `keepsats_ids` list.
    If the "id" is valid and the "json" field is a string, it parses the JSON string
    into a dictionary and validates it against the `KeepsatsTransfer` model.

    Args:
        data (Dict[str, Any]): A dictionary containing the operation data.
            Expected keys are:
            - "id" (str): The operation ID.
            - "json" (str or dict): The JSON data to be processed.

    Returns:
        CustomJsonData: The validated and processed JSON data.

    Raises:
        ValueError: If the operation ID is not recognized or invalid.
    """
    if data["id"] in keepsats_ids:
        if isinstance(data["json"], str):
            data["json"] = json.loads(data["json"])
            return KeepsatsTransfer.model_validate(data["json"])
    raise ValueError(f"Unknown operation ID: {data['id']}")


def custom_json_test(data: Dict[str, Any]) -> bool:
    """
    Checks if the given data dictionary meets specific conditions.

    This function verifies if the "id" key in the provided dictionary exists
    in the global `keepsats_ids` collection and if the value associated with
    the "json" key is a string.

    Args:
        data (Dict[str, Any]): A dictionary containing the keys "id" and "json".

    Returns:
        bool: True if the "id" is in `keepsats_ids` and "json" is a string,
              otherwise False.
    """
    if data["id"] in keepsats_ids:
        if isinstance(data["json"], str):
            return True
    return False


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
        if not custom_json_test(data):
            raise ValueError("Invalid CustomJson data")
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
            return f"{self.json_data.log_str} {self.link}"
        return json.dumps(self.json_data)

    @property
    def notification_str(self) -> str:
        # check if self.json_data has method log_str
        if hasattr(self.json_data, "log_str"):
            return f"{self.json_data.log_str} {self.markdown_link}"
        return json.dumps(self.json_data)

    @classmethod
    def test(cls, data: Dict[str, Any]) -> bool:
        if data.get("json", None) is None:
            return False
        return custom_json_test(data)
