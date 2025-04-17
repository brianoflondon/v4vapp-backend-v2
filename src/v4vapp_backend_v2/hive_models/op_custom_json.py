import json
from datetime import datetime
from typing import List

from pydantic import Field

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import (
    CustomJsonData,
    custom_json_test_data,
    custom_json_test_id,
)
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

    # Extra Fields
    conv: CryptoConv | None = Field(
        default=None,
        description="If the custom_json relates to an amount, store a conversion object.",
    )

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
        if getattr(self.json_data, "sats", None) is not None:
            if self.last_quote is None:
                self.update_quote_sync()
            self.conv = CryptoConversion(
                value=self.json_data.sats, conv_from=Currency.SATS, quote=self.last_quote
            ).conversion

    @property
    def is_watched(self) -> bool:
        """
        Check if the transfer is to a watched user.

        Returns:
            bool: True if the transfer is to a watched user, False otherwise.
        """
        if OpBase.watch_users:
            if custom_json_test_id(self.cj_id):
                # Check if the transfer is to a watched user
                if self.json_data.to_account in OpBase.watch_users:
                    return True
                # Check if the transfer is from a watched user
                if self.json_data.from_account in OpBase.watch_users:
                    return True
        return False

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
