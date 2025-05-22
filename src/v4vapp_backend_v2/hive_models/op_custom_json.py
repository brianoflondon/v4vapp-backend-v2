import json
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

    # Extra Fields
    conv: CryptoConv | None = Field(
        default=None,
        description="If the custom_json relates to an amount, store a conversion object.",
    )

    def __init__(self, **data):
        """
        Initializes an instance of the class with the provided data.

        Args:
            **data: Arbitrary keyword arguments containing the data to initialize the instance.

        Raises:
            ValueError: If the provided JSON data is invalid or cannot be parsed.

        Notes:
            - If `custom_json_test_data` returns a `SpecialJsonType`, the `json` field in the data
              is validated and parsed using the `model_validate` method of `SpecialJsonType`.
            - If the `json_data` attribute contains a `sats` field and `last_quote` is available,
              a cryptocurrency conversion is performed using the `CryptoConversion` class.
              ONLY if `last_quote` is not None and `last_quote.hive_hbd` is not 0.
            - The `super().__init__(**data)` call initializes the base class with the provided data.
            - The `conv` attribute is set to `None` by default.
        """
        SpecialJsonType = custom_json_test_data(data)
        if SpecialJsonType is not None:
            try:
                if isinstance(data["json"], str):
                    json_data = json.loads(data["json"])
                else:
                    json_data = data["json"]
                json_object = SpecialJsonType.model_validate(json_data)
                data["json"] = json_object
            except ValueError as e:
                raise ValueError(
                    f"Invalid JSON data for operation ID {data['id']}: {data['json']} - {e}"
                )
        super().__init__(**data)
        # test if any key in a json_data is a big int necessary if ingesting podpings!
        # if self.cj_id.startswith(("pp_")):
        #     for key, value in self.json_data.items():  # Changed from self.json_data: to self.json_data.items():
        #         if isinstance(value, int) and value > 2**53:
        #             self.json_data[key] = str(value)
        # TODO: Another place to use historical rates when we have them
        if not self.conv:
            if getattr(self.json_data, "sats", None) is not None:
                if (
                    self.last_quote
                    and not self.last_quote.hive_hbd == 0
                    and hasattr(self.json_data, "sats")
                ):
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
