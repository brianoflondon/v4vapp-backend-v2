from typing import Any, Dict, Type, Union

from pydantic import BaseModel, Field, Json

from v4vapp_backend_v2.hive_models.account_name_type import AccNameType

"""
This module defines a custom Pydantic model `KeepsatsTransfer` and related utilities for handling
custom JSON data in the context of the v4vapp backend.

Classes:
    - KeepsatsTransfer: A Pydantic model representing a transfer of sats (satoshis) between accounts
      using the Keepsats service. It includes fields for the sender account, recipient account,
      amount of sats transferred, and an optional memo. The class also provides a `log_str` property
      for generating a human-readable log message.

Type Aliases:
    - CustomJsonData: A union type that can either be a JSON object or an instance of `KeepsatsTransfer`.

Constants:
    - CUSTOM_JSON_IDS: A dictionary mapping custom JSON identifiers to their corresponding Pydantic
      models. Currently, it maps "v4vapp_transfer" to the `KeepsatsTransfer` model.
"""


class PayResult(BaseModel):
    payment_error: str = ""
    payment_preimage: str = ""
    payment_hash: str = ""


class KeepsatsTransfer(BaseModel):
    from_account: AccNameType = Field("", alias="hive_accname_from")
    to_account: AccNameType = Field("", alias="hive_accname_to")
    sats: int
    memo: str = ""
    pay_result: PayResult | None = None
    HIVE: float | None = None
    HBD: float | None = None
    invoice_message: str | None = None

    def __init__(self, **data: Any):
        if data.get("memo", None) is None:
            data["memo"] = ""
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        message_memo = self.invoice_message or self.memo
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {message_memo}"
            )
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )

    @property
    def notification_str(self) -> str:
        message_memo = self.invoice_message or self.memo
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {message_memo}"
            )
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )


CustomJsonData = Union[Json, KeepsatsTransfer]

CUSTOM_JSON_IDS = {"v4vapp_transfer": KeepsatsTransfer}


def custom_json_test_data(data: Dict[str, Any]) -> Type[BaseModel] | None:
    """
    Tests if the JSON data is valid for a specific operation ID.
    This function checks if the provided data's "id" is in the `CUSTOM_JSON_IDS` dictionary.
    If the "id" is valid, it returns the corresponding model class.
    If the "id" is not valid, it returns None.
    Args:
        data (Dict[str, Any]): A dictionary containing the operation data.
            Expected keys are:
            - "id" (str): The operation ID.
            - "json" (str or dict): The JSON data to be processed.
    Returns:
        Type[BaseModel] | None: The model class corresponding to the operation ID,
            or None if the operation ID is not recognized.

    """
    if data.get("id", "") in CUSTOM_JSON_IDS.keys():
        return CUSTOM_JSON_IDS[data["id"]]
    return None


def custom_json_test_id(cj_id: str) -> Type[BaseModel] | None:
    if cj_id in CUSTOM_JSON_IDS:
        return CUSTOM_JSON_IDS[cj_id]
    return None
