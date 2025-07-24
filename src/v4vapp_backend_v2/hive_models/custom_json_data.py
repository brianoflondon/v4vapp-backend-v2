from typing import Any, Dict, List, Type, Union

from pydantic import BaseModel, ConfigDict, Field

from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo
from v4vapp_backend_v2.hive_models.account_name_type import AccNameType
from v4vapp_backend_v2.hive_models.vsc_json_data import VSCActions, VSCTransfer

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
    memo: str = Field("", description="The memo which comes in from the transfer")
    pay_result: PayResult | None = None
    HIVE: float | None = None
    HBD: float | None = None
    invoice_message: str | None = Field(
        None,
        description="Used specifically for invoice messages, when requesting an invoice from a foreign service, this comment will be sent",
    )

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data: Any):
        if data.get("memo", None) is None:
            data["memo"] = ""
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        message_memo = self.invoice_message or self.memo
        message_memo = lightning_memo(message_memo)
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {message_memo}"
            )
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )

    @property
    def description(self) -> str:
        """
        Returns a description string for the Keepsats transfer.
        Used in the LedgerEntry creation.
        If the invoice_message is set, it returns that; otherwise, it returns the memo.
        """
        return self.log_str

    @property
    def notification_str(self) -> str:
        message_memo = self.invoice_message or self.memo
        message_memo = lightning_memo(message_memo)
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {message_memo}"
            )
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )


CustomJsonData = Union[Any, KeepsatsTransfer, VSCTransfer]

CUSTOM_JSON_IDS = {
    "v4vapp_dev_transfer": KeepsatsTransfer,
    "v4vapp_transfer": KeepsatsTransfer,
    "vsc.transfer": VSCTransfer,
    "vsc.withdraw": VSCTransfer,
    "vsc.withdraw_hbd": VSCTransfer,
    "vsc.deposit": VSCTransfer,
    "vsc.deposit_hbd": VSCTransfer,
    "vsc.stake": VSCTransfer,
    "vsc.stake_hbd": VSCTransfer,
    "vsc.unstake": VSCTransfer,
    "vsc.unstake_hbd": VSCTransfer,
    "vsc.actions": VSCActions,
}


def all_custom_json_ids() -> List[str]:
    """
    Returns a list of all custom JSON IDs defined in the CUSTOM_JSON_IDS dictionary.
    This function is useful for retrieving all available custom JSON IDs for validation
    or processing purposes.
    Returns:
        List[str]: A list of custom JSON IDs.
    """
    return list(CUSTOM_JSON_IDS.keys())


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
        return (
            CUSTOM_JSON_IDS[data["id"]] if isinstance(CUSTOM_JSON_IDS[data["id"]], type) else None
        )
    return None


def custom_json_test_id(cj_id: str) -> Type[BaseModel] | None:
    if cj_id in CUSTOM_JSON_IDS:
        return CUSTOM_JSON_IDS[cj_id]
    return None
