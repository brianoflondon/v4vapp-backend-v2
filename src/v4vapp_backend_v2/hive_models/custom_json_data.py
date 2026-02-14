from decimal import Decimal
from typing import Any, Dict, List, Type, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.general_purpose_funcs import lightning_memo, snake_case
from v4vapp_backend_v2.hive.hive_extras import process_user_memo
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
    """
    Represents a Keepsats transfer transaction, including sender and receiver Hive account names, amount in sats,
    memo, payment result, and optional invoice message.

    Attributes:
        from_account (AccNameType): Hive account name of the sender.
        to_account (AccNameType): Hive account name of the receiver.
        sats (int): Amount of sats transferred.
        memo (str): Memo associated with the transfer.
        pay_result (PayResult | None): Result of the payment, if available.
        HIVE (float | None): Optional amount in HIVE currency.
        HBD (float | None): Optional amount in HBD currency.
        invoice_message (str | None): Message used for invoices from foreign services.

    Properties:
        log_str (str): Returns a formatted log string describing the transfer.
        description (str): Returns a description string for the transfer, used in ledger entries.
        notification_str (str): Returns a formatted notification string for the transfer.

    Config:
        model_config: ConfigDict to allow population by field name.

    Methods:
        __init__(**data): Initializes the KeepsatsTransfer object, ensuring memo is set to an empty string if not provided.
    """

    from_account: AccNameType = Field("", alias="hive_accname_from")
    to_account: AccNameType = Field("", alias="hive_accname_to")
    sats: Decimal | None = Field(
        None,
        ge=0,
        description="The amount of sats being transferred. Not needed if we are sending a fixed amount invoice, used if we are using a lightning address or zero value invoice (used as an upper limit sometimes)",
    )
    msats: Decimal | None = Field(
        None,
        ge=0,
        description=(
            "The amount of millisatoshis being transferred. "
            "Used for more precise amounts, especially in invoices. "
            "Mutually exclusive with sats, if both are present, msats will decide the value."
        ),
    )
    memo: str = Field("", description="The memo which comes in from the transfer")
    pay_result: PayResult | None = None
    notification: bool = Field(
        False, description="If True, this is a notification rather than a transfer"
    )
    parent_id: str | None = Field(
        None, description="The short ID of the parent transaction, if applicable"
    )
    hive: Decimal | None = Field(
        default=None,
        description="If converting from Keepsats to Hive/HBD, this amount will be used to calculate how many keepsats to debit",
    )
    hbd: Decimal | None = Field(
        default=None,
        description="If converting from Keepsats to Hive/HBD, this amount will be used to calculate how many keepsats to debit",
    )
    invoice_message: str | None = Field(
        None,
        description="Used specifically for invoice messages, when requesting an invoice from a foreign service, this comment will be sent",
    )

    model_config = ConfigDict(populate_by_name=True)

    def __init__(self, **data: Any):
        if data.get("memo", None) is None:
            data["memo"] = ""
        if data.get("msats") is not None and data.get("sats") is None:
            # If both sats and msats are provided, use msats for the amount
            data["sats"] = int(data["msats"]) // 1_000
        if data.get("sats") is None and data.get("msats") is None:
            data["sats"] = 0
            data["msats"] = 0
        if data.get("sats") is not None and data.get("msats") is None:
            data["sats"] = int(data["sats"])
            data["msats"] = data["sats"] * 1_000
        super().__init__(**data)

    @property
    def notification_str(self) -> str:
        return self.log_str

    @property
    def log_str(self) -> str:
        message_memo = self.invoice_message or self.memo
        message_memo = lightning_memo(message_memo)
        if self.sats is None or self.notification:
            return f"⏩️{self.from_account} notification {message_memo} {self.to_account}"
        if self.to_account == "":
            return (
                f"⏩️{self.from_account} sent {self.sats:,.0f} sats via Keepsats to {message_memo}"
            )
        # This is the case if we are passing the lighting invoice to be paid via Keepsats
        if not self.msats or self.msats == Decimal(0):
            return f"⏩️{self.from_account} instruction to {self.to_account} to pay {lightning_memo(self.memo)} sats via Keepsats"
        return (
            f"⏩️{self.from_account} sent {self.sats:,.0f} sats to {self.to_account} via KeepSats"
        )

    @classmethod
    def name(cls) -> str:
        """
        Returns the name of the class in snake_case format.
        """
        return snake_case(cls.__name__)

    @property
    def log_extra(self) -> Dict[str, Any]:
        """
        Returns a dictionary of extra log information for the Keepsats transfer.
        This is used for logging purposes to provide additional context about the transfer.
        Follows the OpBase pattern of using model_dump() keyed by class name.
        """
        return {self.name(): self.model_dump(exclude_none=True, exclude_unset=True)}

    @property
    def description(self) -> str:
        """
        Returns a description string for the Keepsats transfer.
        Used in the LedgerEntry creation.
        If the invoice_message is set, it returns that; otherwise, it returns the memo.
        """
        return self.log_str

    @property
    def user_memo(self) -> str:
        """
        Returns the user memo, which is the decoded memo if available,
        otherwise returns the original memo.

        Returns:
            str: The user memo.
        """
        return process_user_memo(self.memo)

    @field_validator("sats", "msats", mode="before")
    @classmethod
    def convert_to_decimal(cls, v):
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        return v


CustomJsonData = Union[Any, KeepsatsTransfer, VSCTransfer]

# This dictionary maps custom JSON operation IDs to their corresponding Pydantic models.
# Whilst the v4vapp_dev ones could be generated from the custom_json_prefix, we hardcode them here for better clarity and to avoid potential issues with dynamic generation, such as if
# the suffixes change in the future or if there are other custom JSON IDs that don't follow the prefix pattern
CUSTOM_JSON_IDS: Dict[str, Type[BaseModel]] = {
    "v4vapp_dev_transfer": KeepsatsTransfer,
    "v4vapp_dev_notification": KeepsatsTransfer,
    "v4vapp_transfer": KeepsatsTransfer,
    "v4vapp_notification": KeepsatsTransfer,
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
    extra_ids = InternalConfig().config.hive.custom_json_ids_tracked
    duplicates_removed = set(list(CUSTOM_JSON_IDS.keys()) + extra_ids)
    return list(duplicates_removed)


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
    cj_id = data.get("id", None)
    if cj_id is None:
        return None
    if cj_id in CUSTOM_JSON_IDS.keys():
        return CUSTOM_JSON_IDS[cj_id] if isinstance(CUSTOM_JSON_IDS[cj_id], type) else None

    # Extra steps to combine the custom_json_prefix with the suffixes to check for valid IDs, this allows us to not have to hardcode every custom JSON ID in the config
    if getattr(InternalConfig().config.hive, "custom_json_prefix", None):
        prefix = InternalConfig().config.hive.custom_json_prefix
        if cj_id.startswith(prefix):
            suffix = cj_id[len(prefix) :]
            if suffix in ["_transfer", "_notification"]:
                return KeepsatsTransfer
    return None


# def custom_json_test_id(cj_id: str) -> Type[BaseModel] | None:
#     if cj_id in CUSTOM_JSON_IDS:
#         return CUSTOM_JSON_IDS[cj_id]
#     return None
