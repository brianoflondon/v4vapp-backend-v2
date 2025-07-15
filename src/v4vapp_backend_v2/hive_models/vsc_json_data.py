from typing import List

from pydantic import BaseModel, ConfigDict, Field, model_validator

from v4vapp_backend_v2.hive_models.did_name_type import DIDNameType


class VSCTransfer(BaseModel):
    from_account: DIDNameType = Field(..., alias="from", description="The sender account.")
    to_account: DIDNameType = Field(..., alias="to", description="The recipient account.")
    amount: str = Field(..., description="The amount being transferred.")
    asset: str = Field(..., description="The asset type (e.g., 'hive').")
    memo: str = Field("", description="Optional memo for the transfer.")
    net_id: str = Field(..., description="The network ID (e.g., 'vsc-mainnet').")

    model_config = ConfigDict(
        populate_by_name=True,
    )

    def __init__(self, **data):
        super().__init__(**data)

    @property
    def log_str(self) -> str:
        """
        Generate a log string for the transfer.

        Returns:
            str: A formatted string representing the transfer.
        """
        memo = f" with memo: {self.memo}" if self.memo else ""

        return (
            f"⏩️ VSC {self.from_account} sent {self.amount} {self.asset} "
            f"via VSC to {self.to_account}{memo}"
        )

    @property
    def notification_str(self) -> str:
        """
        Generate a notification string for the transfer.

        Returns:
            str: A formatted string representing the transfer for notifications.
        """
        return self.log_str


class VSCActions(BaseModel):
    ops: List[str] = []
    cleared_ops: List[str] = []

    model_config = ConfigDict(
        populate_by_name=True,
    )

    @model_validator(mode="before")
    @classmethod
    def fix_ops_and_cleared_ops(cls, data):
        # If ops or cleared_ops is an empty string, convert to empty list
        if isinstance(data, dict):
            if isinstance(data.get("ops"), str) and data["ops"] == "":
                data["ops"] = []
            if isinstance(data.get("cleared_ops"), str) and data["cleared_ops"] == "":
                data["cleared_ops"] = []
        return data
