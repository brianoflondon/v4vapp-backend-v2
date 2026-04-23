from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, model_validator

from v4vapp_backend_v2.hive_models.account_name_type import AccName, AccNameType


class VSCCallPayload(BaseModel):
    """Payload nested inside a vsc.call custom_json operation."""

    amount: str = Field(..., description="The amount to transfer (as a string, e.g. '2500').")
    to_account: AccNameType = Field(
        ...,
        alias="to",
        description="Recipient account. Accepts plain Hive names or 'hive:<name>' prefix.",
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def normalise_to(cls, data: Any) -> Any:
        """Strip leading 'hive:' prefix so the value round-trips cleanly through AccName."""
        if isinstance(data, dict):
            raw = data.get("to", "")
            if isinstance(raw, str) and raw.startswith("hive:"):
                data = dict(data)
                data["to"] = raw[len("hive:") :]
        return data


class VSCCall(BaseModel):
    """
    Represents a ``vsc.call`` custom_json operation on the Hive blockchain.

    Example JSON payload::

        {
            "net_id": "vsc-mainnet",
            "caller": "hive:devser.v4vapp",
            "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
            "action": "transfer",
            "payload": {"amount": "2500", "to": "hive:v4vapp-test"},
            "rc_limit": 1000
        }
    """

    net_id: str = Field(..., description="The VSC network identifier (e.g. 'vsc-mainnet').")
    caller: AccNameType = Field(
        ...,
        description="The calling account. Accepts plain Hive names or 'hive:<name>' prefix.",
    )
    contract_id: str = Field(..., description="The VSC smart contract address.")
    action: str = Field(..., description="The contract action to invoke (e.g. 'transfer').")
    payload: VSCCallPayload = Field(..., description="Action-specific payload.")
    rc_limit: int = Field(0, description="Resource credit limit for the operation.")

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def normalise_caller(cls, data: Any) -> Any:
        """Strip leading 'hive:' prefix from caller so AccName validation succeeds."""
        if isinstance(data, dict):
            raw = data.get("caller", "")
            if isinstance(raw, str) and raw.startswith("hive:"):
                data = dict(data)
                data["caller"] = raw[len("hive:") :]
        return data

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def from_account(self) -> AccName:
        """Returns the caller as an AccName (mirrors KeepsatsTransfer / VSCTransfer API)."""
        return AccName(self.caller)

    @property
    def to_account(self) -> AccName:
        """Returns the recipient as an AccName."""
        return AccName(self.payload.to_account)

    @property
    def amount(self) -> str:
        """Returns the transfer amount string from the payload."""
        return self.payload.amount

    @property
    def log_str(self) -> str:
        return (
            f"🔗 VSC call {self.from_account} → {self.contract_id} "
            f"action={self.action} amount={self.amount} to={self.to_account} [{self.net_id}]"
        )

    @property
    def notification_str(self) -> str:
        return self.log_str

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {"vsc_call": self.model_dump(exclude_none=True, exclude_unset=True)}
