import json as json_module
from typing import Any, Dict, List, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class VSCCallPayload(BaseModel):
    """
    Payload for ``vsc.call`` **transfer** actions.

    Example::

        {"amount": "25", "to": "hive:devser.v4vapp", "memo": "user@lnaddr.com #v4vapp"}
    """

    amount: str | None = Field(None, description="Amount to transfer (as a string, e.g. '2500').")
    to: str | None = Field(
        None,
        description="Recipient address in its original network format (e.g. 'hive:<name>', '0x...' for EVM).",
    )
    # V4VAPP specific fields not the necessary part of  Magi Payload
    memo: str = Field("", description="Optional memo attached to the transfer.")
    msats_fee: str | None = Field(
        None,
        description="Optional fee in millisats (as a string, e.g. '250'). Only used for magi transfers.",
    )
    parent_id: str | None = Field(
        None, description="The group_id of the parent transaction, if applicable"
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @property
    def log_str(self) -> str:
        to = f" to={self.to}" if self.to else ""
        memo = f" memo={self.memo}" if self.memo else ""
        return f"🔗 VSC transfer amount={self.amount}{to}{memo}"

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {"vsc_call_payload": self.model_dump(exclude_none=True, exclude_unset=True)}


class VSCSwapPayload(BaseModel):
    """
    Payload for ``vsc.call`` **execute** actions that perform a token swap.

    When the outer ``action`` is ``"execute"``, the ``payload`` field arrives as a
    JSON-encoded *string* on the blockchain.  After deserialisation it contains
    these fields.

    Example (decoded)::

        {
            "type": "swap",
            "version": "1.0.0",
            "asset_in": "HIVE",
            "asset_out": "BTC",
            "amount_in": "26772",
            "min_amount_out": "2041",
            "recipient": "bc1qskmt62sh6ej2tl4ak9wqpr69z7e50yexp2jna9",
            "destination_chain": "BTC"
        }
    """

    type: str = Field(..., description="Payload type (e.g. 'swap').")
    version: str = Field("", description="Protocol version string.")
    asset_in: str = Field(..., description="Input asset ticker (e.g. 'HIVE', 'HBD').")
    asset_out: str = Field(..., description="Output asset ticker (e.g. 'BTC', 'HIVE').")
    amount_in: str = Field(
        ..., description="Amount of the input asset (integer string, no decimal)."
    )
    min_amount_out: str | None = Field(None, description="Minimum acceptable output amount.")
    recipient: str = Field("", description="Destination address or account for the output asset.")
    destination_chain: str = Field("", description="Target chain (e.g. 'BTC', 'HIVE').")

    model_config = ConfigDict(extra="allow")


class VSCIntentArgs(BaseModel):
    """Arguments attached to a single ``VSCIntent``."""

    limit: str = Field("", description="Maximum token amount the intent permits.")
    token: str = Field("", description="Token type (e.g. 'hive', 'hbd').")

    model_config = ConfigDict(extra="allow")


class VSCIntent(BaseModel):
    """
    An intent declaration that accompanies a ``vsc.call`` execute operation.

    Example::

        {"type": "transfer.allow", "args": {"limit": "26.772", "token": "hive"}}
    """

    type: str = Field(..., description="Intent type (e.g. 'transfer.allow').")
    args: VSCIntentArgs = Field(
        default_factory=VSCIntentArgs, description="Intent-specific arguments."
    )

    model_config = ConfigDict(extra="allow")


def _parse_payload(
    raw: Any,
) -> Union["VSCCallPayload", "VSCSwapPayload", Any]:
    """
    Attempt to deserialise a raw payload value into the most specific model.

    - If *raw* is a JSON string, decode it first.
    - If the resulting dict looks like a swap (has ``asset_in``), use ``VSCSwapPayload``.
    - Otherwise use ``VSCCallPayload``.
    - Anything that doesn't match is returned unchanged.
    """
    if isinstance(raw, (VSCCallPayload, VSCSwapPayload)):
        return raw
    if isinstance(raw, str):
        try:
            raw = json_module.loads(raw)
        except (json_module.JSONDecodeError, ValueError):
            return raw
    if isinstance(raw, dict):
        if "asset_in" in raw or raw.get("type") == "swap":
            return VSCSwapPayload.model_validate(raw)
        return VSCCallPayload.model_validate(raw)
    return raw


class VSCCall(BaseModel):
    """
    Represents a ``vsc.call`` custom_json operation on the Hive blockchain.

    Three payload shapes are supported:

    **Transfer** — ``action == "transfer"``, payload is a dict::

        {
            "net_id": "vsc-mainnet",
            "caller": "hive:v4vapp-test",
            "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
            "action": "transfer",
            "payload": {"amount": "25", "to": "hive:devser.v4vapp", "memo": "user@ln.com"},
            "rc_limit": 1000
        }

    **Execute / swap** — ``action == "execute"``, payload is a *JSON string*::

        {
            "net_id": "vsc-mainnet",
            "caller": "hive:zphrs",
            "contract_id": "vsc1Brvi4YZHLkocYNAFd7Gf1JpsPjzNnv4i45",
            "action": "execute",
            "payload": "{\\"type\\":\\"swap\\", \\"asset_in\\":\\"HIVE\\", ...}",
            "rc_limit": 10000,
            "intents": [{"type": "transfer.allow", "args": {"limit": "26.772", "token": "hive"}}]
        }
    """

    net_id: str = Field(
        "vsc-mainnet", description="The VSC network identifier (e.g. 'vsc-mainnet')."
    )
    caller: str = Field(
        ...,
        description="The calling account in its original network format (e.g. 'hive:<name>').",
    )
    contract_id: str = Field(
        "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
        description="The VSC smart contract address. Defaults to sats on BTC Magi.",
    )
    action: str = Field(
        "transfer", description="The contract action to invoke (e.g. 'transfer', 'execute')."
    )
    payload: Any = Field(
        ..., description="Action-specific payload (VSCCallPayload, VSCSwapPayload, or raw)."
    )
    rc_limit: int = Field(2000, description="Resource credit limit for the operation.")
    intents: List[VSCIntent] = Field(
        default_factory=list,
        description="Optional list of intent declarations (only present on execute calls).",
    )

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def normalise(cls, data: Any) -> Any:
        """Deserialise the payload field."""
        if isinstance(data, dict):
            data = dict(data)
            if "payload" in data:
                data["payload"] = _parse_payload(data["payload"])
        return data

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def from_account(self) -> str:
        """Returns the caller in its original network format (e.g. 'hive:<name>')."""
        return self.caller

    @property
    def to_account(self) -> str:
        """
        Returns the recipient in its original network format.
        Only meaningful for transfer payloads; returns an empty string for execute payloads.
        """
        if isinstance(self.payload, VSCCallPayload) and self.payload.to:
            return self.payload.to
        return ""

    @property
    def amount(self) -> str:
        """Returns the relevant amount string from the payload."""
        if isinstance(self.payload, VSCCallPayload) and self.payload.amount:
            return self.payload.amount
        if isinstance(self.payload, VSCSwapPayload):
            return self.payload.amount_in
        return ""

    @property
    def memo(self) -> str:
        """Returns the memo for transfer payloads, empty string otherwise."""
        if isinstance(self.payload, VSCCallPayload):
            return self.payload.memo
        return ""

    @property
    def log_str(self) -> str:
        if isinstance(self.payload, VSCCallPayload):
            to = f" to={self.to_account}" if self.to_account else ""
            memo = f" memo={self.payload.memo}" if self.payload.memo else ""
            return (
                f"🔗 VSC transfer {self.from_account} → {self.contract_id}"
                f" amount={self.amount}{to}{memo} [{self.net_id}]"
            )
        if isinstance(self.payload, VSCSwapPayload):
            return (
                f"🔗 VSC execute {self.from_account} → {self.contract_id}"
                f" swap {self.payload.asset_in}→{self.payload.asset_out}"
                f" amount_in={self.payload.amount_in} [{self.net_id}]"
            )
        return (
            f"🔗 VSC call {self.from_account} → {self.contract_id}"
            f" action={self.action} [{self.net_id}]"
        )

    @property
    def notification_str(self) -> str:
        return self.log_str

    @property
    def log_extra(self) -> Dict[str, Any]:
        return {"vsc_call": self.model_dump(exclude_none=True, exclude_unset=True)}
