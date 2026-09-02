from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Self

from pydantic import BaseModel, Field, field_serializer, model_validator

from v4vapp_backend_v2.config.setup import DashNetwork
from v4vapp_backend_v2.dash.amounts import json_amount, to_decimal
from v4vapp_backend_v2.dash.models.wallet import Derivation


class DashPayoutState(StrEnum):
    PENDING = "PENDING"
    BUILDING = "BUILDING"
    BROADCAST = "BROADCAST"
    LOCKED = "LOCKED"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"


class PayoutCreate(BaseModel):
    """Pay Dash from the watch wallet to a P2PKH address."""

    external_id: str = Field(
        min_length=1,
        max_length=128,
        title="Caller unique id",
        description=(
            "Your unique correlation key (1–128 characters). Same id + same "
            "`address` + same amount is **idempotent** (HTTP 200, same tx). "
            "Reusing the id with different parameters returns HTTP 409."
        ),
        examples=["payout:alice:42"],
    )
    address: str = Field(
        min_length=26,
        max_length=64,
        title="Destination Dash address",
        description=(
            "P2PKH destination. Mainnet `X…`, testnet/regtest `y…`. "
            "Must be valid on the configured Dash network. Do not send to "
            "one of this service's own **OPEN** invoice addresses."
        ),
        examples=["yRd4FhXfVGHXpsuZXPNkMrfD9GVj46pnjt"],
    )
    duffs: Decimal | None = Field(
        default=None,
        gt=0,
        title="Amount in duffs",
        description=(
            "Integer duffs to send (1 DASH = 100,000,000 duffs). Provide "
            "**exactly one** of `duffs` or `sats`. This is the destination "
            "amount unless `subtract_fee` is true."
        ),
        examples=[10_000_000],
    )
    sats: Decimal | None = Field(
        default=None,
        gt=0,
        title="Amount in sats",
        description=(
            "If set instead of `duffs`, converted to duffs at a **fresh** "
            "DASH/BTC quote (ceil, same as invoices). Provide exactly one of "
            "`duffs` or `sats`."
        ),
    )
    subtract_fee: bool = Field(
        default=False,
        title="Subtract miner fee from destination",
        description=(
            "If false (default), the destination receives `duffs`/`sats` and "
            "this wallet pays the miner fee on top. If true, the miner fee is "
            "taken from the amount so the destination receives less."
        ),
    )
    cust_id: str | None = Field(
        default=None,
        title="Customer id",
        description="Optional V4V / Hive customer id stored on the payout.",
    )
    memo: str | None = Field(
        default=None,
        max_length=300,
        title="Note",
        description="Optional free-text note, max 300 characters. Not placed on-chain.",
    )

    @model_validator(mode="after")
    def exactly_one_amount(self) -> Self:
        has_duffs = self.duffs is not None
        has_sats = self.sats is not None
        if has_duffs == has_sats:
            raise ValueError("provide exactly one of duffs or sats")
        return self


class PayoutInputOut(BaseModel):
    txid: str
    vout: int
    duffs: Decimal
    address: str
    path: str

    @field_serializer("duffs", when_used="json")
    def _json_duffs(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class PayoutOut(BaseModel):
    payout_id: str
    external_id: str
    state: DashPayoutState
    network: DashNetwork
    address: str
    duffs: Decimal
    dash_amount: str
    sats: Decimal | None = None
    fee_duffs: Decimal | None = None
    change_duffs: Decimal | None = None
    change_address: str | None = None
    change_derivation: Derivation | None = None
    txid: str | None = None
    subtract_fee: bool = False
    inputs: list[PayoutInputOut] = Field(default_factory=list)
    error: str | None = None
    cust_id: str | None = None
    memo: str | None = None
    created_at: datetime
    broadcast_at: datetime | None = None
    confirmed_at: datetime | None = None

    @field_serializer("duffs", "sats", "fee_duffs", "change_duffs", when_used="json")
    def _json_amount(self, value: Decimal | None) -> int | str | None:
        return json_amount(value)


def doc_to_payout(doc: dict[str, Any]) -> PayoutOut:
    change_der = doc.get("change_derivation")
    return PayoutOut(
        payout_id=str(doc["_id"]),
        external_id=doc["external_id"],
        state=DashPayoutState(doc["state"]),
        network=doc["network"],
        address=doc["address"],
        duffs=to_decimal(doc["duffs"]),
        dash_amount=doc["dash_amount"],
        sats=to_decimal(doc["sats"]) if doc.get("sats") is not None else None,
        fee_duffs=to_decimal(doc["fee_duffs"]) if doc.get("fee_duffs") is not None else None,
        change_duffs=(
            to_decimal(doc["change_duffs"]) if doc.get("change_duffs") is not None else None
        ),
        change_address=doc.get("change_address"),
        change_derivation=Derivation.model_validate(change_der) if change_der else None,
        txid=doc.get("txid"),
        subtract_fee=bool(doc.get("subtract_fee")),
        inputs=[PayoutInputOut.model_validate(row) for row in doc.get("inputs") or []],
        error=doc.get("error"),
        cust_id=doc.get("cust_id"),
        memo=doc.get("memo"),
        created_at=doc["created_at"],
        broadcast_at=doc.get("broadcast_at"),
        confirmed_at=doc.get("confirmed_at"),
    )
