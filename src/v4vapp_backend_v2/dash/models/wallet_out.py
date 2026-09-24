from decimal import Decimal

from pydantic import BaseModel, Field, field_serializer

from v4vapp_backend_v2.config.setup import DashNetwork
from v4vapp_backend_v2.dash.amounts import json_amount


class WalletBalance(BaseModel):
    duffs_total: Decimal = Field(description="Sum of all watched UTXOs (duffs).")
    duffs_spendable: Decimal = Field(
        description=(
            "UTXOs eligible to spend: InstantSend/ChainLock, or enough "
            "confirmations. Excludes OPEN/DETECTED invoice addresses."
        )
    )
    duffs_incoming: Decimal = Field(
        description="On OPEN/DETECTED invoice addresses (in-flight customer payments)."
    )
    duffs_unconfirmed: Decimal = Field(description="0-conf UTXOs not InstantSend-locked.")
    dash_total: str
    dash_spendable: str

    @field_serializer("duffs_total", "duffs_spendable", "duffs_incoming", "duffs_unconfirmed")
    def _json_duffs(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class WalletHd(BaseModel):
    fingerprint: str | None = None
    next_receive_index: int = 0
    next_change_index: int = 0
    descriptor_range_end: int = 0


class WalletUtxo(BaseModel):
    txid: str
    vout: int
    address: str
    duffs: Decimal
    confirmations: int
    instantlock: bool = False
    chainlock: bool = False
    spendable: bool = False
    incoming: bool = False

    @field_serializer("duffs", when_used="json")
    def _json_duffs(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class WalletOut(BaseModel):
    network: DashNetwork
    payouts_enabled: bool
    can_sign: bool = Field(
        description="True when payouts_enabled and a mnemonic file matching the xpub is present."
    )
    balance: WalletBalance
    utxo_count: int
    spendable_utxo_count: int
    hd: WalletHd
    dashd: dict | None = None
    utxos: list[WalletUtxo] | None = Field(
        default=None,
        description="Present only when `include_utxos=true`.",
    )
