from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, Field, field_serializer

from v4vapp_backend_v2.config.setup import DashNetwork, DashSettlePolicy
from v4vapp_backend_v2.dash.amounts import json_amount, to_decimal
from v4vapp_backend_v2.dash.models.wallet import Derivation
from v4vapp_backend_v2.dash.settings import wallet_sender


class DashInvoiceState(StrEnum):
    OPEN = "OPEN"
    DETECTED = "DETECTED"
    SETTLED = "SETTLED"
    UNDERPAID = "UNDERPAID"
    OVERPAID = "OVERPAID"
    EXPIRED = "EXPIRED"
    CANCELED = "CANCELED"


class InvoiceFeesOut(BaseModel):
    conv_fee_percent: str
    conv_fee_base_sats: Decimal
    conv_fee_sats: Decimal
    routing_fee_sats: Decimal
    total_fee_sats: Decimal
    sats_collect: Decimal

    @field_serializer(
        "conv_fee_base_sats",
        "conv_fee_sats",
        "routing_fee_sats",
        "total_fee_sats",
        "sats_collect",
        when_used="json",
    )
    def _json_sats(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class InvoiceCreate(BaseModel):
    external_id: str = Field(min_length=1, max_length=128)
    sats: Decimal = Field(gt=0)
    expires_in_s: int = Field(ge=60, le=86_400)
    lightning_invoice: str | None = Field(
        default=None,
        min_length=10,
        max_length=4096,
        description="Bolt11 the Dash payment is meant to settle (from api-ext)",
    )
    cust_id: str | None = None
    memo: str | None = Field(default=None, max_length=300)
    min_confirmations: int | None = Field(default=None, ge=1, le=100)


class QuoteSnapshot(BaseModel):
    source: str
    fetched_at: str
    btc_usd: str
    dash_usd: str
    dash_btc: str
    sats_per_dash: str
    ttl_s: int


class InvoicePolicy(BaseModel):
    settle_policy: DashSettlePolicy
    underpay_tolerance_duffs: Decimal
    underpay_tolerance_bps: int
    min_confirmations: int
    accept_instantsend: bool
    accept_chainlock: bool

    @field_serializer("underpay_tolerance_duffs", when_used="json")
    def _json_duffs(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class InvoiceTx(BaseModel):
    txid: str
    vout: int
    duffs: Decimal
    confirmations: int
    instantlock: bool
    chainlock: bool
    detected_at: datetime

    @field_serializer("duffs", when_used="json")
    def _json_duffs(self, value: Decimal) -> int | str:
        return json_amount(value)  # type: ignore[return-value]


class InvoiceOut(BaseModel):
    invoice_id: str
    external_id: str
    state: DashInvoiceState
    address: str
    uri_bip21: str
    uri_dashpay: str
    network: DashNetwork
    sats_requested: Decimal
    sats_collect: Decimal | None = None
    duffs_quoted: Decimal
    dash_quoted: str
    fees: InvoiceFeesOut | None = None
    duffs_received: Decimal = Decimal(0)
    sats_credited: Decimal | None = None
    expires_at: datetime
    settle_deadline_at: datetime
    created_at: datetime
    first_seen_at: datetime | None = None
    detected_at: datetime | None = None
    settled_at: datetime | None = None
    canceled_at: datetime | None = None
    expired: bool = False
    late_payment: bool = False
    quote: QuoteSnapshot
    derivation: Derivation
    policy: InvoicePolicy
    txids: list[InvoiceTx] = Field(default_factory=list)
    cust_id: str | None = None
    memo: str | None = None
    lightning_invoice: str | None = None

    @field_serializer(
        "sats_requested",
        "sats_collect",
        "duffs_quoted",
        "duffs_received",
        "sats_credited",
        when_used="json",
    )
    def _json_amount(self, value: Decimal | None) -> int | str | None:
        return json_amount(value)


class InvoiceListOut(BaseModel):
    items: list[InvoiceOut]
    next_cursor: str | None = None


def payment_uri_bip21(address: str, dash_quoted: str) -> str:
    """BIP21 payment URI: `dash:<address>?amount=<amount>`."""
    return f"dash:{address}?amount={dash_quoted}"


def payment_uri_dashpay(
    address: str,
    dash_quoted: str,
    sender: str | None = None,
) -> str:
    """Dash iOS / DashPay wallet payment URL (`dashwallet://` scheme).

        dashwallet://pay=<address>&amount=<amount>&sender=<sender>

    `sender` is only the app name shown to the payer. Settlement is on-chain;
    this API does not handle the wallet's optional `://callback=payack` return.
    """
    label = wallet_sender() if sender is None else sender
    return (
        f"dashwallet://pay={quote(address, safe='')}"
        f"&amount={quote(dash_quoted, safe='')}"
        f"&sender={quote(label, safe='')}"
    )


def invoice_payment_uris(
    address: str,
    dash_quoted: str,
    sender: str | None = None,
) -> tuple[str, str]:
    return (
        payment_uri_bip21(address, dash_quoted),
        payment_uri_dashpay(address, dash_quoted, sender=sender),
    )


def doc_to_out(doc: dict[str, Any]) -> InvoiceOut:
    quote = doc["quote"]
    der = doc["derivation"]
    policy = doc["policy"]
    state = DashInvoiceState(doc["state"])
    address = doc["address"]
    dash_quoted = doc["dash_quoted"]
    uri_bip21, uri_dashpay = invoice_payment_uris(address, dash_quoted)
    return InvoiceOut(
        invoice_id=str(doc["_id"]),
        external_id=doc["external_id"],
        state=state,
        address=address,
        uri_bip21=uri_bip21,
        uri_dashpay=uri_dashpay,
        network=doc["network"],
        sats_requested=to_decimal(doc["sats_requested"]),
        sats_collect=to_decimal(doc["sats_collect"])
        if doc.get("sats_collect") is not None
        else None,
        duffs_quoted=to_decimal(doc["duffs_quoted"]),
        dash_quoted=dash_quoted,
        fees=InvoiceFeesOut.model_validate(doc["fees"]) if doc.get("fees") else None,
        duffs_received=to_decimal(doc.get("duffs_received") or 0),
        sats_credited=(
            to_decimal(doc["sats_credited"]) if doc.get("sats_credited") is not None else None
        ),
        expires_at=doc["expires_at"],
        settle_deadline_at=doc["settle_deadline_at"],
        created_at=doc["created_at"],
        first_seen_at=doc.get("first_seen_at"),
        detected_at=doc.get("detected_at"),
        settled_at=doc.get("settled_at"),
        canceled_at=doc.get("canceled_at"),
        expired=state == DashInvoiceState.EXPIRED,
        late_payment=bool(doc.get("late_payment")),
        quote=QuoteSnapshot(
            source=quote["source"],
            fetched_at=quote["fetched_at"],
            btc_usd=str(quote["btc_usd"]),
            dash_usd=str(quote["dash_usd"]),
            dash_btc=str(quote["dash_btc"]),
            sats_per_dash=str(quote["sats_per_dash"]),
            ttl_s=int(quote["ttl_s"]),
        ),
        derivation=Derivation(
            account=int(der["account"]),
            change=int(der["change"]),
            index=int(der["index"]),
            path=der["path"],
        ),
        policy=InvoicePolicy(
            settle_policy=policy["settle_policy"],
            underpay_tolerance_duffs=to_decimal(policy["underpay_tolerance_duffs"]),
            underpay_tolerance_bps=int(policy["underpay_tolerance_bps"]),
            min_confirmations=int(policy["min_confirmations"]),
            accept_instantsend=bool(policy["accept_instantsend"]),
            accept_chainlock=bool(policy["accept_chainlock"]),
        ),
        txids=[InvoiceTx.model_validate(tx) for tx in doc.get("txids") or []],
        cust_id=doc.get("cust_id"),
        memo=doc.get("memo"),
        lightning_invoice=doc.get("lightning_invoice"),
    )
