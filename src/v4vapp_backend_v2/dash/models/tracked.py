"""Tracked Dash invoice for db_monitor / overwatch / process_tracked_event."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from bson import ObjectId
from pydantic import ConfigDict, Field, computed_field
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.results import UpdateResult

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.dash.amounts import to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency

ICON = "💠"
OP_TYPE = "dash_invoice"
SETTLED_STATES = {DashInvoiceState.SETTLED.value, DashInvoiceState.OVERPAID.value}


class DashInvoiceEvent(TrackedBaseModel):
    """Thin tracked wrapper around a settled ``dash_invoices`` document."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    invoice_id: str = Field(..., description="Mongo _id of the dash invoice")
    external_id: str = ""
    state: str = ""
    lightning_invoice: str | None = None
    cust_id: str | None = None
    memo: str | None = None
    network: str | None = None
    duffs_received: Decimal = Decimal(0)
    sats_credited: Decimal | None = None
    sats_requested: Decimal | None = None
    quote: dict[str, Any] | None = None
    settled_at: datetime | None = None
    created_at: datetime | None = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))

    def __init__(self, **data: Any) -> None:
        if data.get("invoice_id") is None and data.get("_id") is not None:
            data["invoice_id"] = str(data["_id"])
        super().__init__(**data)
        self.timestamp = self.settled_at or self.created_at or datetime.now(tz=UTC)
        if self.quote and (self.conv is None or self.conv.is_unset()):
            try:
                from v4vapp_backend_v2.process.process_dash import quote_from_invoice_snapshot

                duffs = to_decimal(self.duffs_received or 0)
                if duffs > 0:
                    quote = quote_from_invoice_snapshot(self.quote)
                    self.conv = CryptoConversion(
                        conv_from=Currency.DUFFS, value=duffs, quote=quote
                    ).conversion
            except Exception:
                logger.debug(
                    "DashInvoiceEvent conv from quote failed", extra={"notification": False}
                )

    @property
    def collection_name(self) -> str:
        return COL_INVOICES

    @classmethod
    def collection(cls) -> AsyncCollection:
        return InternalConfig.db[COL_INVOICES]

    @computed_field
    @property
    def group_id(self) -> str:
        return self.invoice_id

    @property
    def group_id_p(self) -> str:
        return self.invoice_id

    @property
    def group_id_query(self) -> dict[str, Any]:
        try:
            return {"_id": ObjectId(self.invoice_id)}
        except Exception:
            return {"invoice_id": self.invoice_id}

    @computed_field
    @property
    def short_id(self) -> str:
        return self.invoice_id[:8]

    @property
    def short_id_p(self) -> str:
        return self.short_id

    @computed_field
    @property
    def op_type(self) -> str:
        return OP_TYPE

    @property
    def from_account(self) -> str:
        return self.cust_id or ""

    @property
    def log_str(self) -> str:
        sats = self.sats_credited if self.sats_credited is not None else self.sats_requested
        sats_txt = f"{to_decimal(sats):,.0f} sats" if sats is not None else "sats n/a"
        ln = " + ln" if self.lightning_invoice else ""
        return (
            f"{ICON} Dash invoice {self.short_id} {self.state} "
            f"{to_decimal(self.duffs_received):,.0f} duffs → {sats_txt}{ln}"
        )

    @property
    def log_extra(self) -> dict[str, Any]:
        return {
            "invoice_id": self.invoice_id,
            "external_id": self.external_id,
            "state": self.state,
            "cust_id": self.cust_id,
            "group_id": self.invoice_id,
            "short_id": self.short_id,
        }

    async def save(
        self,
        exclude_unset: bool = False,
        exclude_none: bool = True,
        mongo_kwargs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> UpdateResult:
        """Only stamp processing metadata; never rewrite the invoice document."""
        from v4vapp_backend_v2.database.db_retry import mongo_call

        _ = (exclude_unset, exclude_none, kwargs)
        if mongo_kwargs is None:
            mongo_kwargs = {}
        now = datetime.now(tz=UTC)
        update = {"process_time": self.process_time, "tracked_at": now}
        return await mongo_call(
            lambda: InternalConfig.db[COL_INVOICES].update_one(
                self.group_id_query, {"$set": update}, **mongo_kwargs
            ),
            error_code=f"db_save_error_{COL_INVOICES}",
            context=f"{COL_INVOICES}:{self.group_id_p}",
        )
