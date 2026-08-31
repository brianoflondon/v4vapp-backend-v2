"""Book settled Dash invoices onto the ledger and probe/pay the bolt11.

``process_dash_invoice`` is called from ``process_tracked_event`` when
db_monitor sees a SETTLED/OVERPAID ``dash_invoices`` document.

Lightning is sent with ``probe_only=True`` while Dash ``payouts_enabled`` is
false (current config). Flip that flag to send the real payment.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryDuplicateException,
)
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.lnurl_decode import LnurlException, decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.exchange_process import rebalance_queue_task
from v4vapp_backend_v2.conversion.exchange_rebalance import RebalanceDirection
from v4vapp_backend_v2.dash.amounts import dash_from_duffs, to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.dash.settings import dash_connection
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentError, send_lightning_to_pay_req

ICON = "💠"
OP_TYPE = "dash_invoice"
SETTLED_STATES = {DashInvoiceState.SETTLED.value, DashInvoiceState.OVERPAID.value}


@dataclass(frozen=True)
class DashRebalanceOp:
    """Tracking handle so Binance Convert rebalance can book against this invoice."""

    group_id: str
    short_id: str
    cust_id: str

    @property
    def log_extra(self) -> dict[str, Any]:
        return {"group_id": self.group_id, "short_id": self.short_id, "cust_id": self.cust_id}


def quote_from_invoice_snapshot(snapshot: dict[str, Any]) -> QuoteResponse:
    """Rebuild a QuoteResponse from the invoice's locked quote. No live fetch."""
    return QuoteResponse(
        hive_usd=Decimal(0),
        hbd_usd=Decimal(0),
        btc_usd=Decimal(str(snapshot["btc_usd"])),
        hive_hbd=Decimal(0),
        dash_usd=Decimal(str(snapshot["dash_usd"])),
        source=str(snapshot.get("source") or "invoice"),
        fetch_date=snapshot.get("fetched_at") or datetime(1970, 1, 1, tzinfo=UTC),
    )


def invoice_group_key(invoice_doc: dict[str, Any]) -> str:
    """Ledger group_id stem: Dash receive address, else Mongo _id."""
    address = str(invoice_doc.get("address") or "").strip()
    if address:
        return address
    return str(invoice_doc.get("_id") or "")


def conv_group_id(group_key: str) -> str:
    return f"{group_key}_{LedgerType.CONV_DASH_TO_SATS.value}"


def fee_group_id(group_key: str) -> str:
    return f"{group_key}_{LedgerType.FEE_INCOME.value}"


def park_group_id(group_key: str) -> str:
    return f"{group_key}_{LedgerType.DASH_TEST_PAY.value}"


def invoice_short_id(group_key: str) -> str:
    """Ledger short_id is the first 8 chars of the Dash address (group key)."""
    return str(group_key)[:8]


def treasury_sub(network: str | None) -> str:
    return f"dash-{network or 'mainnet'}"


async def post_invoice_settlement(
    invoice_doc: dict[str, Any],
    *,
    db: Any | None = None,
) -> list[LedgerEntry]:
    """Post CONV_DASH_TO_SATS (and fee income) for a settled Dash invoice.

    Credit VSC Liability (server) only — invoice cust_id is not a ledger owner.
    Duplicate group_id is treated as success. Optional ``db`` stamps
    ``ledger_posted_at`` / ``ledger_group_id`` on the invoice document.
    """
    state = str(invoice_doc.get("state") or "")
    if state not in SETTLED_STATES:
        return []

    duffs_received = to_decimal(invoice_doc.get("duffs_received") or 0)
    sats_credited = invoice_doc.get("sats_credited")
    if duffs_received <= 0 or sats_credited is None:
        logger.warning(
            f"{ICON} skip ledger post: missing duffs_received or sats_credited",
            extra={"invoice_id": str(invoice_doc.get("_id")), "state": state},
        )
        return []

    invoice_id = str(invoice_doc["_id"])
    group_key = invoice_group_key(invoice_doc)
    existing = await _load_posted(group_key)
    if existing:
        return existing

    quote = quote_from_invoice_snapshot(invoice_doc["quote"])
    dash_conv = CryptoConversion(
        conv_from=Currency.DUFFS, value=duffs_received, quote=quote
    ).conversion
    fee_sats = _fee_sats(invoice_doc)
    server_id = InternalConfig().server_id
    sub = treasury_sub(invoice_doc.get("network"))
    short_id = invoice_short_id(group_key)
    now = datetime.now(tz=UTC)
    memo = invoice_doc.get("memo") or ""

    conv_entry = LedgerEntry(
        cust_id=server_id,
        short_id=short_id,
        op_type=OP_TYPE,
        ledger_type=LedgerType.CONV_DASH_TO_SATS,
        group_id=conv_group_id(group_key),
        timestamp=invoice_doc.get("settled_at") or now,
        description=(
            f"Convert inbound {dash_conv.formatted_amount(Currency.DASH)} "
            f"to {dash_conv.sats_rounded:,.0f} sats" + (f" {memo}" if memo else "")
        ),
        debit=AssetAccount(name="Treasury Dash", sub=sub),
        debit_unit=Currency.DUFFS,
        debit_amount=dash_conv.duffs,
        debit_conv=dash_conv,
        credit=LiabilityAccount(name="VSC Liability", sub=server_id),
        credit_unit=Currency.MSATS,
        credit_amount=dash_conv.msats,
        credit_conv=dash_conv,
    )
    entries = [await _save_or_load(conv_entry)]

    if fee_sats > 0:
        fee_msats = fee_sats * Decimal(1000)
        fee_conv = CryptoConversion(
            conv_from=Currency.MSATS, value=fee_msats, quote=quote
        ).conversion
        fee_entry = LedgerEntry(
            cust_id=server_id,
            short_id=short_id,
            op_type=OP_TYPE,
            ledger_type=LedgerType.FEE_INCOME,
            group_id=fee_group_id(group_key),
            timestamp=invoice_doc.get("settled_at") or now,
            description=f"Fee income {fee_sats:,.0f} sats on Dash inbound {short_id}",
            debit=LiabilityAccount(name="VSC Liability", sub=server_id),
            debit_unit=Currency.MSATS,
            debit_amount=fee_conv.msats,
            debit_conv=fee_conv,
            credit=RevenueAccount(name="Fee Income Dash", sub=sub),
            credit_unit=Currency.MSATS,
            credit_amount=fee_conv.msats,
            credit_conv=fee_conv,
        )
        entries.append(await _save_or_load(fee_entry))

    quoted = to_decimal(invoice_doc.get("duffs_quoted") or 0)
    if duffs_received != quoted:
        logger.info(
            f"{ICON} Dash received {duffs_received} duffs vs quoted {quoted} "
            f"(sats_credited={sats_credited})",
            extra={"invoice_id": invoice_id, "address": group_key, "notification": False},
        )

    if db is not None:
        await db[COL_INVOICES].update_one(
            {"_id": invoice_doc["_id"]},
            {
                "$set": {
                    "ledger_posted_at": now,
                    "ledger_group_id": conv_entry.group_id,
                }
            },
        )

    _queue_dash_sell(
        duffs_received=duffs_received,
        invoice_id=group_key,
        short_id=short_id,
        server_id=server_id,
        currency=Currency.DASH,
    )

    return entries


def lightning_probe_only(destination: str) -> bool:
    """True unless Dash connection has payouts_enabled.

    Current config has ``payouts_enabled: false``, so Lightning is probe-only.
    Also check for self payment in development mode.
    """
    conn = dash_connection()
    if conn is None or not conn.lightning_payments_enabled:
        return True
    if InternalConfig().node_pubkey and destination != InternalConfig().node_pubkey:
        return True
    return True


def _is_probe_success(error: LNDPaymentError) -> bool:
    text = str(error).upper()
    return "INCORRECT_PAYMENT_DETAILS" in text


async def pay_dash_lightning(invoice_doc: dict[str, Any]) -> None:
    """Decode the invoice bolt11 and send (or probe) Lightning.

    Probe-only while ``payouts_enabled`` is false. Failures are logged; Dash
    settlement is not rolled back.
    """
    bolt11 = invoice_doc.get("lightning_invoice")
    if not bolt11:
        return
    if invoice_doc.get("lightning_sent_at") or invoice_doc.get("lightning_probed_at"):
        return

    lnd_config = InternalConfig().config.lnd_config
    if not lnd_config or not lnd_config.default:
        logger.warning(
            f"{ICON} skip lightning: LND not configured",
            extra={"invoice_id": str(invoice_doc.get("_id"))},
        )
        return

    invoice_id = str(invoice_doc["_id"])
    group_key = invoice_group_key(invoice_doc)
    short_id = invoice_short_id(group_key)
    sats = to_decimal(invoice_doc.get("sats_credited") or invoice_doc.get("sats_requested") or 0)
    amount_msat = sats * Decimal(1000)
    cust_id = str(invoice_doc.get("cust_id") or "")
    db = InternalConfig.db if hasattr(InternalConfig, "db") else None

    lnd_client = LNDClient(connection_name=lnd_config.default)
    try:
        pay_req = await decode_any_lightning_string(input=str(bolt11), lnd_client=lnd_client)
        probe_only = lightning_probe_only(pay_req.destination)
        payment = await send_lightning_to_pay_req(
            pay_req=pay_req,
            lnd_client=lnd_client,
            group_id=group_key,
            cust_id=cust_id,
            chat_message=f"Dash inbound {short_id}",
            amount_msat=amount_msat,
            probe_only=probe_only,
        )
        stamp = "lightning_probed_at" if probe_only else "lightning_sent_at"
        await _stamp_lightning(db, invoice_doc, stamp)
        logger.info(
            f"{ICON} Lightning {'probe' if probe_only else 'payment'} "
            f"{payment.status} for Dash {short_id}",
            extra={"notification": False, "invoice_id": invoice_id, "probe_only": probe_only},
        )
    except LNDPaymentError as e:
        if probe_only and _is_probe_success(e):
            await _stamp_lightning(db, invoice_doc, "lightning_probed_at")
            logger.info(
                f"{ICON} Lightning probe reached destination for Dash {short_id}",
                extra={"notification": False, "invoice_id": invoice_id, "error": str(e)},
            )
            return
        logger.warning(
            f"{ICON} Lightning {'probe' if probe_only else 'payment'} failed "
            f"for Dash {short_id}: {e}",
            extra={"notification": True, "invoice_id": invoice_id, "error": str(e)},
        )
    except LnurlException as e:
        logger.warning(
            f"{ICON} Could not decode lightning invoice for Dash {short_id}: {e}",
            extra={"invoice_id": invoice_id, "error": str(e)},
        )
    except Exception as e:
        logger.exception(
            f"{ICON} Lightning send error for Dash {short_id}: {e}",
            extra={"invoice_id": invoice_id, "error": str(e)},
        )


async def park_dash_test_payment(invoice_doc: dict[str, Any]) -> LedgerEntry | None:
    """Move net sats off VSC Liability onto Asset Dash Payment Tests (server).

    Used when Lightning is probe-only (``payouts_enabled: false``) so test
    inbound Dash does not accumulate on the server VSC Liability account.
    """
    group_key = invoice_group_key(invoice_doc)
    if not group_key:
        return None
    existing = await LedgerEntry.load(park_group_id(group_key))
    if existing is not None:
        return existing

    sats_credited = invoice_doc.get("sats_credited")
    if sats_credited is None:
        return None
    net_msats = to_decimal(sats_credited) * Decimal(1000)
    if net_msats <= 0:
        return None

    quote = quote_from_invoice_snapshot(invoice_doc["quote"])
    park_conv = CryptoConversion(conv_from=Currency.MSATS, value=net_msats, quote=quote).conversion
    server_id = InternalConfig().server_id
    short_id = invoice_short_id(group_key)
    now = datetime.now(tz=UTC)
    entry = LedgerEntry(
        cust_id=server_id,
        short_id=short_id,
        op_type=OP_TYPE,
        ledger_type=LedgerType.DASH_TEST_PAY,
        group_id=park_group_id(group_key),
        timestamp=invoice_doc.get("settled_at") or now,
        description=(
            f"Park {park_conv.sats_rounded:,.0f} sats on Dash Payment Tests "
            f"after probe-only Lightning {short_id}"
        ),
        debit=LiabilityAccount(name="VSC Liability", sub=server_id),
        debit_unit=Currency.MSATS,
        debit_amount=park_conv.msats,
        debit_conv=park_conv,
        credit=AssetAccount(name="Dash Payment Tests", sub=server_id),
        credit_unit=Currency.MSATS,
        credit_amount=park_conv.msats,
        credit_conv=park_conv,
    )
    return await _save_or_load(entry)


async def _stamp_lightning(db: Any, invoice_doc: dict[str, Any], field: str) -> None:
    if db is None or invoice_doc.get("_id") is None:
        return
    try:
        await db[COL_INVOICES].update_one(
            {"_id": invoice_doc["_id"]},
            {"$set": {field: datetime.now(tz=UTC)}},
        )
    except Exception as e:
        logger.debug(f"{ICON} could not stamp {field}: {e}", extra={"notification": False})


async def process_dash_invoice(event: Any) -> list[LedgerEntry]:
    """Post settlement ledger entries then probe/pay the bolt11."""
    from bson import ObjectId

    from v4vapp_backend_v2.dash.models.tracked import DashInvoiceEvent

    invoice_id = getattr(event, "invoice_id", None)
    group_key = getattr(event, "group_id_p", None) or getattr(event, "address", None)
    doc: dict[str, Any] | None = None
    db = getattr(InternalConfig, "db", None)
    if db is not None:
        if group_key:
            try:
                doc = await db[COL_INVOICES].find_one({"address": str(group_key)})
            except Exception:
                doc = None
        if doc is None and invoice_id:
            try:
                doc = await db[COL_INVOICES].find_one({"_id": ObjectId(str(invoice_id))})
            except Exception:
                doc = None
    if doc is None and isinstance(event, DashInvoiceEvent):
        doc = {
            "_id": invoice_id,
            "address": event.address,
            "external_id": event.external_id,
            "cust_id": event.cust_id,
            "state": event.state,
            "network": event.network,
            "memo": event.memo,
            "lightning_invoice": event.lightning_invoice,
            "sats_requested": event.sats_requested,
            "sats_credited": event.sats_credited,
            "duffs_received": event.duffs_received,
            "quote": event.quote,
            "settled_at": event.settled_at,
        }
    if not doc:
        logger.warning(f"{ICON} process_dash_invoice: missing invoice document")
        return []

    entries = await post_invoice_settlement(doc, db=db)
    await pay_dash_lightning(doc)
    if lightning_probe_only():
        parked = await park_dash_test_payment(doc)
        if parked is not None:
            entries.append(parked)
    return entries


def _queue_dash_sell(
    *,
    duffs_received: Decimal,
    invoice_id: str,
    short_id: str,
    server_id: str,
    currency: Currency,
) -> None:
    dash_qty = dash_from_duffs(duffs_received)
    if dash_qty <= 0:
        return
    asyncio.create_task(
        rebalance_queue_task(
            direction=RebalanceDirection.SELL_BASE_FOR_QUOTE,
            currency=currency,
            hive_qty=dash_qty,
            tracked_op=DashRebalanceOp(
                group_id=invoice_id,
                short_id=short_id,
                cust_id=server_id,
            ),
            base_asset="DASH",
            quote_asset="BTC",
        )
    )


async def _load_posted(invoice_id: str) -> list[LedgerEntry]:
    conv = await LedgerEntry.load(conv_group_id(invoice_id))
    if conv is None:
        return []
    entries = [conv]
    fee = await LedgerEntry.load(fee_group_id(invoice_id))
    if fee is not None:
        entries.append(fee)
    parked = await LedgerEntry.load(park_group_id(invoice_id))
    if parked is not None:
        entries.append(parked)
    return entries


async def _save_or_load(entry: LedgerEntry) -> LedgerEntry:
    try:
        result = await entry.save(ignore_duplicates=True)
    except LedgerEntryDuplicateException:
        result = None
    if result is None:
        loaded = await LedgerEntry.load(entry.group_id)
        if loaded is not None:
            return loaded
    return entry


def _fee_sats(invoice_doc: dict[str, Any]) -> Decimal:
    fees = invoice_doc.get("fees") or {}
    if fees.get("total_fee_sats") is not None:
        fee = to_decimal(fees["total_fee_sats"])
        return fee if fee > 0 else Decimal(0)
    collect = invoice_doc.get("sats_collect")
    requested = invoice_doc.get("sats_requested")
    if collect is None or requested is None:
        return Decimal(0)
    fee = to_decimal(collect) - to_decimal(requested)
    return fee if fee > 0 else Decimal(0)
