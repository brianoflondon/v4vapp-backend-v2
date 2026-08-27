"""Book settled Dash invoices onto the ledger.

Callable from process_tracked_event later. Does not pay bolt11 and does not
hook the Dash watcher.
"""

from __future__ import annotations

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
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.dash.amounts import to_decimal
from v4vapp_backend_v2.dash.collections import COL_INVOICES
from v4vapp_backend_v2.dash.models.invoice import DashInvoiceState
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency

ICON = "💠"
OP_TYPE = "dash_invoice"
SETTLED_STATES = {DashInvoiceState.SETTLED.value, DashInvoiceState.OVERPAID.value}


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


def conv_group_id(invoice_id: str) -> str:
    return f"{invoice_id}_{LedgerType.CONV_DASH_TO_SATS.value}"


def fee_group_id(invoice_id: str) -> str:
    return f"{invoice_id}_{LedgerType.FEE_INCOME.value}"


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
    existing = await _load_posted(invoice_id)
    if existing:
        return existing

    quote = quote_from_invoice_snapshot(invoice_doc["quote"])
    dash_conv = CryptoConversion(
        conv_from=Currency.DUFFS, value=duffs_received, quote=quote
    ).conversion
    fee_sats = _fee_sats(invoice_doc)
    server_id = InternalConfig().server_id
    sub = treasury_sub(invoice_doc.get("network"))
    short_id = str(invoice_doc.get("external_id") or invoice_id)
    now = datetime.now(tz=UTC)
    memo = invoice_doc.get("memo") or ""

    conv_entry = LedgerEntry(
        cust_id=server_id,
        short_id=short_id,
        op_type=OP_TYPE,
        ledger_type=LedgerType.CONV_DASH_TO_SATS,
        group_id=conv_group_id(invoice_id),
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
            group_id=fee_group_id(invoice_id),
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
            extra={"invoice_id": invoice_id, "notification": False},
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

    return entries


async def _load_posted(invoice_id: str) -> list[LedgerEntry]:
    conv = await LedgerEntry.load(conv_group_id(invoice_id))
    if conv is None:
        return []
    entries = [conv]
    fee = await LedgerEntry.load(fee_group_id(invoice_id))
    if fee is not None:
        entries.append(fee)
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
