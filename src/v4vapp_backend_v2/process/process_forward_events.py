from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import AssetAccount, RevenueAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry
from v4vapp_backend_v2.accounting.ledger_type_class import LedgerType
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.models.tracked_forward_models import TrackedForwardEvent


async def process_forward(
    tracked_forward_event: TrackedForwardEvent,
) -> List[LedgerEntry]:
    """
    Marks the given TrackedForwardEvent as included on the ledger by updating its
    `included_on_ledger` field to True and setting the `ledger_entry_id`.

    Args:
        tracked_forward_event (TrackedForwardEvent): The HTLC forward event to update.

    Returns:
        None
    """

    # Right now the system will store every forward event as a unique ledger entry.
    # In future, we may want to batch these into single ledger entries per time period.
    total_fee, pending_for_ledger = await TrackedForwardEvent.pending_for_ledger_inclusion()
    for event in pending_for_ledger:
        logger.info(event.log_str, extra={"notification": False, **event.log_extra})
        ledger_entry = await add_forward_to_ledger(event)
        event.ledger_entry_id = ledger_entry.group_id
        event.included_on_ledger = True
        await event.save()
    if total_fee > Decimal("500"):
        pass

    return []


async def add_forward_to_ledger(
    forward_event: TrackedForwardEvent,
) -> LedgerEntry:
    """
    Creates ledger entries for the given TrackedForwardEvent representing
    the routing fee earned from the HTLC forward.

    Args:
        tracked_forward_event (TrackedForwardEvent): The HTLC forward event to process.

    Returns:
        LedgerEntry: The created ledger entry for the routing fee.
    """
    # Create a ledger entry for the routing fee

    msat_fee = (forward_event.fee or Decimal("0")) * Decimal("1000")
    node = InternalConfig().node_name
    ledger_type = LedgerType.ROUTING_FEE
    if datetime.now(tz=timezone.utc) - forward_event.timestamp > timedelta(minutes=5):
        quote = await TrackedBaseModel.nearest_quote(forward_event.timestamp)
    else:
        await TrackedBaseModel.update_quote()
        quote = TrackedBaseModel.last_quote

    fee_conv = CryptoConversion(conv_from=Currency.MSATS, value=msat_fee, quote=quote).conversion

    ledger_entry = LedgerEntry(
        cust_id=forward_event.from_channel,
        short_id=forward_event.short_id,
        group_id=f"{forward_event.group_id}_{ledger_type.value}",
        ledger_type=ledger_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=(
            f"Routing fee {forward_event.fee:,.3f} sats "
            f"{forward_event.fee_ppm} ppm "
            f"{forward_event.from_channel} -> {forward_event.to_channel}"
        ),
        debit=AssetAccount(
            name="External Lightning Payments",
            sub=node,
        ),
        debit_amount=msat_fee,
        debit_unit=Currency.MSATS,
        debit_conv=fee_conv,
        credit=RevenueAccount(
            name="Routing Fee Income",
            sub=node,
        ),
        credit_amount=msat_fee,
        credit_unit=Currency.MSATS,
        credit_conv=fee_conv,
        extra_data=[forward_event.group_id],
    )
    await ledger_entry.save()
    return ledger_entry
