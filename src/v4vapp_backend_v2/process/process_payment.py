from datetime import datetime, timezone
from decimal import Decimal
from typing import List

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import (
    LedgerEntry,
    LedgerEntryDuplicateException,
    LedgerType,
)
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.hive_to_keepsats import conversion_hive_to_keepsats
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import is_clean_memo, process_clean_memo
from v4vapp_backend_v2.hive.hive_extras import HiveNotHiveAccount
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.models.payment_models import Payment
from v4vapp_backend_v2.process.hive_notification import reply_with_hive
from v4vapp_backend_v2.process.hold_release_keepsats import release_keepsats
from v4vapp_backend_v2.process.process_errors import HiveToLightningError


async def process_payment_success(
    payment: Payment, initiating_op: TrackedAny, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Processes a successful payment by handling both Keepsats and Hive transfers.

    This function treats all payments as Keepsats payments. If the initiating operation is a Hive transfer,
    it first converts Hive to Keepsats before proceeding. The function updates conversion rates for both the
    initiating operation and the payment, records the payment in the ledger, and returns the list of resulting
    ledger entries.

    Args:
        payment (Payment): The payment object containing details of the successful payment.
        old_ledger_entry (LedgerEntry): The original ledger entry associated with the payment.
        nobroadcast (bool, optional): If True, prevents broadcasting certain operations. Defaults to False.

    Returns:
        list[LedgerEntry]: A list of ledger entries resulting from processing the payment.

    Raises:
        HiveToLightningError: If the initiating operation cannot be loaded or Hive to Keepsats conversion fails.

    """
    logger.info(
        f"Processing payment: {payment.short_id} {payment.value_msat / 1000:,.0f} sats, fee: {payment.fee_msat / 1000:,.0f} sats",
        extra={"notification": False},
    )

    # if initiating_op is None:
    #     raise HiveToLightningError(
    #         f"Could not load initiating operation for group_id: {old_ledger_entry.group_id}"
    #     )
    # Find existing ledger entries for this payment
    existing_ledger_entries = (
        await LedgerEntry.collection()
        .find(filter={"group_id": {"$regex": f"{payment.group_id}"}})
        .to_list()
    )
    if existing_ledger_entries:
        message = f"Payment {payment.group_id} already processed with existing {len(existing_ledger_entries)} ledger entries."
        logger.warning(message)
        raise LedgerEntryDuplicateException(message)

    cust_id = payment.cust_id or ""
    ledger_entries_list: list[LedgerEntry] = []
    quote = await TrackedBaseModel.nearest_quote(timestamp=initiating_op.timestamp)
    if initiating_op.conv is None or initiating_op.conv.is_unset():
        await initiating_op.update_conv(quote=quote)
    if payment.conv is None or payment.conv.is_unset():
        await payment.update_conv(quote=quote)

    if isinstance(initiating_op, TransferBase) and not initiating_op.paywithsats:
        # First we must convert the correct amount of Hive to Keepsats
        # This will also send the answer reply (either a hive transfer or custom_json)
        cost_of_payment_msat = Decimal(payment.value_msat) + Decimal(payment.fee_msat)
        try:
            await conversion_hive_to_keepsats(
                server_id=initiating_op.to_account,
                cust_id=cust_id,
                tracked_op=initiating_op,
                msats=cost_of_payment_msat,
                nobroadcast=nobroadcast,
                value_sat_rounded=payment.value_sat_rounded,
                fee_sat_rounded=payment.fee_sat_rounded,
            )
        except Exception as e:
            raise HiveToLightningError(f"Failed to convert Hive to Keepsats for payment: {e}")

    # At this point we can record the payment using Keepsats
    payment_ledger_entries = await record_payment(payment=payment, quote=quote)
    ledger_entries_list.extend(payment_ledger_entries)
    await release_keepsats(tracked_op=initiating_op)

    if isinstance(initiating_op, CustomJson):
        initiating_op.change_memo = process_clean_memo(initiating_op.d_memo)
        end_memo = f" | {initiating_op.lightning_memo}" if initiating_op.lightning_memo else ""

        if not is_clean_memo(initiating_op.lightning_memo):
            initiating_op.change_memo = f"Paid Invoice with Keepsats{end_memo}"
        await initiating_op.save()

        reply_details = HiveReturnDetails(
            tracked_op=initiating_op,
            original_memo=initiating_op.memo,
            action=ReturnAction.CUSTOM_JSON,
            pay_to_cust_id=cust_id,
        )
        try:
            _ = await reply_with_hive(details=reply_details)

        except HiveNotHiveAccount as e:
            logger.info(f"Not sending to a non-Hive Account: {e}")

    return ledger_entries_list


async def record_payment(payment: Payment, quote: QuoteResponse) -> list[LedgerEntry]:
    """
    Lightning payment settlement flow (customer spends sats via node).

    Operates by moving value from the VSC Liability (owed to user) out through the
    node’s external payment channel, recording any network fee as an expense.

        Context (earlier steps handled elsewhere):
            - Customer previously acquired sats (liability created).
            - This function applies the spend and (optionally) the network fee.

    Step 1 (Withdraw Lightning)  LedgerType.WITHDRAW_LIGHTNING
        Reclassify the customer's balance to an external payment in flight.
            Debit: Liability VSC Liability (customer) - MSATS  (reduce what we owe user)
            Credit: Asset External Lightning Payments (node / contra) - MSATS (allocate outbound)
        Net effect: Decreases Liabilities, decreases (contra) Asset bucket → value leaves platform control.
        Amount: payment.value_msat + payment.fee_msat (principal + expected fee)

    Step 2 (Lightning Network Fee, optional)  LedgerType.FEE_EXPENSE (only if fee_msat > 0)
        Recognize the actual network routing fee as an expense.
            Debit: Expense Fee Expenses Lightning (node) - MSATS
            Credit: Asset Treasury Lightning (server) - MSATS
        Net effect: Increases Expenses, reduces Treasury Lightning Asset (economic outflow).
        Amount: payment.fee_msat

    Notes:
        - If fee_msat is zero, only the withdrawal entry is recorded.
        - The principal portion (value_msat) is implicitly consumed by the external payment path.
        - Conversion object (payment.conv) supplies fiat or cross-asset valuation.
        - MSATS used for precision; display may convert to SATS (÷1000).

    Args:
        payment: Lightning payment domain object (includes value_msat, fee_msat, cust_id, conv, destination).
        quote: QuoteResponse used to build conversion for fee entry (if present).

    Returns:
        list[LedgerEntry]: Persisted ledger entries (1 or 2 items).

    Accounting Summary:
        - VSC liability reduced by (value_msat + fee_msat).
        - External payment allocated (principal + fee expectation).
        - Actual fee recognized as expense (if > 0).
        - Economic impact: Expense increases by fee; equity decreases accordingly.

    """

    ledger_entries_list = []
    node_name = InternalConfig().node_name
    cust_id = payment.cust_id or ""

    # MARK: 1 Withdraw Lightning
    ledger_type = LedgerType.WITHDRAW_LIGHTNING
    cost_of_payment_msat = Decimal(payment.value_msat) + Decimal(payment.fee_msat)
    outgoing_conv = payment.conv
    outgoing_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        op_type=payment.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Send {payment.value_sat_rounded} sats to Node {payment.destination} (fee: {payment.fee_sat_rounded})",
        debit=LiabilityAccount(
            name="VSC Liability",
            sub=cust_id,  # This is the CUSTOMER
        ),
        debit_unit=Currency.MSATS,
        debit_amount=cost_of_payment_msat,
        debit_conv=outgoing_conv,
        credit=AssetAccount(name="External Lightning Payments", sub=node_name, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=cost_of_payment_msat,
        credit_conv=payment.conv,
    )
    await outgoing_ledger_entry.save()
    ledger_entries_list.append(outgoing_ledger_entry)

    # MARK: 2: Lightning Network Fee
    # Only record the Lightning fee if it is greater than 0 msats
    if payment.fee_msat > 0:
        lightning_fee_conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=payment.fee_msat,
            quote=quote,
        ).conversion
        ledger_type = LedgerType.FEE_EXPENSE
        fee_ledger_entry_sats = LedgerEntry(
            cust_id=cust_id,
            short_id=payment.short_id,
            ledger_type=ledger_type,
            group_id=f"{payment.group_id}-{ledger_type.value}",
            op_type=payment.op_type,
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Fee Expenses Lightning fee: {payment.fee_msat / 1000:,.0f} sats",
            debit=ExpenseAccount(
                name="Fee Expenses Lightning",
                sub=node_name,  # This is paid from the node
            ),
            debit_unit=Currency.MSATS,
            debit_amount=payment.fee_msat,
            debit_conv=lightning_fee_conv,
            credit=AssetAccount(
                name="Treasury Lightning",
                sub=node_name,  # This is the SERVER
            ),
            credit_unit=Currency.MSATS,
            credit_amount=payment.fee_msat,
            credit_conv=lightning_fee_conv,
        )
        await fee_ledger_entry_sats.save()
        ledger_entries_list.append(fee_ledger_entry_sats)

    return ledger_entries_list
