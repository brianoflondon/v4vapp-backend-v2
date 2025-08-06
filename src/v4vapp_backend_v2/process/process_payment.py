from datetime import datetime, timezone
from typing import List

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance_printout
from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import HiveToLightningError
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.hive_to_keepsats import conversion_hive_to_keepsats
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.payment_models import Payment


async def process_payment_success(
    payment: Payment, old_ledger_entry: LedgerEntry, nobroadcast: bool = False
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
        f"Processing payment: {payment.group_id} with old ledger entry: {old_ledger_entry}",
        extra={"notification": False},
    )
    initiating_op = await load_tracked_object(tracked_obj=old_ledger_entry.group_id)
    if initiating_op is None:
        raise HiveToLightningError(
            f"Could not load initiating operation for group_id: {old_ledger_entry.group_id}"
        )
    # Find existing ledger entries for this payment
    existing_ledger_entries = (
        await LedgerEntry.collection()
        .find(filter={"group_id": {"$regex": f"{payment.group_id}"}})
        .to_list()
    )
    if existing_ledger_entries:
        raise HiveToLightningError(
            f"Payment {payment.group_id} already processed with existing {len(existing_ledger_entries)} ledger entries."
        )

    cust_id = payment.cust_id or ""
    ledger_entries_list: list[LedgerEntry] = []
    quote = await TrackedBaseModel.nearest_quote(timestamp=initiating_op.timestamp)
    if initiating_op.conv is None or initiating_op.conv.is_unset():
        await initiating_op.update_conv(quote=quote)
    if payment.conv is None or payment.conv.is_unset():
        await payment.update_conv(quote=quote)

    net_sats, details = await keepsats_balance_printout(
        cust_id=cust_id,
    )

    if isinstance(initiating_op, TransferBase) and not initiating_op.paywithsats:
        # First we must convert the correct amount of Hive to Keepsats
        cost_of_payment_msat = payment.value_msat + payment.fee_msat
        try:
            await conversion_hive_to_keepsats(
                server_id=initiating_op.to_account,
                cust_id=cust_id,
                tracked_op=initiating_op,
                convert_amount=Amount(f"{initiating_op.amount}"),
                msats=cost_of_payment_msat,
                nobroadcast=nobroadcast,
            )
        except Exception as e:
            raise HiveToLightningError(f"Failed to convert Hive to Keepsats for payment: {e}")

    net_sats, details = await keepsats_balance_printout(
        cust_id=cust_id,
    )

    # At this point we can record the payment using Keepsats
    payment_ledger_entries = await record_payment(payment=payment, quote=quote)
    ledger_entries_list.extend(payment_ledger_entries)
    return ledger_entries_list


async def record_payment(payment: Payment, quote: QuoteResponse) -> list[LedgerEntry]:
    """
    Records the ledger entries for a successful Lightning payment.
    This function creates and saves ledger entries for the following actions:
    1. Withdraw Lightning: Allocates outgoing Lightning payment from the customer's liability account to the external Lightning payments account.
    2. Send Lightning Payment: Records the external Lightning payment from the external Lightning payments account to the treasury Lightning account.
    3. Lightning Network Fee (if applicable): Records the Lightning network fee as an expense from the node's fee expenses account to the treasury Lightning account.
    Args:
        payment (Payment): The payment object containing details of the Lightning payment.
        quote (QuoteResponse): The quote response object used for currency conversion.
    Returns:
        list[LedgerEntry]: A list of saved ledger entries corresponding to the payment actions.
    """

    ledger_entries_list = []
    node_name = InternalConfig().config.lnd_config.default
    cust_id = payment.cust_id or ""

    # MARK: 5 Withdraw Lightning
    ledger_type = LedgerType.WITHDRAW_LIGHTNING
    cost_of_payment_msat = payment.value_msat + payment.fee_msat
    outgoing_conv = payment.conv
    outgoing_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        op_type=payment.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Allocate outgoing Lightning {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
        debit=LiabilityAccount(
            name="Customer Liability",
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

    # MARK: 6 Send Lightning Payment
    ledger_type = LedgerType.LIGHTNING_EXTERNAL_SEND
    external_payment_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        op_type=payment.op_type,
        timestamp=datetime.now(tz=timezone.utc),
        description=f"External Lightning payment {cost_of_payment_msat / 1000:,.0f} SATS to {payment.destination}",
        debit=AssetAccount(
            name="External Lightning Payments",
            sub=node_name,
            contra=True,  # This is FROM the External Lightning Payments account
        ),
        debit_unit=Currency.MSATS,
        debit_amount=cost_of_payment_msat,
        debit_conv=payment.conv,
        credit=AssetAccount(name="Treasury Lightning", sub=node_name, contra=False),
        credit_unit=Currency.MSATS,
        credit_amount=cost_of_payment_msat,
        credit_conv=payment.conv,
    )
    await external_payment_ledger_entry.save()
    ledger_entries_list.append(external_payment_ledger_entry)

    # MARK: 7: Lightning Network Fee
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
