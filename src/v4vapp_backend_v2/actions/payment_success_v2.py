from datetime import datetime, timezone

from v4vapp_backend_v2.accounting.account_balances import get_keepsats_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import HiveToLightningError
from v4vapp_backend_v2.actions.hive_to_keepsats import hive_to_keepsats_deposit
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency, QuoteResponse
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.payment_models import Payment


async def process_payment_success(
    payment: Payment, old_ledger_entry: LedgerEntry, nobroadcast: bool = False
) -> list[LedgerEntry]:
    """
    New way of processing payment success actions.
    Treats all payments as a Keepsats payment but if necessary, converts Hive into Keepsats first.
    """
    initiating_op = await load_tracked_object(tracked_obj=old_ledger_entry.group_id)
    if initiating_op is None:
        raise HiveToLightningError(
            f"Could not load initiating operation for group_id: {old_ledger_entry.group_id}"
        )
    cust_id = payment.cust_id or ""
    ledger_entries_list: list[LedgerEntry] = []
    quote = await TrackedBaseModel.nearest_quote(timestamp=initiating_op.timestamp)
    if initiating_op.conv is None or initiating_op.conv.is_unset():
        await initiating_op.update_conv(quote=quote)
    if payment.conv is None or payment.conv.is_unset():
        await payment.update_conv(quote=quote)

    if isinstance(initiating_op, TransferBase) and not initiating_op.paywithsats:
        # This is a Hive transfer, convert it to Keepsats first
        cost_of_payment_msat = payment.value_msat + payment.fee_msat
        try:
            conv_ledger_entries, conv_reason, return_hive_amount = await hive_to_keepsats_deposit(
                hive_transfer=initiating_op,
                msats_to_deposit=cost_of_payment_msat,
                nobroadcast=nobroadcast,
            )
            ledger_entries_list.extend(conv_ledger_entries)
        except HiveToLightningError as e:
            logger.error(
                f"Failed to convert Hive to Keepsats: {e}",
                extra={"notification": False, **initiating_op.log_extra, **payment.log_extra},
            )
            raise e

    # Now check the conversion succeeded and we can proceed with the Keepsats payment
    balance_before, details_before = await get_keepsats_balance(cust_id=cust_id)
    payment_ledger_entries = await record_payment(payment=payment, quote=quote)
    ledger_entries_list.extend(payment_ledger_entries)

    balance_after, details_after = await get_keepsats_balance(cust_id=cust_id)

    # Check if there is still Hive or HBE left in the account, initiate sweep.

    return ledger_entries_list


async def record_payment(payment: Payment, quote: QuoteResponse) -> list[LedgerEntry]:
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
        description=f"Allocate outgoing Lightning {cost_of_payment_msat} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
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
