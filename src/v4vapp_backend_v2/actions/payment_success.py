from datetime import timedelta

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.hive_to_lnd import (
    calculate_hive_return_change,
    lightning_payment_sent,
)
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.general_purpose_funcs import timestamp_inc
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.payment_models import Payment

# MARK: Hive Payment Success


async def hive_to_lightning_payment_success(
    payment: Payment, old_ledger_entry: LedgerEntry, nobroadcast: bool = False
) -> list[LedgerEntry]:
    """
    Handle successful Lightning payment and generate corresponding ledger entries.

    This function processes a successful Lightning Network payment, updates related ledger entries,
    and manages the conversion and reconciliation between Hive and Lightning assets. It handles
    various accounting steps such as conversion, contra reconciliation, fee income, payment fulfillment,
    and fee expenses. If a matching inbound Hive payment is found, it updates the ledger accordingly.
    Otherwise, it raises a NotImplementedError for unhandled cases.

        payment (Payment): The Lightning payment object containing payment details and custom records.
        nobroadcast (bool, optional): If True, prevents broadcasting of certain events. Defaults to False.

    Returns:
        list[LedgerEntry]: A list of new ledger entries created as a result of the successful payment.

    Raises:
        NotImplementedError: If the payment does not match an expected inbound Hive payment or is otherwise unhandled.
    Handle successful Lightning payment.

    """
    # MARK: Hive to Lightning Payment Success
    quote = await TrackedBaseModel.nearest_quote(payment.timestamp)
    timestamp = timestamp_inc(payment.timestamp, inc=timedelta(seconds=0.01))
    hive_transfer = await load_tracked_object(old_ledger_entry.group_id)
    if not isinstance(hive_transfer, TransferBase):
        raise NotImplementedError(
            f"Unhandled operation type: {type(hive_transfer)} for hive_to_lightning_payment_success {payment.group_id_p}"
        )

    # This means we have found an inbound Hive payment and this is the matching
    # payment
    # https://x.com/i/grok/share/YLSqQzDg6kmuxeaohwtff7s1m
    # Build the new ledger entry for the  Conversion of Hive to Sats in the hive_to_lightning file.

    # Mark: Record the Payment in the Hive Op
    cost_of_payment_msat = payment.value_msat + payment.fee_msat
    message = f"Lightning payment sent for operation {hive_transfer.group_id_p}: {payment.payment_hash} {payment.route_str} {cost_of_payment_msat / 1000:,.0f} sats"

    # This will re-calculate the change and the fee and update the fee_conv
    # After this, the correct fee to use is in hive_transfer.fee_conv.msats
    # Do NOT use hive_transfer.fee_conv.msats_fee as it is the fee value of the fee!
    change_amount = await calculate_hive_return_change(
        hive_transfer=hive_transfer,
        payment=payment,
    )
    assert hive_transfer.fee_conv is not None, (
        "Fee conversion should not be None after calculating change."
    )

    assert hive_transfer.change_amount is not None, (
        "Change amount should not be None after calculating change."
    )

    node_name = InternalConfig().config.lnd_config.default
    ledger_entries_list = []

    # Identify the customer and server
    cust_id = hive_transfer.from_account
    server_id = hive_transfer.to_account

    # Note: step 1 of this process is receipt of Hive and that is handled in the
    # hive_to_lightning.py file, so we start from step 2 here.
    # MARK: 2 Conversion of Hive to Sats
    conversion_credit_amount = hive_transfer.amount.beam - change_amount
    conversion_debit_amount = cost_of_payment_msat + hive_transfer.fee_conv.msats
    conversion_credit_debit_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=conversion_debit_amount,
        quote=quote,
    ).conversion
    ledger_type = LedgerType.CONV_HIVE_TO_LIGHTNING
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        op_type=payment.op_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Conv {conversion_credit_amount} to {conversion_debit_amount / 1000:,.0f} sats {payment.destination} for {cust_id}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub=node_name,  # This is the SERVER Lightning
        ),
        debit_unit=Currency.MSATS,
        debit_amount=conversion_credit_debit_conv.msats,
        debit_conv=conversion_credit_debit_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,  # This is the Server
        ),
        credit_unit=hive_transfer.unit,
        credit_amount=conversion_credit_amount.amount,
        credit_conv=conversion_credit_debit_conv,
    )
    await conversion_ledger_entry.save()
    ledger_entries_list.append(conversion_ledger_entry)

    # MARK: 3 Contra Reconciliation Entry
    ledger_type = LedgerType.CONTRA_HIVE_TO_LIGHTNING
    contra_h_conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        op_type=payment.op_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Contra conversion of {conversion_credit_amount} for Hive balance reconciliation",
        debit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,  # This is the Server
            contra=False,  # This removes funds from the Server's Hive account
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=conversion_credit_amount.amount,
        debit_conv=conversion_credit_debit_conv,
        credit=AssetAccount(
            name="Converted Hive Offset",
            sub=server_id,  # This is the Server
            contra=True,  # Adds them to the Converted Hive Offset account
        ),
        credit_unit=hive_transfer.unit,
        credit_amount=conversion_credit_amount.amount,
        credit_conv=conversion_credit_debit_conv,  # No conversion needed
    )
    await contra_h_conversion_ledger_entry.save()
    ledger_entries_list.append(contra_h_conversion_ledger_entry)

    # MARK: 4 Fee Income
    ledger_type = LedgerType.FEE_INCOME
    fee_credit_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=hive_transfer.fee_conv.msats,
        quote=quote,
    ).conversion
    fee_debit_value = getattr(hive_transfer.fee_conv, hive_transfer.unit.lower())
    fee_debit_amount_hive = Amount(f"{fee_debit_value:.3f} {hive_transfer.unit.upper()}")

    fee_ledger_entry_hive = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        op_type=payment.op_type,
        timestamp=next(timestamp),
        description=f"Fee Lightning {cust_id} {cost_of_payment_msat / 1000:,.0f} sats",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the CUSTOMER
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=fee_debit_amount_hive.amount,
        debit_conv=fee_credit_conv,
        credit=RevenueAccount(
            name="Fee Income Lightning",
            sub=node_name,  # This is the SERVER
        ),
        credit_unit=Currency.MSATS,
        credit_amount=hive_transfer.fee_conv.msats,
        credit_conv=fee_credit_conv,
    )
    await fee_ledger_entry_hive.save()
    ledger_entries_list.append(fee_ledger_entry_hive)

    # MARK: 5 Fulfill Main Payment Obligation
    ledger_type = LedgerType.WITHDRAW_LIGHTNING
    outgoing_debit_amount = conversion_credit_amount - fee_debit_amount_hive
    outgoing_conv = CryptoConversion(
        conv_from=hive_transfer.unit,
        amount=outgoing_debit_amount,
        quote=quote,
    ).conversion
    outgoing_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        op_type=payment.op_type,
        timestamp=next(timestamp),
        description=f"Allocate outgoing Lightning {outgoing_debit_amount} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the CUSTOMER
            contra=False,
        ),
        debit_unit=hive_transfer.unit,
        debit_amount=outgoing_debit_amount.amount,
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
        timestamp=next(timestamp),
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
            timestamp=next(timestamp),
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

    hive_transfer.add_reply(
        reply_id=payment.group_id_p,
        reply_type="payment",
        reply_msat=cost_of_payment_msat,
        reply_error=None,
        reply_message=message,
    )
    await hive_transfer.save()

    await lightning_payment_sent(
        payment=payment,
        hive_transfer=hive_transfer,
        nobroadcast=nobroadcast,
    )

    return ledger_entries_list


# MARK: Keepsats Payment Success


async def keepsats_to_lightning_payment_success(
    payment: Payment, old_ledger_entry: LedgerEntry, nobroadcast: bool = False
) -> list[LedgerEntry]:
    """
    Handle successful Keepsats payment and generate corresponding ledger entries.

    This function processes a successful Keepsats payment, updates related ledger entries,
    and manages the conversion and reconciliation between Hive and Lightning assets.
    It handles various accounting steps such as conversion, contra reconciliation, fee income,
    payment fulfillment, and fee expenses.

        payment (Payment): The Keepsats payment object containing payment details and custom records.
        old_ledger_entry (LedgerEntry): The original ledger entry associated with the payment.
        nobroadcast (bool, optional): If True, prevents broadcasting of reply events. Defaults to False.

    Returns:
        list[LedgerEntry]: A list of new ledger entries created as a result of the successful payment.
    """
    quote = await TrackedBaseModel.nearest_quote(payment.timestamp)
    timestamp = timestamp_inc(payment.timestamp, inc=timedelta(seconds=0.01))
    original_op = await load_tracked_object(old_ledger_entry.group_id)
    if not isinstance(original_op, TransferBase | CustomJson):
        raise NotImplementedError(
            f"Unhandled operation type: {type(original_op)} for hive_to_lightning_payment_success {payment.group_id_p}"
        )

    # Identify the customer and server
    cust_id = original_op.cust_id
    # server_id = hive_transfer.to_account

    # Mark: Record the Payment in the Hive Op
    cost_of_payment_msat = payment.value_msat + payment.fee_msat
    message = f"Keepsats payment sent for operation {original_op.group_id_p}: {payment.payment_hash} {payment.route_str} {cost_of_payment_msat / 1000:,.0f} sats"

    node_name = "keepsats"
    ledger_entries_list = []

    # MARK: 2z Fulfill Main Payment Obligation
    # THIS DOESN'T INCLUDE THE LIGHTING FEE
    ledger_type = LedgerType.WITHDRAW_KEEPSATS
    outgoing_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"Allocate outgoing Keepsats {payment.value_msat / 1000:,.0f} sats to {payment.destination}",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,  # This is the CUSTOMER
            contra=False,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=payment.value_msat,
        debit_conv=payment.conv,
        credit=AssetAccount(name="External Lightning Payments", sub=node_name, contra=True),
        credit_unit=Currency.MSATS,
        credit_amount=payment.value_msat,
        credit_conv=payment.conv,
    )
    await outgoing_ledger_entry.save()
    ledger_entries_list.append(outgoing_ledger_entry)

    # MARK: 2b Send Lightning Payment
    ledger_type = LedgerType.LIGHTNING_EXTERNAL_SEND
    outgoing_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=payment.short_id,
        ledger_type=ledger_type,
        group_id=f"{payment.group_id}-{ledger_type.value}",
        timestamp=next(timestamp),
        description=f"External Lightning payment {payment.value_msat / 1000:,.0f} sats to {payment.destination}",
        debit=AssetAccount(
            name="External Lightning Payments",
            sub=node_name,
            contra=True,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=payment.value_msat,
        debit_conv=payment.conv,
        credit=AssetAccount(name="Treasury Lightning", sub=node_name, contra=False),
        credit_unit=Currency.MSATS,
        credit_amount=payment.value_msat,
        credit_conv=payment.conv,
    )
    await outgoing_ledger_entry.save()
    ledger_entries_list.append(outgoing_ledger_entry)

    # MARK: 3: Lightning Network Fee Charge Entry
    # Only record the Lightning fee if it is greater than 0 msats
    if payment.fee_msat > 0:
        lightning_fee_conv = CryptoConversion(
            conv_from=Currency.MSATS,
            value=payment.fee_msat,
            quote=quote,
        ).conversion

        # The fee is taken directly from the customer's liability account and passed into the
        # Treasury Lightning account as a fee charge. From there it is then sent to expenses
        ledger_type = LedgerType.FEE_CHARGE
        external_payment_ledger_entry = LedgerEntry(
            cust_id=cust_id,
            short_id=payment.short_id,
            ledger_type=ledger_type,
            group_id=f"{payment.group_id}-{ledger_type.value}",
            timestamp=next(timestamp),
            description=f"External Lightning payment of {cost_of_payment_msat / 1000:,.0f} SATS to {payment.destination}",
            debit=LiabilityAccount(
                name="Customer Liability",
                sub=cust_id,
            ),
            debit_unit=Currency.MSATS,
            debit_amount=payment.fee_msat,
            debit_conv=lightning_fee_conv,
            credit=AssetAccount(name="Treasury Lightning", sub=node_name),
            credit_unit=Currency.MSATS,
            credit_amount=payment.fee_msat,
            credit_conv=lightning_fee_conv,
        )
        await external_payment_ledger_entry.save()
        ledger_entries_list.append(external_payment_ledger_entry)

        ledger_type = LedgerType.FEE_EXPENSE
        fee_ledger_entry_sats = LedgerEntry(
            cust_id=cust_id,
            short_id=payment.short_id,
            ledger_type=ledger_type,
            group_id=f"{payment.group_id}-{ledger_type.value}",
            timestamp=next(timestamp),
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

    # Custom Json doesn't have an amount, or change.
    if isinstance(original_op, TransferBase):
        original_op.change_amount = original_op.amount

    original_op.add_reply(
        reply_id=payment.group_id_p,
        reply_type="payment",
        reply_msat=cost_of_payment_msat,
        reply_error=None,
        reply_message=message,
    )
    await original_op.save()

    # For Hive transfer we send a payment notification.
    # The question is whether we should send a notification for CustomJson
    # events too.
    # TODO: #142 Send custom_json receipt and process notifications or not?

    if isinstance(original_op, TransferBase):
        await lightning_payment_sent(
            payment=payment,
            hive_transfer=original_op,
            nobroadcast=nobroadcast,
        )
    return ledger_entries_list
