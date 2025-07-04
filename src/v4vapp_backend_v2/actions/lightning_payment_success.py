import asyncio

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_type import (
    AssetAccount,
    ExpenseAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.hive_to_lightning import (
    calculate_hive_return_change,
    lightning_payment_sent,
)
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.models.payment_models import Payment


async def payment_success(payment: Payment, nobroadcast: bool = False) -> list[LedgerEntry]:
    """
    Handle successful Lightning payment.

    Args:
        payment_hash (str): The hash of the payment.
        amount_msat (int): The amount in millisatoshis.
        description (str | None): Optional description of the payment.
    """
    if payment.succeeded and payment.custom_records:
        v4vapp_group_id = payment.custom_records.v4vapp_group_id or ""
        keysend_message = payment.custom_records.keysend_message or ""
        existing_ledger_entry = await TrackedBaseModel.db_client.find_one(
            collection_name=LedgerEntry.collection(), query={"group_id": v4vapp_group_id}
        )
        if existing_ledger_entry:
            old_ledger_entry = LedgerEntry.model_validate(existing_ledger_entry)
            quote = await TrackedBaseModel.nearest_quote(timestamp=payment.timestamp)
            print(old_ledger_entry)
            print(old_ledger_entry.draw_t_diagram())
            hive_transfer = old_ledger_entry.op
            # MARK: Hive to Lightning Payment Success
            if isinstance(hive_transfer, TransferBase):
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

                # Also need to update the ledger_entry for the original hive_transfer
                old_ledger_entry.op = hive_transfer
                await old_ledger_entry.update_op()

                node_name = InternalConfig().config.lnd_config.default
                ledger_entries_list = []

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
                ledger_type = LedgerType.CONV_H_L
                conversion_ledger_entry = LedgerEntry(
                    ledger_type=ledger_type,
                    group_id=f"{payment.group_id}-{ledger_type.value}",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Conv {conversion_credit_amount} to {conversion_debit_amount / 1000:,.0f} sats {hive_transfer.to_account}",
                    debit=AssetAccount(
                        name="Treasury Lightning",
                        sub=node_name,  # This is the SERVER Lightning
                    ),
                    debit_unit=Currency.MSATS,
                    debit_amount=conversion_credit_debit_conv.msats,
                    debit_conv=conversion_credit_debit_conv,
                    credit=AssetAccount(
                        name="Customer Deposits Hive",
                        sub=hive_transfer.to_account,  # This is the Server
                    ),
                    credit_unit=hive_transfer.unit,
                    credit_amount=conversion_credit_amount.amount,
                    credit_conv=conversion_credit_debit_conv,
                )
                ledger_entries_list.append(conversion_ledger_entry)

                # MARK: 3 Contra Reconciliation Entry
                ledger_type = LedgerType.CONTRA_H_L
                contra_h_conversion_ledger_entry = LedgerEntry(
                    ledger_type=ledger_type,
                    group_id=f"{payment.group_id}-{ledger_type.value}",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Contra conversion of {conversion_credit_amount} for Hive balance reconciliation",
                    debit=AssetAccount(
                        name="Customer Deposits Hive",
                        sub=hive_transfer.to_account,  # This is the Server
                        contra=False,  # This removes funds from the Server's Hive account
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=conversion_credit_amount.amount,
                    debit_conv=conversion_credit_debit_conv,
                    credit=AssetAccount(
                        name="Converted Hive Offset",
                        sub=hive_transfer.to_account,  # This is the Server
                        contra=True,  # Adds them to the Converted Hive Offset account
                    ),
                    credit_unit=hive_transfer.unit,
                    credit_amount=conversion_credit_amount.amount,
                    credit_conv=conversion_credit_debit_conv,  # No conversion needed
                )
                ledger_entries_list.append(contra_h_conversion_ledger_entry)

                # MARK: 4 Fee Income
                ledger_type = LedgerType.FEE_INCOME
                fee_credit_conv = CryptoConversion(
                    conv_from=Currency.MSATS,
                    value=hive_transfer.fee_conv.msats,
                    quote=quote,
                ).conversion
                fee_debit_value = getattr(hive_transfer.fee_conv, hive_transfer.unit.lower())
                fee_debit_amount_hive = Amount(
                    f"{fee_debit_value:.3f} {hive_transfer.unit.upper()}"
                )

                fee_ledger_entry_hive = LedgerEntry(
                    ledger_type=ledger_type,
                    group_id=f"{payment.group_id}-{ledger_type.value}",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Fee Lightning {hive_transfer.from_account} {cost_of_payment_msat / 1000:,.0f} sats",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
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
                ledger_entries_list.append(fee_ledger_entry_hive)

                # MARK: 5 Fulfill Main Payment Obligation
                ledger_type = LedgerType.LIGHTNING_OUT
                outgoing_debit_amount = conversion_credit_amount - fee_debit_amount_hive
                outgoing_conv = CryptoConversion(
                    conv_from=hive_transfer.unit,
                    amount=outgoing_debit_amount,
                    quote=quote,
                ).conversion
                outgoing_ledger_entry = LedgerEntry(
                    ledger_type=ledger_type,
                    group_id=f"{payment.group_id}-{ledger_type.value}",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Allocate outgoing {outgoing_debit_amount} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
                        contra=False,
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=outgoing_debit_amount.amount,
                    debit_conv=outgoing_conv,
                    credit=AssetAccount(
                        name="External Lightning Payments", sub=node_name, contra=True
                    ),
                    credit_unit=Currency.MSATS,
                    credit_amount=cost_of_payment_msat,
                    credit_conv=payment.conv,
                )
                ledger_entries_list.append(outgoing_ledger_entry)

                # MARK: 6 Send Lightning Payment
                ledger_type = LedgerType.LIGHTNING_CONTRA
                external_payment_ledger_entry = LedgerEntry(
                    ledger_type=ledger_type,
                    group_id=f"{payment.group_id}-{ledger_type.value}",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"External Lightning payment of {cost_of_payment_msat / 1000:,.0f} SATS to {payment.destination}",
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
                ledger_entries_list.append(external_payment_ledger_entry)

                # outgoing_debit_amount = conversion_credit_amount - fee_debit_amount_hive
                # outgoing_conv = CryptoConversion(
                #     conv_from=hive_transfer.unit,
                #     amount=outgoing_debit_amount,
                #     quote=quote,
                # ).conversion
                # outgoing_ledger_entry = LedgerEntry(
                #     group_id=f"{payment.group_id}_outgoing",
                #     timestamp=payment.timestamp,
                #     op=payment,
                #     description=f"Allocate outgoing {outgoing_debit_amount} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
                #     debit=LiabilityAccount(
                #         name="Customer Liability Hive",
                #         sub=hive_transfer.from_account,  # This is the CUSTOMER
                #     ),
                #     debit_unit=hive_transfer.unit,
                #     debit_amount=outgoing_debit_amount.amount,
                #     debit_conv=outgoing_conv,
                #     credit=LiabilityAccount(
                #         name="Lightning Payment Clearing",
                #         sub=node_name,
                #     ),
                #     credit_unit=hive_transfer.unit,
                #     credit_amount=outgoing_debit_amount.amount,
                #     credit_conv=outgoing_conv,
                # )
                # ledger_entries_list.append(outgoing_ledger_entry)

                # MARK: 5 External Lightning Payment
                # # Uses the cost_of_payment_msat which includes the fee
                # external_payment_ledger_entry = LedgerEntry(
                #     group_id=f"{payment.group_id}_external_payment",
                #     timestamp=payment.timestamp,
                #     op=payment,
                #     description=f"External Lightning payment of {cost_of_payment_msat / 1000:,.0f} SATS to {payment.destination}",
                #     debit=AssetAccount(
                #         name="Treasury Lightning",
                #         sub=node_name,
                #         contra=True,  # This is FROM the External Lightning Payments account
                #     ),
                #     debit_unit=Currency.MSATS,
                #     debit_amount=cost_of_payment_msat,
                #     debit_conv=payment.conv,
                #     credit=LiabilityAccount(name="Lightning Payment Clearing", sub=node_name, contra=True),
                #     credit_unit=Currency.MSATS,
                #     credit_amount=cost_of_payment_msat,
                #     credit_conv=payment.conv,
                # )
                # ledger_entries_list.append(external_payment_ledger_entry)

                # MARK: 5.5 Clear Lightning Payment Clearing
                # clear_lightning_clearing_ledger_entry = LedgerEntry(
                #     group_id=f"{payment.group_id}_clearing",
                #     timestamp=payment.timestamp,
                #     op=payment,
                #     description=f"Clear Lightning Payment Clearing for {hive_transfer.from_account}",
                #     debit=LiabilityAccount(
                #         name="Lightning Payment Clearing",
                #         sub=node_name,  # This is the SERVER
                #     ),
                #     debit_unit=hive_transfer.unit,
                #     debit_amount=outgoing_debit_amount.amount,
                #     debit_conv=outgoing_conv,
                #     credit=LiabilityAccount(
                #         name="Customer Liability Hive",
                #         sub=hive_transfer.from_account,  # This is the CUSTOMER
                #     ),
                #     credit_unit=hive_transfer.unit,
                #     credit_amount=outgoing_debit_amount.amount,
                #     credit_conv=outgoing_conv,
                # )
                # ledger_entries_list.append(clear_lightning_clearing_ledger_entry)

                # MARK: 6 Service Fee
                # Build the 3b ledger entry: the Fee expense

                # Consider adding a ledger entry for the  payment.fee_msat
                # cost_of_payment_msat = payment.value_msat + payment.fee_msat

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
                        ledger_type=ledger_type,
                        group_id=f"{payment.group_id}-{ledger_type.value}",
                        timestamp=payment.timestamp,
                        op=payment,
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
                    ledger_entries_list.append(fee_ledger_entry_sats)

                hive_transfer.add_reply(
                    reply_id=payment.group_id_p,
                    reply_type="payment",
                    reply_msat=cost_of_payment_msat,
                    reply_error=None,
                    reply_message=message,
                )

                asyncio.create_task(
                    lightning_payment_sent(
                        payment=payment,
                        hive_transfer=hive_transfer,
                        nobroadcast=nobroadcast,
                    )
                )
                return ledger_entries_list

        raise NotImplementedError(f"Not implemented yet {v4vapp_group_id} {keysend_message}")
    return []
