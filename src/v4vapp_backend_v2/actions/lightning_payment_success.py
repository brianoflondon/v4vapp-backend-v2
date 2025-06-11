import asyncio

from v4vapp_backend_v2.accounting.account_type import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry
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

                change_amount = await calculate_hive_return_change(
                    hive_transfer=hive_transfer,
                    payment=payment,
                )
                assert hive_transfer.change_amount is not None, (
                    "Change amount should not be None after calculating change."
                )

                # These two functions are carried out in the calculate_hive_return_change function
                # hive_transfer.change_amount = change_amount
                # await hive_transfer.update_conv()

                quote = await TrackedBaseModel.nearest_quote(timestamp=payment.timestamp)
                hive_transfer.fee_conv = CryptoConversion(
                    conv_from=Currency.MSATS,
                    value=hive_transfer.conv.msats_fee,
                    quote=quote,
                ).conversion
                ans = await hive_transfer.save(
                    include={"replies", "fee_conv", "change_amount", "change_conv"}
                )
                # Also need to update the ledger_entry for the original hive_transfer
                old_ledger_entry.op = hive_transfer
                await old_ledger_entry.update_op()

                node_name = InternalConfig().config.lnd_config.default
                ledger_entries_list = []

                # MARK: Conversion of Hive to Sats
                conversion_credit_amount = hive_transfer.amount.beam - change_amount
                conversion_debit_amount = cost_of_payment_msat + hive_transfer.conv.msats_fee
                conversion_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_conversion",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Conv {conversion_credit_amount} to {conversion_debit_amount / 1000:,.0f} sats {hive_transfer.to_account}",
                    debit=AssetAccount(
                        name="Treasury Lightning",
                        sub=node_name,  # This is the SERVER Lightning
                    ),
                    debit_unit=Currency.MSATS,
                    debit_amount=conversion_debit_amount,
                    debit_conv=payment.conv,
                    credit=AssetAccount(
                        name="Customer Deposits Hive",
                        sub=hive_transfer.to_account,  # This is the CUSTOMER
                    ),
                    credit_unit=hive_transfer.unit,
                    credit_amount=conversion_credit_amount.amount,
                    credit_conv=hive_transfer.conv,
                )
                ledger_entries_list.append(conversion_ledger_entry)

                # MARK: Outgoing
                # Build the 3a ledger entry: Outgoing Lightning Payment
                fee_debit_amount = getattr(hive_transfer.fee_conv, hive_transfer.unit.lower())
                outgoing_debit_amount = (
                    hive_transfer.amount.beam - change_amount - fee_debit_amount
                )
                outgoing_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_outgoing",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Outgoing Lightning Payment {outgoing_debit_amount} {cost_of_payment_msat / 1000:,.0f} sats to {payment.destination}",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=outgoing_debit_amount.amount,
                    debit_conv=hive_transfer.conv,
                    credit=AssetAccount(
                        name="Treasury Lightning",
                        sub=node_name,  # This is the SERVER
                    ),
                    credit_unit=Currency.MSATS,
                    credit_amount=cost_of_payment_msat,
                    credit_conv=payment.conv,
                )
                ledger_entries_list.append(outgoing_ledger_entry)

                # MARK: Fee
                # Build the 3b ledger entry: the Fee expense
                fee_ledger_entry = LedgerEntry(
                    group_id=f"{payment.group_id}_fee",
                    timestamp=payment.timestamp,
                    op=payment,
                    description=f"Fee Lightning {hive_transfer.from_account} {cost_of_payment_msat / 1000:,.0f} sats",
                    debit=LiabilityAccount(
                        name="Customer Liability Hive",
                        sub=hive_transfer.from_account,  # This is the CUSTOMER
                    ),
                    debit_unit=hive_transfer.unit,
                    debit_amount=fee_debit_amount,
                    debit_conv=hive_transfer.fee_conv,
                    credit=RevenueAccount(
                        name="Fee Income Lightning",
                        sub=node_name,  # This is the SERVER
                    ),
                    credit_unit=Currency.MSATS,
                    credit_amount=hive_transfer.conv.msats_fee,
                    credit_conv=hive_transfer.fee_conv,
                )
                ledger_entries_list.append(fee_ledger_entry)

                hive_transfer.add_reply(
                    reply_id=payment.group_id_p,
                    reply_type="payment",
                    reply_msat=cost_of_payment_msat,
                    reply_error=None,
                    reply_message=message,
                )
                await hive_transfer.save()
                # This will initiate the return payment
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
