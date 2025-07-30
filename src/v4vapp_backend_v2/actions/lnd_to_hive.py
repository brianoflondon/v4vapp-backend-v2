from datetime import datetime, timezone
from typing import List, Tuple

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_account_classes import (
    AssetAccount,
    LiabilityAccount,
    RevenueAccount,
)
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import LightningToHiveError
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.models.invoice_models import Invoice


async def process_lightning_to_hive(
    invoice: Invoice, nobroadcast: bool = False
) -> Tuple[List[LedgerEntry], str, Amount]:
    """
    Process a Lightning to Hive transfer based on the invoice details.
    """
    # MARK: 1. Checks
    # This mirrors the checks in
    # actions.hive_to_lnd.process_hive_to_lightning
    if invoice.recv_currency not in {Currency.HIVE, Currency.HBD}:
        raise LightningToHiveError(
            f"Unsupported currency for Lightning to Hive transfer. {invoice.recv_currency}"
        )

    cust_id = AccName(invoice.cust_id)
    if not cust_id or not cust_id.is_hive:
        raise LightningToHiveError(
            f"Invalid CustID for Lightning to Hive transfer: {invoice.cust_id}"
        )

    reply_messages = []
    for reply in invoice.replies:
        message = f"Operation has a {reply.reply_type} reply, skipping processing."
        logger.info(
            message,
            extra={"notification": False, **invoice.log_extra},
        )
        reply_messages.append(message)
    if reply_messages:
        raise LightningToHiveError(f"Operation already has replies: {', '.join(reply_messages)}")

    hive_config = InternalConfig().config.hive
    lnd_config = InternalConfig().config.lnd_config
    if (
        not hive_config
        or not lnd_config
        or not lnd_config.default
        or not hive_config.server_account
        or not hive_config.server_account.name
    ):
        # Log a warning if the configuration is missing
        return_hive_message = (
            f"Missing configuration details for Hive or LND: {hive_config}, {lnd_config}"
        )
        logger.warning(return_hive_message, extra={"notification": False})
        raise LightningToHiveError(return_hive_message)

    server_id = hive_config.server_account.name
    node_name = lnd_config.default

    # MARK: Ledger Entries
    # This mirrors and reverses the conversion logic in
    # actions.payment_success.hive_to_lightning_payment_success

    quote = await TrackedBaseModel.nearest_quote(invoice.timestamp)

    value_msat = invoice.value_msat
    net_value_msat = invoice.value_msat - invoice.conv.msats_fee

    net_value_conv = CryptoConversion(
        value=net_value_msat,
        conv_from=Currency.MSATS,
        quote=quote,
    ).conversion

    fee_amount_conv = CryptoConversion(
        value=invoice.conv.msats_fee,
        conv_from=Currency.MSATS,
        quote=quote,
    ).conversion

    receive_amount_hive_hbd = Amount("0.001 HIVE")  # Default return amount
    fee_amount_hive_hbd = Amount("0.001 HIVE")  # Default fee amount
    full_amount_hive_hbd = Amount("0.001 HIVE")  # Default full amount
    if invoice.recv_currency == Currency.HIVE:
        full_amount_hive_hbd = invoice.conv.amount_hive
        receive_amount_hive_hbd = net_value_conv.amount_hive
        fee_amount_hive_hbd = fee_amount_conv.amount_hive
    elif invoice.recv_currency == Currency.HBD:
        full_amount_hive_hbd = invoice.conv.amount_hbd
        receive_amount_hive_hbd = net_value_conv.amount_hbd
        fee_amount_hive_hbd = fee_amount_conv.amount_hbd

    ledger_entries_list: List[LedgerEntry] = []

    # MARK: 1. Receive Lightning Ledger Entry
    ledger_type = LedgerType.LIGHTNING_EXTERNAL_IN
    receive_lightning_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=invoice.short_id,
        op_type=invoice.op_type,
        ledger_type=ledger_type,
        group_id=f"{invoice.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Receive {value_msat / 1000:,.0f} sats for {invoice.cust_id}",
        debit=AssetAccount(
            name="Treasury Lightning",
            sub=node_name,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=value_msat,
        debit_conv=invoice.conv,
        credit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,
        ),
        credit_unit=invoice.recv_currency,
        credit_amount=full_amount_hive_hbd.amount,
        credit_conv=invoice.conv,
    )
    await receive_lightning_ledger_entry.save()
    ledger_entries_list.append(receive_lightning_ledger_entry)

    # MARK: 2 Conversion of Sats to Hive or HBD
    ledger_type = LedgerType.CONV_LIGHTNING_TO_HIVE
    conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=invoice.short_id,
        ledger_type=ledger_type,
        op_type=invoice.op_type,
        group_id=f"{invoice.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Conv {value_msat / 1000:,.0f} sats to {receive_amount_hive_hbd} fee: {fee_amount_conv.sats:,.0f} sats",
        debit=AssetAccount(name="Customer Deposits Hive", sub=server_id),
        debit_unit=invoice.recv_currency,
        debit_amount=receive_amount_hive_hbd.amount,
        debit_conv=net_value_conv,
        credit=AssetAccount(
            name="Treasury Lightning",
            sub=node_name,
        ),
        credit_unit=Currency.MSATS,
        credit_amount=net_value_msat,
        credit_conv=net_value_conv,
    )
    await conversion_ledger_entry.save()
    ledger_entries_list.append(conversion_ledger_entry)

    # MARK: 3 Contra Reconciliation Entry
    ledger_type = LedgerType.CONTRA_LIGHTNING_TO_HIVE
    contra_l_conversion_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=invoice.short_id,
        ledger_type=ledger_type,
        op_type=invoice.op_type,
        group_id=f"{invoice.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Contra conversion of {receive_amount_hive_hbd} for Hive balance reconciliation",
        debit=AssetAccount(
            name="Converted Hive Offset",
            sub=server_id,
            contra=True,
        ),
        debit_unit=invoice.recv_currency,
        debit_amount=receive_amount_hive_hbd.amount,
        debit_conv=net_value_conv,
        credit=AssetAccount(
            name="Customer Deposits Hive",
            sub=server_id,
            contra=False,
        ),
        credit_unit=invoice.recv_currency,
        credit_amount=receive_amount_hive_hbd.amount,
        credit_conv=net_value_conv,
    )
    await contra_l_conversion_ledger_entry.save()
    ledger_entries_list.append(contra_l_conversion_ledger_entry)

    # MARK: 4 Fee Income
    ledger_type = LedgerType.FEE_INCOME
    fee_income_ledger_entry = LedgerEntry(
        cust_id=cust_id,
        short_id=invoice.short_id,
        ledger_type=ledger_type,
        op_type=invoice.op_type,
        group_id=f"{invoice.group_id}-{ledger_type.value}",
        timestamp=datetime.now(tz=timezone.utc),
        description=f"Fee Lightning {cust_id} {invoice.value_msat / 1000:,.0f} sats",
        debit=LiabilityAccount(
            name="Customer Liability",
            sub=cust_id,
        ),
        debit_unit=Currency.MSATS,
        debit_amount=invoice.conv.msats_fee,
        debit_conv=fee_amount_conv,
        credit=RevenueAccount(
            name="Fee Income Hive",
            sub=server_id,
        ),
        credit_unit=Currency.MSATS,
        credit_amount=invoice.conv.msats_fee,
        credit_conv=fee_amount_conv,
    )
    await fee_income_ledger_entry.save()
    ledger_entries_list.append(fee_income_ledger_entry)

    # MARK: 5 Transfer Hive to Customer
    # ledger_type = LedgerType.WITHDRAW_HIVE
    # withdrawal_ledger_entry = LedgerEntry(
    #     cust_id=cust_id,
    #     short_id=invoice.short_id,
    #     ledger_type=ledger_type,
    #     op_type=invoice.op_type,
    #     group_id=f"{invoice.group_id}-{ledger_type.value}",
    #     timestamp=datetime.now(tz=timezone.utc),
    #     description=f"Withdraw {receive_amount_hive_hbd} to {cust_id}",
    #     debit=AssetAccount(
    #         name="Customer Deposits Hive",
    #         sub=server_id,
    #     ),
    #     debit_unit=invoice.recv_currency,
    #     debit_amount=receive_amount_hive_hbd.amount,
    #     debit_conv=net_value_conv,
    #     credit=LiabilityAccount(
    #         name="Customer Liability",
    #         sub=cust_id,
    #     ),
    #     credit_unit=invoice.recv_currency,
    #     credit_amount=receive_amount_hive_hbd.amount,
    #     credit_conv=net_value_conv,
    # )
    # await withdrawal_ledger_entry.save()
    # ledger_entries_list.append(withdrawal_ledger_entry)

    reason = f"You received {value_msat / 1000:,.0f} sats converted to {receive_amount_hive_hbd}"
    return ledger_entries_list, reason, receive_amount_hive_hbd


# Last line of the file
