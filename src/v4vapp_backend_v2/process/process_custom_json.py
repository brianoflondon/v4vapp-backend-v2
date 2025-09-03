import asyncio
from datetime import datetime, timezone
from typing import List

from colorama import Fore, Style

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount, RevenueAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.hive_notification import reply_with_hive
from v4vapp_backend_v2.process.hold_release_keepsats import release_keepsats
from v4vapp_backend_v2.process.process_errors import (
    CustomJsonToLightningError,
    InsufficientBalanceError,
)


async def custom_json_internal_transfer(
    custom_json: CustomJson, keepsats_transfer: KeepsatsTransfer, nobroadcast: bool = False
) -> List[LedgerEntry]:
    """
    Must perform balance check before processing the transfer.

    Processes an internal transfer operation based on custom JSON input.
    This asynchronous function handles the transfer of Keepsats between two accounts,
    records the transaction in the ledger, and sends a notification to the recipient if the transfer amount
    exceeds the minimum invoice payment threshold.

    If the original transaction is a Hive transfer, it will perform the return operation.
    Args:
        custom_json (CustomJson): The custom JSON object containing operation details.
        keepsats_transfer (KeepsatsTransfer): The transfer details including source, destination, amount, and memo.
        nobroadcast (bool, optional): If True, suppresses broadcasting the notification. Defaults to False.
    Returns:
        LedgerEntry: The ledger entry representing the transfer transaction.
    Notes:
        - If the transfer amount is below the minimum notification threshold, no notification is sent.
        - The function saves the ledger entry and optionally sends a notification to the recipient.
    """
    # This is a transfer between two accounts
    logger.info(
        f"{custom_json.short_id} Processing CustomJson transfer: {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats"
    )
    if not keepsats_transfer or not keepsats_transfer.sats:
        raise CustomJsonToLightningError("Keepsats transfer amount is zero.")

    net_msats, account_balance = await keepsats_balance(cust_id=keepsats_transfer.from_account)
    keepsats_transfer.msats = (
        keepsats_transfer.sats * 1_000 if not keepsats_transfer.msats else keepsats_transfer.msats
    )
    server_id = InternalConfig().server_id
    fee_transfer = False
    fee_sats = custom_json.fee_memo
    if fee_sats > 0 and keepsats_transfer.sats <= fee_sats and custom_json.to_account == server_id:
        fee_transfer = True

    # Add a buffer of 1 sat 1_000 msats to avoid rounding issues
    if net_msats + 1_000 < keepsats_transfer.msats:
        message = f"Insufficient Keepsats balance for {'fee' if fee_transfer else 'transfer'}: {keepsats_transfer.from_account} has {net_msats // 1000:,.0f} sats, but transfer requires {keepsats_transfer.sats:,} sats."
        if fee_transfer:
            logger.info(message)
        # The order in which refunds arrive from payment, and fees are taken is not always predictable
        # ALWAYS account for fees when processing refunds
        if not fee_transfer:
            logger.warning(message)
            # Sending this to follow_on_transfer which will deal with the balance failure and send notification
            return_details = HiveReturnDetails(
                tracked_op=custom_json,
                original_memo=keepsats_transfer.memo,
                reason_str=message,
                action=ReturnAction.CHANGE,
                pay_to_cust_id=keepsats_transfer.from_account,
                nobroadcast=nobroadcast,
            )
            trx = await reply_with_hive(details=return_details, nobroadcast=nobroadcast)
            logger.info(
                f"{Fore.WHITE}Reply after custom_json transfer failure due to insufficient balance{Style.RESET_ALL}",
                extra={
                    "notification": False,
                    "trx": trx,
                    **custom_json.log_extra,
                    **return_details.log_extra,
                },
            )
            raise InsufficientBalanceError(message)

    debit_credit_amount = keepsats_transfer.msats

    ledger_entries: List[LedgerEntry] = []

    user_memo = (
        keepsats_transfer.user_memo
        or f"{keepsats_transfer.to_account} received {keepsats_transfer.sats:,} sats from {keepsats_transfer.from_account}"
    )
    description = f"Transfer {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats"
    ledger_type = (
        LedgerType.CUSTOM_JSON_TRANSFER if not fee_transfer else LedgerType.CUSTOM_JSON_FEE
    )
    transfer_ledger_entry = LedgerEntry(
        cust_id=custom_json.cust_id,
        short_id=custom_json.short_id,
        ledger_type=ledger_type,
        group_id=f"{custom_json.group_id}-{ledger_type.value}",
        user_memo=user_memo,
        timestamp=custom_json.timestamp,
        description=description,
        op_type=custom_json.op_type,
        debit=LiabilityAccount(name="VSC Liability", sub=keepsats_transfer.from_account),
        debit_conv=custom_json.conv,
        debit_amount=debit_credit_amount,
        debit_unit=Currency.MSATS,
        credit=LiabilityAccount(name="VSC Liability", sub=keepsats_transfer.to_account),
        credit_conv=custom_json.conv,
        credit_unit=Currency.MSATS,
        credit_amount=debit_credit_amount,
    )
    # TODO: #144 need to look into where else `user_memo` needs to be used
    await transfer_ledger_entry.save()
    ledger_entries.append(transfer_ledger_entry)
    return_details = None
    if keepsats_transfer.parent_id:
        parent_op = await load_tracked_object(tracked_obj=keepsats_transfer.parent_id)
        if (
            getattr(parent_op, "cust_id", None)
            and parent_op
            and parent_op.op_type
            in [
                "transfer",
                "recurrent_transfer",
                "fill_recurrent_transfer",
            ]
            and not fee_transfer
        ):
            return_details = HiveReturnDetails(
                tracked_op=parent_op,
                original_memo=keepsats_transfer.memo,
                reason_str=description,
                action=ReturnAction.CHANGE,
                pay_to_cust_id=parent_op.cust_id,
                amount=parent_op.change_amount,
                nobroadcast=nobroadcast,
            )
    elif not fee_transfer:
        # If there is no parent_id, we assume this is a new transfer but we don't acknowledge fees
        return_details = HiveReturnDetails(
            tracked_op=custom_json,
            original_memo=keepsats_transfer.memo,
            reason_str=description,
            action=ReturnAction.CUSTOM_JSON,
            pay_to_cust_id=keepsats_transfer.to_account,
            nobroadcast=nobroadcast,
        )

    if fee_transfer:
        await TrackedBaseModel.update_quote()
        fee_direction = custom_json.fee_direction
        quote = TrackedBaseModel.last_quote
        fee_conv = CryptoConversion(
            value=keepsats_transfer.msats, conv_from=Currency.MSATS, quote=quote
        ).conversion
        cust_id = custom_json.from_account
        ledger_type = LedgerType.FEE_INCOME
        fee_ledger_entry = LedgerEntry(
            short_id=custom_json.short_id,
            op_type=custom_json.op_type,
            cust_id=cust_id,
            ledger_type=ledger_type,
            group_id=f"{custom_json.group_id}-{ledger_type.value}",
            timestamp=datetime.now(tz=timezone.utc),
            description=f"Fee for Keepsats {keepsats_transfer.msats / 1000:,.0f} sats for {cust_id}",
            debit=LiabilityAccount(
                name="VSC Liability",
                sub=server_id,
            ),
            debit_unit=Currency.MSATS,
            debit_amount=keepsats_transfer.msats,
            debit_conv=fee_conv,
            credit=RevenueAccount(
                name="Fee Income Keepsats",
                sub=fee_direction,
            ),
            user_memo=f"NEED TO SET USER MEMO {ledger_type.printout}",
            credit_unit=Currency.MSATS,
            credit_amount=keepsats_transfer.msats,
            credit_conv=fee_conv,
        )
        await fee_ledger_entry.save()
        ledger_entries.append(fee_ledger_entry)
        if keepsats_transfer.parent_id:
            parent_op = await load_tracked_object(tracked_obj=keepsats_transfer.parent_id)
            if parent_op:
                await release_keepsats(tracked_op=parent_op, fee=True)

    if return_details:
        trx = await reply_with_hive(
            details=return_details,
            nobroadcast=nobroadcast,
        )

    return ledger_entries
