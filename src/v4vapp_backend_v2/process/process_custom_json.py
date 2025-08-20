from colorama import Fore, Style

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance_printout
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry_class import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.tracked_any import load_tracked_object
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.hive_notification import reply_with_hive
from v4vapp_backend_v2.process.process_errors import (
    CustomJsonToLightningError,
    InsufficientBalanceError,
)


async def custom_json_internal_transfer(
    custom_json: CustomJson, keepsats_transfer: KeepsatsTransfer, nobroadcast: bool = False
) -> LedgerEntry:
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
        f"Processing CustomJson transfer: {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats"
    )
    if not keepsats_transfer or not keepsats_transfer.sats:
        raise CustomJsonToLightningError("Keepsats transfer amount is zero.")

    net_msats, account_balance = await keepsats_balance_printout(
        cust_id=keepsats_transfer.from_account
    )
    keepsats_transfer.msats = (
        keepsats_transfer.sats * 1_000 if not keepsats_transfer.msats else keepsats_transfer.msats
    )
    # # From the server -> customer
    # if keepsats_transfer.from_account == InternalConfig().server_id:
    #     if net_msats < keepsats_transfer.msats:
    #         logger.warning(
    #             f"Ignoring low Server Keepsats balance {net_msats // 1000:,.0f} sats is insufficient for transfer of {keepsats_transfer.sats:,} sats.",
    #             extra={"notification": False},
    #         )
    # # From customer -> server
    # else:
    if True:  # Always check keepsats balance
        # Add a buffer of 1 sat 1_000 msats to avoid rounding issues
        if net_msats + 1_000 < keepsats_transfer.msats:
            message = f"Insufficient Keepsats balance for transfer: {keepsats_transfer.from_account} has {net_msats // 1000:,.0f} sats, but transfer requires {keepsats_transfer.sats:,} sats."
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

    user_memo = (
        keepsats_transfer.user_memo
        or f"{keepsats_transfer.to_account} received {keepsats_transfer.sats:,} sats from {keepsats_transfer.from_account}"
    )
    description = f"Transfer {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats"
    ledger_type = LedgerType.CUSTOM_JSON_TRANSFER
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
    else:
        # If there is no parent_id, we assume this is a new transfer
        return_details = HiveReturnDetails(
            tracked_op=custom_json,
            original_memo=keepsats_transfer.memo,
            reason_str=description,
            action=ReturnAction.CUSTOM_JSON,
            pay_to_cust_id=keepsats_transfer.to_account,
            nobroadcast=nobroadcast,
        )

    if return_details:
        trx = await reply_with_hive(
            details=return_details,
            nobroadcast=nobroadcast,
        )

    return transfer_ledger_entry
