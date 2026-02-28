from typing import Any, Dict

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.actions.tracked_models import ReplyType
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb
from v4vapp_backend_v2.hive.hive_extras import (
    CustomJsonSendError,
    HiveTransferError,
    get_hive_amount_from_trx_reply,
    get_verified_hive_client,
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.lock_str_class import CustIDType, LockStr

MEMO_FOOTER = " | Thank you for using v4v.app"


async def check_for_outstanding_hive_balance(cust_id: CustIDType, amount: Amount) -> Amount:
    """
    Asynchronously checks for outstanding HIVE or HBD balance for a given customer.

    This function retrieves the customer's account balance and calculates the net amount
    by subtracting the relevant balance (HIVE or HBD) from the provided amount. If the net
    amount is negative, it returns a minimum amount of 0.001 for the respective symbol.
    Otherwise, it returns the net amount formatted to 3 decimal places. If the symbol
    is neither HIVE nor HBD, it returns the original amount unchanged.

    Parameters:
        cust_id (CustIDType): The customer ID to check the balance for.
        amount (Amount): The amount we are sending to be reduced if the account is in deficit.

    Returns:
        Amount: The outstanding amount after adjustment, or the original amount if symbol is invalid.
    """
    _, account_balance = await keepsats_balance(cust_id)
    # looking for a positive hive or hbd balance.

    # Override checks for sending to the app's own account, we want to allow sending the full amount back to the app even if it looks like they have a negative balance, because this is likely just them paying back a previous transfer.
    if cust_id == "v4vapp.sus":
        return amount
    to_send = amount
    if amount.symbol == "HIVE":
        net = account_balance.hive_amount - amount
        if net < Amount("0.000 HIVE"):
            to_send = amount + net

    elif amount.symbol == "HBD":
        net = account_balance.hbd_amount - amount
        if net < Amount("0.000 HBD"):
            to_send = amount + net

    if to_send < Amount(f"0.001 {amount.symbol}"):
        to_send = Amount(f"0.001 {amount.symbol}")

    return to_send


async def reply_with_hive(details: HiveReturnDetails, nobroadcast: bool = False) -> Dict[str, str]:
    """
    Processes a Hive return or notification based on the provided details.

    Depending on the action specified in `details`, this function either sends a Hive transfer or a custom JSON notification.
    It verifies the recipient's Hive account, constructs the memo, executes the transfer or notification, updates conversion
    information, and attaches a reply to the original tracked operation.

    Args:
        details (HiveReturnDetails): The details of the Hive return or notification, including recipient, amount, reason, and tracked operation.
        nobroadcast (bool, optional): If True, the transaction will not be broadcast to the Hive network. Defaults to False.

    Returns:
        Dict[str, str]: The transaction result dictionary, containing information such as transaction ID and operation details.

    Side Effect:
        Adds a reply to the original transaction

    Raises:
        HiveTransferError: If the recipient customer ID is not a valid Hive account.
    """
    logger.debug(
        f"Replying with Hive details: {details.original_memo}", extra={"notification": False}
    )
    # decide whether we are allowed to send a Hive transfer at all
    if not LockStr(details.pay_to_cust_id).is_hive:
        logger.warning(
            f"Tracked operation customer ID {details.pay_to_cust_id} is not a valid Hive account.",
            extra={"notification": False, **details.tracked_op.log_extra},
        )
        send_hive = False
    else:
        send_hive = True

    # callers may explicitly request that we always use a custom_json instead of
    # a direct Hive transfer (useful for very small amounts where the transfer
    # fee would eat the value or when we simply want a notification).
    if details.force_custom_json:
        logger.debug(
            "force_custom_json flag set, will send custom_json instead of Hive transfer",
            extra={"notification": False, **details.tracked_op.log_extra},
        )
        send_hive = False

    # raise HiveNotHiveAccount(
    #     f"Tracked operation customer ID {details.pay_to_cust_id} is not a valid Hive account."
    # )

    logger.debug(
        f"Processing return/change for: {details.tracked_op.group_id}",
        extra={"notification": False, **details.tracked_op.log_extra},
    )
    # This is where we will deal with the inbound memo for # clean need to do this.

    amount = Amount("0.001 HIVE")

    if details.action in [ReturnAction.REFUND, ReturnAction.CHANGE]:
        if details.amount:
            amount = Amount(str(details.amount)) or Amount("0.001 HIVE")
        if details.tracked_op and getattr(details.tracked_op, "change_amount", None):
            amount = details.tracked_op.change_amount.beam or Amount("0.001 HIVE")
        else:
            logger.debug(
                "No change amount found in tracked operation, using default amount.",
                extra={"notification": False, **details.tracked_op.log_extra},
            )

    if details.action == ReturnAction.CONVERSION:
        amount = Amount(str(details.amount))

    if getattr(details.tracked_op, "change_memo", None):
        memo = str(details.tracked_op.change_memo)
    else:
        memo = details.reason_str if details.reason_str else "No reason provided"

    memo += f" | § {details.tracked_op.short_id}"

    if not details.clean:
        memo += f"{MEMO_FOOTER}"

    hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)

    # NORMALLY we send Hive transfers back but if this was initiated by a custom JSON, we send
    # a custom JSON back to the original sender.
    # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
    # We Override for conversions because those will be set off by custom_json
    return_amount_msat = 0
    trx: Dict[str, Any] = {}
    reply_type = ReplyType.UNKNOWN
    error_message = ""

    # Only send a Hive transfer if we're allowed (send_hive True), the tracked
    # operation isn't already a custom_json, and we're not requesting a custom
    # json reply via the force flag.  conversions always go out as transfers even
    # though they may originate as custom_json.
    if send_hive and (
        details.tracked_op.op_type != "custom_json" or details.action == ReturnAction.CONVERSION
    ):
        reply_type = ReplyType.TRANSFER
        trx = {}
        adjusted_amount = await check_for_outstanding_hive_balance(
            cust_id=details.pay_to_cust_id, amount=amount
        )
        try:
            trx = await send_transfer(
                hive_client=hive_client,
                from_account=server_account_name,
                to_account=details.pay_to_cust_id,  # Repay to the original sender
                amount=adjusted_amount,
                memo=memo,
            )
        except HiveTransferError as e:
            error_message = f"Failed to send Hive transfer: {e}"
            logger.error(
                error_message,
                extra={"notification": True, **details.tracked_op.log_extra},
            )
        if not error_message:
            return_amount = get_hive_amount_from_trx_reply(trx)
            await TransferBase.update_quote()
            details.tracked_op.change_conv = CryptoConversion(
                conv_from=return_amount.symbol,
                amount=return_amount,
                quote=TransferBase.last_quote,
            ).conversion
            return_amount_msat = int(details.tracked_op.change_conv.msats)

    # Custom JSONs are used for notifications and do not have a sats amount
    elif details.tracked_op.op_type == "custom_json" or not send_hive:
        reply_type = ReplyType.CUSTOM_JSON
        notification = KeepsatsTransfer(
            from_account=server_account_name,
            memo=memo,
            to_account=details.pay_to_cust_id,
            msats=details.msats,
            invoice_message=details.original_memo,
            parent_id=details.tracked_op.group_id,
            notification=True,
        )
        if details.msats and details.msats > 0:
            custom_json_id = InternalConfig().config.hive.custom_json_prefix + "_transfer"
        else:
            custom_json_id = InternalConfig().config.hive.custom_json_prefix + "_notification"
        try:
            trx = await send_custom_json(
                json_data=notification.model_dump(exclude_none=True, exclude_unset=True),
                send_account=server_account_name,
                active=True,
                id=custom_json_id,
                hive_client=hive_client,
            )
            return_amount_msat = 0  # Custom JSON does not have a return amount in msats
        except CustomJsonSendError as e:
            error_message = f"Failed to send Hive custom_json: {e}"
            logger.error(
                error_message,
                extra={"notification": True, **notification.log_extra},
            )

    if details.tracked_op and trx:
        reason = f"Reply for operation {details.tracked_op.group_id}: {trx.get('trx_id', '')}"
        details.tracked_op.add_reply(
            reply_id=trx.get("trx_id", ""),
            reply_type=reply_type,
            reply_msat=return_amount_msat,
            reply_error=None,
            reply_message=reason,
        )
        await details.tracked_op.save()

        logger.debug(
            "Updated tracked_op with reply",
            extra={"notification": False, **details.tracked_op.log_extra},
        )
        return trx
    elif details.tracked_op and error_message:
        details.tracked_op.add_reply(
            reply_id="",
            reply_type=ReplyType.HIVE_ERROR,
            reply_msat=0,
            reply_error=error_message,
            reply_message="Error sending Hive reply",
        )
        await details.tracked_op.save()
    return {}


async def send_notification_custom_json(
    tracked_op: TrackedAny,
    notification: KeepsatsTransfer,
) -> Dict[str, str]:
    """
    Sends a custom JSON notification for a Keepsats transfer using the Hive blockchain.

    Args:
        notification (KeepsatsTransfer): The Keepsats transfer notification data to be sent.

    Returns:
        Dict[str, str]: The transaction result if successful, otherwise an empty dictionary.

    Raises:
        Exception: Logs and handles any exceptions that occur during the notification process.
    """
    try:
        hive_client = await get_verified_hive_client_for_accounts([notification.from_account])
        trx = await send_custom_json(
            json_data=notification.model_dump(exclude_none=True, exclude_unset=True),
            send_account=notification.from_account,
            active=True,
            id="v4vapp_dev_notification",
            hive_client=hive_client,
        )
        logger.debug(
            f"Sent custom_json notification for: {notification.log_str} {trx.get('trx_id', '')}",
            extra={"notification": True, **notification.log_extra},
        )
        reason = f"Custom Json reply for operation {tracked_op.group_id}: {trx.get('trx_id', '')}"
        tracked_op.add_reply(
            reply_id=trx.get("trx_id", ""),
            reply_type=ReplyType.CUSTOM_JSON,
            reply_msat=0,
            reply_error=None,
            reply_message=reason,
        )
        await tracked_op.save()
        reply = tracked_op.replies[-1] if tracked_op.replies else ""
        logger.debug(
            f"Updated tracked_op with reply: {reply}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        return trx
        # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
    except Exception as e:
        logger.error(
            f"Error sending custom_json notification: {e}",
            extra={"notification": False, **notification.log_extra},
        )
        return {}


async def send_transfer_custom_json(
    transfer: KeepsatsTransfer,
    nobroadcast: bool = False,
) -> Dict[str, str]:
    """
    Sends a custom JSON transfer on the Hive blockchain.
    The get_verified_hive_client function will handle the account verification and use Server keys if
    this is a customer to customer or customer to server transfer.

    Args:
        from_account (str): The Hive account sending the transfer.
        to_account (str): The Hive account receiving the transfer.
        amount (Amount): The amount to be transferred.
        memo (str, optional): The memo for the transfer. Defaults to an empty string.
        nobroadcast (bool, optional): If True, the transaction will not be broadcasted. Defaults to False.

    Returns:
        Dict[str, str]: The transaction result if successful, otherwise an empty dictionary.
    """
    try:
        hive_config = InternalConfig().config.hive
        hive_client = await get_verified_hive_client_for_accounts(
            [transfer.from_account, transfer.to_account], nobroadcast=nobroadcast
        )
        if hive_config.hive_accs.get(transfer.from_account):
            send_from = transfer.from_account
        else:
            send_from = InternalConfig().server_id
        # TODO: #169 add pending for custom_json
        json_data = transfer.model_dump(exclude_none=True, exclude_unset=True)
        json_data_converted = convert_decimals_for_mongodb(json_data)
        id = InternalConfig().config.hive.custom_json_prefix + "_transfer"
        trx = await send_custom_json(
            json_data=json_data_converted,
            send_account=send_from,
            active=True,
            id=id,
            hive_client=hive_client,
            nobroadcast=nobroadcast,
        )
        logger.debug(
            f"Sent custom_json transfer: {transfer.log_str} {trx.get('trx_id', '')}",
            extra={"notification": False, **transfer.log_extra},
        )
        return trx
    # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
    except Exception as e:
        logger.exception(
            f"Error sending custom_json transfer: {e}",
            extra={"notification": False, **transfer.log_extra},
        )
        return {}
