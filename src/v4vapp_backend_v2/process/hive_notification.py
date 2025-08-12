from typing import Any, Dict

from nectar.amount import Amount

from v4vapp_backend_v2.actions.cust_id_class import CustID, CustIDType
from v4vapp_backend_v2.actions.tracked_any import TrackedAny
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.hive.hive_extras import (
    HiveTransferError,
    get_verified_hive_client,
    get_verified_hive_client_for_accounts,
    send_custom_json,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.process.process_errors import (
    HiveToLightningError,
    KeepsatsDepositNotificationError,
)

MEMO_FOOTER = " | Thank you for using v4v.app"


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
    logger.info(
        f"Replying with Hive details: {details.original_memo}", extra={"notification": False}
    )
    if not CustID(details.pay_to_cust_id).is_hive:
        logger.error(
            "Tracked operation customer ID is not a valid Hive account.",
            extra={"notification": False, **details.tracked_op.log_extra},
        )
        raise HiveTransferError("Tracked operation customer ID is not a valid Hive account.")

    logger.info(
        f"Processing return/change for: {details.tracked_op.group_id}",
        extra={"notification": False, **details.tracked_op.log_extra},
    )
    # This is where we will deal with the inbound memo for # clean need to do this.

    amount = Amount("0.001 HIVE")

    if details.action in [ReturnAction.REFUND, ReturnAction.CHANGE]:
        amount = Amount(str(details.amount)) or Amount("0.001 HIVE")
        if details.tracked_op.change_amount:
            amount = details.tracked_op.change_amount.beam or Amount("0.001 HIVE")
        else:
            logger.warning(
                "No change amount found in tracked operation, using default amount.",
                extra={"notification": False, **details.tracked_op.log_extra},
            )

    if details.action == ReturnAction.CONVERSION:
        amount = Amount(str(details.amount))

    if details.tracked_op.change_memo:
        memo = details.tracked_op.change_memo
    else:
        memo = details.reason_str if details.reason_str else "No reason provided"

    memo += f" | § {details.tracked_op.short_id}{MEMO_FOOTER}"

    hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)

    # NORMALLY we send Hive transfers back but if this was initiated by a custom JSON, we send
    # a custom JSON back to the original sender.
    # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
    # We Override for conversions because those will be set off by custom_json
    if details.action == ReturnAction.CONVERSION or details.tracked_op.op_type != "custom_json":
        trx: Dict[str, Any] = await send_transfer(
            hive_client=hive_client,
            from_account=server_account_name,
            to_account=details.pay_to_cust_id,  # Repay to the original sender
            amount=amount,
            memo=memo,
        )
        try:
            op_dict = trx["operations"][0][1]  # type: dict
            return_amount = Amount(op_dict["amount"])
        except (KeyError, IndexError):
            return_amount = Amount("0.001 HIVE")
        if not return_amount:
            return_amount = Amount("0.001 HIVE")
        await TransferBase.update_quote()
        details.tracked_op.change_conv = CryptoConversion(
            conv_from=return_amount.symbol,
            amount=return_amount,
            quote=TransferBase.last_quote,
        ).conversion
        return_amount_msat = details.tracked_op.change_conv.msats

    # Custom JSONs are used for notifications and do not have a sats amount
    else:
        notification = KeepsatsTransfer(
            from_account=server_account_name,
            memo=memo,
            to_account=details.pay_to_cust_id,
            invoice_message=details.original_memo,
            parent_id=details.tracked_op.group_id,
            notification=True,
        )
        trx = await send_custom_json(
            json_data=notification.model_dump(exclude_none=True, exclude_unset=True),
            send_account=server_account_name,
            active=True,
            id="v4vapp_dev_notification",
            hive_client=hive_client,
        )
        return_amount_msat = 0  # Custom JSON does not have a return amount in msats

    # # Now add the Hive reply to the original Hive operation
    # reason = (
    #     f"{details.action} for operation {details.tracked_op.group_id}: {trx.get('trx_id', '')}"
    # )
    # details.tracked_op.add_reply(
    #     reply_id=trx.get("trx_id", ""),
    #     reply_type=details.tracked_op.op_type,
    #     reply_msat=return_amount_msat,
    #     reply_error=None,
    #     reply_message=reason,
    # )
    # await details.tracked_op.save()

    return trx


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
        logger.info(
            f"Sending custom_json notification for Keepsats transfer: {notification.log_str}",
            extra={"notification": True, **notification.log_extra},
        )
        hive_client = await get_verified_hive_client_for_accounts([notification.from_account])
        trx = await send_custom_json(
            json_data=notification.model_dump(exclude_none=True, exclude_unset=True),
            send_account=notification.from_account,
            active=True,
            id="v4vapp_dev_notification",
            hive_client=hive_client,
        )
        reason = f"Custom Json reply for operation {tracked_op.group_id}: {trx.get('trx_id', '')}"
        tracked_op.add_reply(
            reply_id=trx.get("trx_id", ""),
            reply_type="custom_json",
            reply_msat=0,
            reply_error=None,
            reply_message=reason,
        )
        await tracked_op.save()
        logger.info(
            f"Updated tracked_op with reply: {tracked_op.replies[-1]}",
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
        logger.info(
            f"Sending custom_json transfer: {transfer.log_str}",
            extra={"notification": True, **transfer.log_extra},
        )
        hive_client = await get_verified_hive_client_for_accounts([transfer.from_account])
        trx = await send_custom_json(
            json_data=transfer.model_dump(exclude_none=True, exclude_unset=True),
            send_account=transfer.from_account,
            active=True,
            id="v4vapp_dev_transfer",
            hive_client=hive_client,
            nobroadcast=nobroadcast,
        )
        return trx
    # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
    except Exception as e:
        logger.exception(
            f"Error sending custom_json transfer: {e}",
            extra={"notification": False, **transfer.log_extra},
        )
        return {}


async def depreciated_send_notification_hive_transfer(
    tracked_op: TrackedAny,
    reason: str,
    clean: bool = False,
    amount: Amount | None = None,
    pay_to_cust_id: CustIDType | None = None,
    nobroadcast: bool = False,
) -> Dict[str, str]:
    """
    Send a notification and process a Hive transfer repayment for a tracked operation.

    This function handles the repayment of a Hive to Lightning operation by sending a transfer
    back to the original Hive account. It logs the process, verifies the customer ID, sends the
    transfer, updates the tracked operation with the reply, and handles errors appropriately.

    Args:
        tracked_op (TrackedAny): The tracked operation object containing details of the original transaction.
        reason (str): The reason for the repayment or notification.
        clean (bool, optional): If True send the bare reason without extra metadata.
        amount (Amount | None, optional): The amount to be transferred. Defaults to "0.001 HIVE" if not provided.
        pay_to_cust_id (CustIDType | None, optional): The customer ID to send the repayment to. If not provided, it is extracted from the tracked operation.
        nobroadcast (bool, optional): If True, the transaction will not be broadcasted to the Hive network. Defaults to False.

    Returns:
        Dict[str, str]: The transaction dictionary returned by the Hive client upon successful transfer.

    Raises:
        KeepsatsDepositNotificationError: If the customer ID is missing or invalid.
        HiveToLightningError: If the transfer fails or an unexpected error occurs.

    """
    if not pay_to_cust_id:
        pay_to_cust_id = getattr(tracked_op, "cust_id", None)
    if not pay_to_cust_id:
        logger.error(
            "Tracked operation does not have a customer ID.",
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise KeepsatsDepositNotificationError("Tracked operation does not have a customer ID.")

    if not CustID(pay_to_cust_id).is_hive:
        logger.error(
            "Tracked operation customer ID is not a valid Hive account.",
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise KeepsatsDepositNotificationError(
            "Tracked operation customer ID is not a valid Hive account."
        )

    logger.info(
        f"Processing return/change for: {tracked_op.log_str}",
        extra={"notification": False, **tracked_op.log_extra},
    )
    logger.info(
        f"Reason: {reason} amount: {amount}",
        extra={"reason": reason, "amount": amount, "nobroadcast": nobroadcast},
    )
    hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)

    # We don't check the operation was already paid here because that is done in the processing function
    amount = Amount("0.001 HIVE") if amount is None else amount
    try:
        if clean:
            memo = reason
        else:
            memo = f"{reason} | § {tracked_op.short_id}{MEMO_FOOTER}"
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=server_account_name,
            to_account=pay_to_cust_id,  # Repay to the original sender
            amount=amount,
            memo=memo,
        )
        if trx:
            # MARK: 5. Update tracked_op
            logger.info(
                f"Successfully paid reply to Hive to Lightning operation: {tracked_op.log_str}",
                extra={
                    "notification": True,
                    "trx": trx,
                    **tracked_op.log_extra,
                },
            )
            try:
                return_amount = Amount(trx["operations"][0][1]["amount"])
            except (KeyError, IndexError):
                return_amount = Amount("0.001 HIVE")
            if not return_amount:
                return_amount = Amount("0.001 HIVE")
            await TransferBase.update_quote()
            tracked_op.change_conv = CryptoConversion(
                conv_from=return_amount.symbol,
                amount=return_amount,
                quote=TransferBase.last_quote,
            ).conversion
            return_amount_msat = tracked_op.change_conv.msats
            # Now add the Hive reply to the original Hive transfer operation
            reason = (
                f"Change transaction for operation {tracked_op.group_id}: {trx.get('trx_id', '')}"
            )
            tracked_op.add_reply(
                reply_id=trx.get("trx_id", ""),
                reply_type="transfer",
                reply_msat=return_amount_msat,
                reply_error=None,
                reply_message=reason,
            )
            await tracked_op.save()
            logger.info(
                f"Updated tracked_op with reply: {tracked_op.replies[-1]}",
                extra={"notification": False, **tracked_op.log_extra},
            )
            return trx
        else:
            raise HiveTransferError("No transaction created during Hive to Lightning repayment")
    except HiveTransferError as e:
        # TODO: #151 Important: this Hive transfer needs to be stored and reprocessed later if it fails for balance or network issues
        message = f"Failed to repay Hive to Lightning operation: {e}"
        tracked_op.add_reply(
            reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
        )
        await tracked_op.save()
        logger.error(
            message,
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise HiveToLightningError(message)

    except Exception as e:
        message = f"Unexpected error during Hive to Lightning repayment: {e}"
        tracked_op.add_reply(
            reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
        )
        await tracked_op.save()
        logger.error(
            message,
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise HiveToLightningError(message)
