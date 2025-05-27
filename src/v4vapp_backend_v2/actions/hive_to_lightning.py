import asyncio

from v4vapp_backend_v2.actions.lnurl_decode import LnurlException, decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive.hive_extras import HiveTransferError, get_hive_client, send_transfer
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentExpired, send_lightning_to_pay_req
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment

MEMO_FOOTER = " - Thank you for using v4v.app"


class HiveToLightningError(Exception):
    """
    Custom exception for Hive to Lightning errors.
    """

    pass


async def check_for_hive_to_lightning(op: OpAny) -> bool:
    """
    Check if the Hive to Lightning process is running.
    """
    # Placeholder for actual implementation
    return True


async def can_attempt_to_pay(pay_req: PayReq) -> bool:
    """
    Check the status of the invoice.
    """
    # Placeholder for actual implementation
    async with TransferBase.db_client as db_client:
        invoice = await db_client.find_one("invoices", {"payment_request": pay_req.pay_req_str})
        if not invoice:
            logger.info(
                f"Invoice {pay_req.pay_req_str} not found in the database.",
                extra={"notification": False, "pay_req": pay_req.model_dump()},
            )
            return True
        if invoice.status != "open":
            logger.warning(
                f"Invoice {pay_req.pay_req_str} is not open, current status: {invoice.status}.",
                extra={"notification": False, "pay_req": pay_req.model_dump()},
            )
            return False
    return True


async def process_hive_to_lightning(op: TransferBase, nobroadcast: bool = False) -> None:
    """
    Process a transfer operation from Hive to Lightning.

    This asynchronous function handles the workflow for transferring funds from a Hive account to a Lightning Network destination. It performs the following steps:
    - Checks if the operation is already locked and skips processing if so.
    - Validates the presence of required Hive and Lightning configuration.
    - Ensures the operation is intended for the server account.
    - Checks if the operation is eligible for processing.
    - Decodes the Lightning invoice from the operation memo.
    - Ensures conversion details are present or attempts to update them.
    - Initiates the Lightning payment using the decoded invoice and conversion details.
    - Handles exceptions related to invoice decoding and payment expiration, logging and raising appropriate errors or triggering repayment as needed.

    Args:
        op (TransferBase): The transfer operation to process.
        nobroadcast (bool, optional): If True, prevents broadcasting the transaction. Defaults to False.

    Raises:
        HiveToLightningError: If configuration is missing, invoice decoding fails, or conversion details are unavailable.

    """
    if op.locked:
        logger.info(
            f"Operation is already locked, skipping processing. {op.log_str}",
            extra={"notification": False, "op": op.model_dump()},
        )
        raise HiveToLightningError(f"Operation is locked: {op.locked}")
    if op.reply_id:
        logger.info(
            f"Operation already has a reply transaction, skipping processing. {op.log_str}",
            extra={"notification": False, "op": op.model_dump()},
        )
        raise HiveToLightningError(f"Operation already has a reply transaction: {op.reply_id}")
    hive_config = InternalConfig().config.hive
    lnd_config = InternalConfig().config.lnd_config
    if (
        not hive_config
        or not lnd_config
        or not lnd_config.default
        or not hive_config.server_account
    ):
        # Log a warning if the configuration is missing
        message = f"Missing configuration details for Hive or LND: {hive_config}, {lnd_config}"
        logger.warning(message, extra={"notification": False})
        raise HiveToLightningError(message)
    async with op:
        server_account = hive_config.server_account.name
        if op.to_account == server_account:
            # Process the operation
            if await check_for_hive_to_lightning(op):
                # Placeholder for actual processing logic
                logger.info(
                    f"Processing operation to {server_account} ({op.from_account} -> {op.to_account})",
                    extra={"notification": False, "op": op.model_dump()},
                )
                if op.memo:
                    try:
                        pay_req = await decode_incoming_payment_message(op=op)
                        if not pay_req:
                            raise HiveToLightningError("Failed to decode Lightning invoice")
                        if op.conv is None or op.conv.is_unset():
                            logger.warning(
                                f"Conversion details missing for operation: {op.memo}",
                                extra={"notification": False, **op.log_extra},
                            )
                            await op.update_conv()
                        if not op.conv:
                            logger.error(
                                "Conversion details not found for operation, failed to update conversion.",
                                extra={"notification": False, **op.log_extra},
                            )
                            raise HiveToLightningError(
                                "Conversion details not found for operation"
                            )
                        await send_lightning_to_pay_req(
                            pay_req=pay_req,
                            lnd_client=LNDClient(connection_name=lnd_config.default),
                            chat_message=op.group_id,
                            group_id=op.group_id,
                            async_callback=lightning_payment_sent,
                            callback_args={"op": op},
                            amount_msat=op.conv.msats - op.conv.msats_fee,
                            fee_limit_ppm=500,
                        )

                    except LnurlException as e:
                        logger.info(
                            f"Error decoding Lightning invoice: {e}",
                            extra={"notification": False, "op": op.model_dump()},
                        )
                        raise HiveToLightningError(f"Error decoding Lightning invoice: {e}")

                    except LNDPaymentExpired as e:
                        logger.warning(
                            f"Lightning payment expired: {e}",
                            extra={"notification": False, "op": op.model_dump()},
                        )
                        asyncio.create_task(
                            return_hive_transfer(
                                op=op, reason="Lightning invoice expired", nobroadcast=nobroadcast
                            )
                        )

                else:
                    logger.warning(
                        "Failed to decode Lightning invoice",
                        extra={"notification": False, "op": op.model_dump()},
                    )


async def decode_incoming_payment_message(op: TransferBase) -> PayReq | None:
    """
    Decodes an incoming Lightning payment message and validates its value and conversion limits.

    Args:
        message (str): The Lightning payment request string to decode.

    Returns:
        PayReq | None: The decoded payment request object with conversion details if valid and within limits, otherwise None.

    Logs:
        - Information about the processing and decoding of the payment request.
        - Details about the decoded invoice and conversion status.

    Raises:
        None directly, but may propagate exceptions from called methods if not handled elsewhere.
    """

    lnd_config = InternalConfig().config.lnd_config
    logger.info(f"Processing payment request: {op.memo}")
    lnd_client = LNDClient(connection_name=lnd_config.default)
    try:
        pay_req = await decode_any_lightning_string(input=op.memo, lnd_client=lnd_client)
        return pay_req
    except Exception as e:
        logger.error(f"Error decoding Lightning invoice: {e}")
        return None


async def lightning_payment_sent(payment: Payment, op: TransferBase):
    """
    Callback function to be called when a Lightning payment is sent.

    Args:
        payment (Payment): The Payment object representing the sent payment.
        op (TransferBase): The TransferBase object representing the operation.

    Returns:
        None
    """
    # Placeholder for actual implementation
    await op.unlock_op()
    logger.info(
        f"Lightning payment sent: {payment.log_str}",
        extra={"notification": False, "op": op.model_dump(), "payment": payment.model_dump()},
    )


async def return_hive_transfer(op: TransferBase, reason: str, nobroadcast: bool = False) -> None:
    """
    Repay the Hive to Lightning operation.
    This function is called when a Lightning payment fails or expires.
    It attempts to repay the Hive operation by sending the funds back to the original sender.
    """
    # Placeholder for actual implementation
    logger.info(
        f"Repaying Hive to Lightning operation: {op.log_str}",
        extra={"notification": False, "op": op.model_dump()},
    )
    hive_config = InternalConfig().config.hive
    if not hive_config.server_account:
        raise HiveToLightningError("Missing Hive server account configuration for repayment")

    memo_key = hive_config.server_account.memo_key or ""
    active_key = hive_config.server_account.active_key or ""
    if not memo_key or not active_key:
        raise HiveToLightningError("Missing Hive server account keys for repayment")

    hive_client = get_hive_client(
        keys=[
            hive_config.server_account.memo_key,
            hive_config.server_account.active_key,
        ],
        nobroadcast=nobroadcast,
    )
    try:
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=hive_config.server_account.name,
            to_account=op.from_account,  # Repay to the original sender
            amount=op.amount.beam,
            memo=f"{reason} - {op.group_id}{MEMO_FOOTER}",
        )
        if trx:
            # Need to updated the original operation with the reply transactions
            op.reply_id = trx.get("trx_id", "")
            save_result = await op.save()
            logger.info(
                f"Successfully repaid Hive to Lightning operation: {op.reply_id}",
                extra={
                    "notification": True,
                    "trx": trx,
                    "save_result": save_result,
                    **op.log_extra,
                },
            )
    except HiveTransferError as e:
        op.reply_error = e
        await op.save()
        logger.error(
            f"Failed to repay Hive to Lightning operation: {e}",
            extra={"notification": False, "op": op.model_dump()},
        )
        raise HiveToLightningError(f"Failed to repay Hive to Lightning operation: {e}")
    # Logic to repay the Hive operation
    # This could involve creating a new TransferBase operation to send the funds back
    # to the original sender or handling it according to your application's logic.
