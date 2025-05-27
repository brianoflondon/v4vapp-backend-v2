from v4vapp_backend_v2.actions.lnurl_decode import LnurlException, decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentExpired, send_lightning_to_pay_req
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment


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


async def process_hive_to_lightning(op: TransferBase) -> None:
    """
    Process the Hive to Lightning operation.
    """
    # Placeholder for actual implementation
    if op.locked:
        logger.info(
            f"Operation {op.log_str} is already locked, skipping processing.",
            extra={"notification": False, "op": op.model_dump()},
        )
        return
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
                        await repay_hive_to_lightning(op=op)

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


async def repay_hive_to_lightning(op: TransferBase) -> None:
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
    # Logic to repay the Hive operation
    # This could involve creating a new TransferBase operation to send the funds back
    # to the original sender or handling it according to your application's logic.
