from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.pay_req import PayReq


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


async def process_hive_to_lightning(op: TransferBase) -> None:
    """
    Process the Hive to Lightning operation.
    """
    # Placeholder for actual implementation
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
                    pay_req = await decode_incoming_payment_message(op.memo)
                    if pay_req:
                        pass
                    else:
                        logger.warning(
                            "Failed to decode Lightning invoice",
                            extra={"notification": False, "op": op.model_dump()},
                        )
                except Exception as e:
                    logger.error(
                        f"Error decoding Lightning invoice: {e}",
                        extra={"notification": False, "op": op.model_dump()},
                    )
                    raise HiveToLightningError(f"Error decoding Lightning invoice: {e}")
            else:
                logger.warning(
                    "Failed to decode Lightning invoice",
                    extra={"notification": False, "op": op.model_dump()},
                )



async def decode_incoming_payment_message(message: str) -> PayReq | None:
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
    logger.info(f"Processing payment request: {message}")
    lnd_client = LNDClient(connection_name=lnd_config.default)
    pay_req = await decode_any_lightning_string(input=message, lnd_client=lnd_client)
    if pay_req and pay_req.value:
        # This is where we will check the value of the invoice, the amount of Hive sent
        # The fee and the usage of the user.
        logger.info(
            f"Decoded Lightning invoice: {pay_req}",
            extra={"notification": False, "pay_req": pay_req.model_dump()},
        )
        sats_to_send = pay_req.value
        send_conversion = CryptoConversion(conv_from=Currency.SATS, value=sats_to_send)
        await send_conversion.get_quote()
        pay_req.conv = send_conversion.conversion
        if pay_req.conv.in_limits:
            logger.info(
                f"Pay request decoded {pay_req.memo}", extra={"pay_req": pay_req.model_dump()}
            )
            return pay_req
    return None
