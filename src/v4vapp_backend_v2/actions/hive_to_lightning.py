from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient


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
        logger.warning("Hive or LND configuration is missing.")
        return
    server_account = hive_config.server_account.name

    if op.to_account == server_account:
        # Process the operation
        if await check_for_hive_to_lightning(op):
            # Placeholder for actual processing logic
            logger.info(
                f"Processing operation to {server_account} ({op.from_account} -> {op.to_account})",
                extra={"notification": False, "op": op.model_dump()},
            )
            lnd_client = LNDClient(connection_name=lnd_config.default)
            pay_req = await decode_any_lightning_string(input=op.d_memo, lnd_client=lnd_client)
            if pay_req:
                logger.info(
                    f"Decoded Lightning invoice: {pay_req}",
                    extra={"notification": False, "op": op.model_dump()},
                )
            else:
                logger.warning(
                    "Failed to decode Lightning invoice",
                    extra={"notification": False, "op": op.model_dump()},
                )

        # first double check this is a transfer to the server account
    else:
        logger.warning(
            f"Operation to sent to {server_account} ({op.from_account} -> {op.to_account})",
            extra={"notification": False, "op": op.model_dump()},
        )
