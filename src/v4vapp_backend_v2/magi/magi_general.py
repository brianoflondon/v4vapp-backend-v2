from typing import Any, Dict

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.process.hive_notification import send_magi_transfer_custom_json


async def send_magi_transaction(
    vsc_payload: VSCCallPayload, caller: str | None = None, nobroadcast: bool = False
) -> Dict[str, Any]:
    """
    Test sending a Magi transaction by simulating the process of creating a VSCCallPayload and sending it to the Magi API.
    """
    vsc_call = None
    try:
        if caller:
            caller_acc_name = AccName(caller)
        else:
            server_id = InternalConfig().server_id
            caller_acc_name = AccName(server_id)
            caller = f"{caller_acc_name.magi_prefix}"

        vsc_call = VSCCall(
            net_id="vsc-mainnet",
            contract_id="vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
            action="transfer",
            caller=caller_acc_name.magi_prefix,
            payload=vsc_payload,
            rc_limit=2000,
            intents=[],
        )

        trx = await send_magi_transfer_custom_json(
            vsc_call=vsc_call,
            nobroadcast=nobroadcast,
            caller=caller,
        )
        trx_id = trx.get("trx_id", "Failed") if trx else "Failed"
        logger.info(
            f"Sent MAGI transfer custom JSON trx_id: {trx_id}",
            extra={"trx": trx, **vsc_call.log_extra},
        )
        return trx
    except Exception as e:
        if not vsc_call:
            logger.error(
                f"Unexpected error in send_magi_transaction before vsc_call creation: {e}",
                extra={"notification": False},
            )
            return {"error": "Failed to create VSCCall object."}
        logger.error(
            f"Unexpected error in send_magi_transaction: {e}",
            extra={"notification": False, **vsc_call.log_extra},
        )
        return {"error": "Failed to send Magi transaction."}
