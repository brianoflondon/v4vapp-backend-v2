from typing import Any, Dict

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.magi.magi_classes import MagiBTCTransferEvent
from v4vapp_backend_v2.process.hive_notification import send_magi_transfer_custom_json


async def send_magi_transaction(
    vsc_payload: VSCCallPayload, caller: str | None = None, nobroadcast: bool = False
) -> Dict[str, Any]:
    """
    Send a Magi transfer transaction using the provided VSCCallPayload.
    Args:
        vsc_payload (VSCCallPayload): The payload for the Magi transfer.
        caller (str | None): The account name of the caller. If None, it will default to the server's account.
        nobroadcast (bool): If True, the transaction will not be broadcasted to the network.
    Returns:
        Dict[str, Any]: The result of the transaction, including the transaction ID if successful.
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


async def find_magi_btc(custom_json: CustomJson) -> MagiBTCTransferEvent | None:
    """
    Look for Magi BTC transfer event in the magi_btc collection to see if a given custom_json
    has been processed and accepted by the Magi system.
    Args:
        custom_json (CustomJson): The custom JSON data to analyze for Magi BTC transfer events.
    Returns:
        MagiBTCTransferEvent | None: The Magi BTC transfer event if found, otherwise None.
    """
    if not custom_json or custom_json.cj_id != "vsc.call":
        logger.warning(
            "No valid custom JSON provided to find_magi_btc.",
            extra={"notification": False},
        )
        return None

    try:
        trx_id = custom_json.trx_id
        btc_event_raw = await MagiBTCTransferEvent.collection().find_one({
            "indexer_tx_hash": trx_id
        })
        if btc_event_raw:
            btc_event = MagiBTCTransferEvent.model_validate(btc_event_raw)
            logger.info(
                f"Found Magi BTC transfer event for trx_id {trx_id}: {btc_event.log_str}",
                extra={**custom_json.log_extra, **btc_event.log_extra},
            )
            time_delta = btc_event.timestamp - custom_json.timestamp
            logger.info(
                f"Time delta between custom JSON and Magi BTC event: {time_delta.total_seconds()} seconds",
                extra={
                    "custom_json_time": custom_json.timestamp,
                    "btc_event_time": btc_event.timestamp,
                },
            )
            return btc_event
        else:
            logger.info(
                f"No Magi BTC transfer event found for trx_id {trx_id}",
                extra={"notification": False, **custom_json.log_extra},
            )

        return None

    except Exception as e:
        logger.error(
            f"Error while looking for Magi BTC transfer events: {e}",
            extra={"custom_json": custom_json.json},
        )


if __name__ == "__main__":
    import asyncio

    from v4vapp_backend_v2.database.db_pymongo import DBConn

    async def main_test():
        InternalConfig(config_filename="devhive.config.yaml")
        db_conn = DBConn()
        await db_conn.setup_database()

        trx_id = "cfd355ea1312372461597191f9fbf803f01e84a4"

        custom_json_raw = await CustomJson.collection().find_one({"trx_id": trx_id})
        if custom_json_raw:
            custom_json = CustomJson.model_validate(custom_json_raw)
            print(f"Found custom JSON for trx_id {trx_id}: {custom_json.log_str}")

        btc_event = await find_magi_btc(custom_json=custom_json)

        print(
            btc_event.log_str
            if btc_event
            else f"No Magi BTC transfer event found for trx_id {trx_id}"
        )

    asyncio.run(main_test())
