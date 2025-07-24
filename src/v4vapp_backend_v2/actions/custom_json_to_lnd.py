from typing import Any, Dict

from v4vapp_backend_v2.accounting.account_balances import get_keepsats_balance
from v4vapp_backend_v2.actions.actions_errors import CustomJsonToLightningError
from v4vapp_backend_v2.actions.hold_release_keepsats import hold_keepsats, release_keepsats
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    LNDPaymentError,
    LNDPaymentExpired,
    send_lightning_to_pay_req,
)


async def process_custom_json_to_lightning(
    custom_json: CustomJson, keepsats_transfer: KeepsatsTransfer, nobroadcast: bool = False
) -> None:
    """ """
    # is this a lightning invoice?
    if not keepsats_transfer.memo:
        raise CustomJsonToLightningError("Keepsats transfer does not have an invoice message.")
    lnd_config = InternalConfig().config.lnd_config
    lnd_client = LNDClient(connection_name=lnd_config.default)
    return_hive_message = ""
    release_hold = True
    try:
        pay_req = await decode_any_lightning_string(
            input=keepsats_transfer.memo,
            lnd_client=lnd_client,
            zero_amount_invoice_send_msats=keepsats_transfer.sats * 1000,
            comment=keepsats_transfer.invoice_message,
        )
        if not pay_req:
            raise CustomJsonToLightningError("Failed to decode Lightning payment request.")

        keepsats_balance, net_sats = await get_keepsats_balance(
            cust_id=keepsats_transfer.from_account
        )
        logger.info(f"Keepsats balance BEFORE Hold: {net_sats:,.0f}")

        if keepsats_balance is None or net_sats is None:
            raise CustomJsonToLightningError("Failed to retrieve Keepsats balance or net sats.")

        if net_sats < pay_req.amount_msat / 1000:
            raise CustomJsonToLightningError(
                f"Insufficient Keepsats balance: {net_sats:,.0f} sats available, {pay_req.amount_msat / 1000:,.0f} sats required."
            )

        await hold_keepsats(
            amount_msats=pay_req.value_msat + pay_req.fee_estimate,
            cust_id=custom_json.cust_id,
            tracked_op=custom_json,
        )

        keepsats_balance, net_sats = await get_keepsats_balance(
            cust_id=keepsats_transfer.from_account
        )
        logger.info(f"Keepsats balance AFTER Hold: {net_sats:,.0f}")

        amount_msats = min(
            keepsats_transfer.sats * 1000, pay_req.amount_msat, int(net_sats * 1000)
        )

        chat_message = f"Sending sats from v4v.app | § {custom_json.short_id} |"
        payment = await send_lightning_to_pay_req(
            pay_req=pay_req,
            lnd_client=lnd_client,
            chat_message=chat_message,
            group_id=custom_json.group_id_p,
            cust_id=custom_json.cust_id,
            paywithsats=True,
            amount_msat=amount_msats,
            fee_limit_ppm=lnd_config.lightning_fee_limit_ppm,
        )
        logger.info(
            f"Lightning payment sent after custom_json {payment.group_id_p}",
            extra={
                "notification": True,
                **custom_json.log_extra,
                **payment.log_extra,
            },
        )
        # If the payment succeeded we do not release the HOLD here, were release it when the payment ledger entries are made
        release_hold = False
        return payment

    except CustomJsonToLightningError as e:
        return_hive_message = f"Error processing Hive to Lightning operation: {e}"
        logger.warning(
            return_hive_message,
            extra={"notification": False, **custom_json.log_extra},
        )

    except LNDPaymentExpired as e:
        return_hive_message = f"Lightning payment expired: {e}"
        logger.warning(
            return_hive_message,
            extra={"notification": False, **custom_json.log_extra},
        )

    except LNDPaymentError as e:
        return_hive_message = f"Lightning payment error: {e}"
        logger.error(
            return_hive_message,
            extra={"notification": False, **custom_json.log_extra},
        )

    except Exception:
        logger.exception(
            "Unexpected error during Hive to Lightning processing",
            extra={"notification": False, **custom_json.log_extra},
        )
        # we don't release a keepsats hold if an unknown error occurred
        release_hold = False

    finally:
        if release_hold:
            await release_keepsats(tracked_op=custom_json)

        if return_hive_message:
            try:
                await custom_json_notification(
                    custom_json=custom_json,
                    reason=return_hive_message,
                    nobroadcast=nobroadcast,
                )
            except Exception as e:
                logger.exception(
                    f"Error returning Hive transfer: {e}",
                    extra={
                        "notification": False,
                        "reason": return_hive_message,
                        **custom_json.log_extra,
                    },
                )


async def custom_json_notification(
    custom_json: CustomJson,
    reason: str,
    nobroadcast: bool = False,
) -> Dict[str, Any]:
    """
    Create a notification payload for a custom JSON operation.
    """
    return {
        "type": "custom_json_notification",
        "short_id": custom_json.short_id,
        "group_id": custom_json.group_id,
        "cust_id": custom_json.cust_id,
        "reason": reason,
        "nobroadcast": nobroadcast,
    }


# Last line of the file
