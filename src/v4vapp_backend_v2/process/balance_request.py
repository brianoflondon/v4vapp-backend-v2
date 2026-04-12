import json
from datetime import datetime, timezone
from decimal import Decimal

from colorama import Fore, Style
from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import ReplyType
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import (
    HiveTransferError,
    get_verified_hive_client,
    send_transfer,
)
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.process.hive_notification import check_for_outstanding_hive_balance


async def reply_with_balance_request(transfer: TrackedTransfer, nobroadcast: bool = False) -> None:
    """
    Reply to a balance request transfer with the current balance of the sender.
    The sender will be the customer because we only get here from a transfer to the
    server account with the balance request memo, which is only sent by the customer.

    Args:
        transfer (TrackedTransfer): The transfer operation to reply to.

    Returns:
        None
    """

    amount = Amount(str(transfer.amount))
    net_msats, _ = await keepsats_balance(
        cust_id=transfer.cust_id, line_items=False, notifications=False
    )

    net_sats = net_msats / Decimal(1000)

    return_details_str = (
        f"Current balance is {net_sats:,.0f} sats | timestamp {datetime.now(tz=timezone.utc).isoformat()} | "
        f"{net_msats} msats | {net_sats:.3f} sats | § {transfer.short_id}"
    )
    return_details_dict = {
        "return_details_str": return_details_str,
        "msats": int(net_msats),
        "sats": f"{net_sats:.3f}",
        "reply_to": transfer.short_id,
        "original_memo": transfer.d_memo,
    }
    logger.info(
        f"{return_details_str}",
        extra={
            "notification": False,
            "return_details_dict": return_details_dict,
            **transfer.log_extra,
        },
    )
    transfer.change_memo = json.dumps(return_details_dict, ensure_ascii=False)
    adjusted_amount = await check_for_outstanding_hive_balance(
        cust_id=transfer.cust_id, amount=amount
    )
    transfer.change_amount = AmountPyd(amount=adjusted_amount)
    await transfer.save()
    try:
        hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=server_account_name,
            to_account=transfer.cust_id,  # Repay to the original sender
            amount=adjusted_amount,
            memo=transfer.change_memo,
            is_private=transfer.balance_request_private,
        )
    except HiveTransferError as e:
        error_message = f"Failed to send Hive transfer: {e}"
        logger.error(
            error_message,
            extra={"notification": True, **transfer.log_extra},
        )
        return
    except Exception as e:
        error_message = f"Unexpected error during balance reply Hive transfer: {e}"
        logger.error(
            error_message,
            extra={"notification": True, **transfer.log_extra},
        )
        return

    trx_id = trx.get("trx_id", "unknown")
    logger.info(
        f"{Fore.WHITE}Reply with Hive transfer to {transfer.cust_id} {transfer.short_id} {trx_id}{Style.RESET_ALL}",
        extra={
            "notification": False,
            "trx": trx,
            **transfer.log_extra,
        },
    )
    msats = int(transfer.conv.msats) if transfer.conv else int(0)
    transfer.add_reply(
        reply_id=trx_id,
        reply_type=ReplyType.TRANSFER,
        reply_message=transfer.change_memo,
        reply_msat=msats,
    )
    await transfer.save()
