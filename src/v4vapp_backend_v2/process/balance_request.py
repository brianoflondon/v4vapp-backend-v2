from decimal import Decimal

from colorama import Fore, Style
from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_balances import keepsats_balance
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.hive.hive_extras import (
    HiveTransferError,
    get_verified_hive_client,
    send_transfer,
)
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
    net_msats, account_balance = await keepsats_balance(
        cust_id=transfer.cust_id, line_items=False, notifications=False
    )

    net_sats = net_msats / Decimal(1000)

    return_details_str = (
        f"Balance request received for {transfer.cust_id}. Current balance is {net_sats:.3f} sats."
    )
    return_details_dict = {
        "balance_string": return_details_str,
        "msats": int(net_msats),
        "sats": f"{net_sats:.3f}",
        "reply_to": transfer.short_id,
        "original_memo": transfer.d_memo,
    }

    transfer.change_memo = f"#{str(return_details_dict)}"
    await transfer.save()
    adjusted_amount = await check_for_outstanding_hive_balance(
        cust_id=transfer.cust_id, amount=amount
    )

    try:
        hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=server_account_name,
            to_account=transfer.to_account,  # Repay to the original sender
            amount=adjusted_amount,
            memo=transfer.change_memo,
        )
        trx_id = trx.get("trx_id", "unknown")
        logger.info(
            f"{Fore.WHITE}Reply with Hive transfer to {transfer.cust_id} {transfer.short_id} {trx_id}{Style.RESET_ALL}",
            extra={
                "notification": False,
                "trx": trx,
                **transfer.log_extra,
            },
        )
    except HiveTransferError as e:
        error_message = f"Failed to send Hive transfer: {e}"
        logger.error(
            error_message,
            extra={"notification": True, **transfer.log_extra},
        )
