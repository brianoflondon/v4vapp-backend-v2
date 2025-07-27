from v4vapp_backend_v2.accounting.account_balances import keepsats_balance_printout
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import CustomJsonToLightningError
from v4vapp_backend_v2.actions.hive_notification import send_notification_hive_transfer
from v4vapp_backend_v2.actions.hold_release_keepsats import hold_keepsats, release_keepsats
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
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

        net_sats, keepsats_balance = await keepsats_balance_printout(
            cust_id=keepsats_transfer.from_account
        )

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

        net_sats_after, keepsats_balance = await keepsats_balance_printout(
            cust_id=keepsats_transfer.from_account, previous_sats=net_sats
        )

        amount_msats = min(
            keepsats_transfer.sats * 1000, pay_req.amount_msat, int(net_sats_after * 1000)
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
        return

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
                trx = await send_notification_hive_transfer(
                    tracked_op=custom_json,
                    reason=return_hive_message,
                    nobroadcast=nobroadcast,
                )
                logger.info(
                    f"Notification transaction created for operation {custom_json.group_id}",
                    extra={"notification": True, "trx": trx, **custom_json.log_extra},
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


async def custom_json_internal_transfer(
    custom_json: CustomJson, keepsats_transfer: KeepsatsTransfer, nobroadcast: bool = False
) -> LedgerEntry:
    """
    Processes an internal transfer operation based on custom JSON input.
    This asynchronous function handles the transfer of Keepsats between two accounts,
    records the transaction in the ledger, and sends a notification to the recipient if the transfer amount
    exceeds the minimum invoice payment threshold.
    Args:
        custom_json (CustomJson): The custom JSON object containing operation details.
        keepsats_transfer (KeepsatsTransfer): The transfer details including source, destination, amount, and memo.
        nobroadcast (bool, optional): If True, suppresses broadcasting the notification. Defaults to False.
    Returns:
        LedgerEntry: The ledger entry representing the transfer transaction.
    Notes:
        - If the transfer amount is below the minimum notification threshold, no notification is sent.
        - The function saves the ledger entry and optionally sends a notification to the recipient.
    """
    # This is a transfer between two accounts
    logger.info(
        f"Processing CustomJson transfer: {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats"
    )
    ledger_type = LedgerType.CUSTOM_JSON_TRANSFER
    transfer_ledger_entry = LedgerEntry(
        cust_id=custom_json.cust_id,
        short_id=custom_json.short_id,
        ledger_type=ledger_type,
        group_id=f"{custom_json.group_id}-{ledger_type.value}",
        user_memo=keepsats_transfer.user_memo,
        timestamp=custom_json.timestamp,
        description=f"Transfer {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats",
        op_type=custom_json.op_type,
        debit=LiabilityAccount(name="Customer Liability", sub=keepsats_transfer.from_account),
        debit_conv=custom_json.conv,
        debit_amount=keepsats_transfer.sats * 1000,
        debit_unit=Currency.MSATS,
        credit=LiabilityAccount(name="Customer Liability", sub=keepsats_transfer.to_account),
        credit_conv=custom_json.conv,
        credit_unit=Currency.MSATS,
        credit_amount=keepsats_transfer.sats * 1000,
    )
    # TODO: #144 need to look into where else `user_memo` needs to be used
    await transfer_ledger_entry.save()

    if keepsats_transfer.sats <= V4VConfig().data.minimum_invoice_payment_sats:
        logger.info(f"Invoice {custom_json.short_id} is below the minimum notification threshold.")
        return transfer_ledger_entry

    # this is where #clean needs to be evaluated
    if keepsats_transfer.user_memo:
        reason = f"{keepsats_transfer.user_memo}"
    else:
        reason = (
            f"You received {keepsats_transfer.sats:,} sats from {keepsats_transfer.from_account}"
        )

    trx = await send_notification_hive_transfer(
        pay_to_cust_id=keepsats_transfer.to_account,
        tracked_op=custom_json,
        reason=reason,
        nobroadcast=nobroadcast,
    )
    logger.info(
        f"Notification transaction created for operation {custom_json.group_id}",
        extra={"notification": True, "trx": trx, **custom_json.log_extra},
    )
    return transfer_ledger_entry


# Last line of the file
