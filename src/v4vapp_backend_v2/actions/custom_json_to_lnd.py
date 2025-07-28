from v4vapp_backend_v2.accounting.account_balances import keepsats_balance_printout
from v4vapp_backend_v2.accounting.ledger_account_classes import LiabilityAccount
from v4vapp_backend_v2.accounting.ledger_entry import LedgerEntry, LedgerType
from v4vapp_backend_v2.actions.actions_errors import CustomJsonToLightningError
from v4vapp_backend_v2.actions.hive_notification import send_notification_custom_json
from v4vapp_backend_v2.actions.hold_release_keepsats import hold_keepsats, release_keepsats
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_prices import Currency
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
        zero_amount_invoice = keepsats_transfer.sats * 1000 if keepsats_transfer.sats else 0
        pay_req = await decode_any_lightning_string(
            input=keepsats_transfer.memo,
            lnd_client=lnd_client,
            zero_amount_invoice_send_msats=zero_amount_invoice,
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
        logger.info(
            f"Hold placed for {pay_req.value_msat + pay_req.fee_estimate} msats {keepsats_transfer.from_account}"
        )
        net_sats_after, keepsats_balance = await keepsats_balance_printout(
            cust_id=keepsats_transfer.from_account, previous_sats=net_sats
        )

        amount_msats = min(zero_amount_invoice, pay_req.amount_msat, int(net_sats_after * 1000))

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
            logger.info(f"Hold released for {keepsats_transfer.from_account}")

        notification = KeepsatsTransfer(
            from_account=InternalConfig().config.hive.server_account.name,
            to_account=keepsats_transfer.from_account,
            memo=return_hive_message or keepsats_transfer.log_str,
            notification=True,
            invoice_message=keepsats_transfer.invoice_message,
            parent_id=custom_json.group_id,
        )
        trx = await send_notification_custom_json(
            tracked_op=custom_json, notification=notification
        )
        logger.info(
            f"Notification transaction created for operation {custom_json.group_id}",
            extra={"notification": False, "trx": trx, **custom_json.log_extra},
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
    if not keepsats_transfer.sats:
        raise CustomJsonToLightningError("Keepsats transfer amount is zero.")
    debit_credit_amount = keepsats_transfer.sats * 1_000

    user_memo = (
        keepsats_transfer.user_memo
        or f"You received {keepsats_transfer.sats:,} sats from {keepsats_transfer.from_account}"
    )

    transfer_ledger_entry = LedgerEntry(
        cust_id=custom_json.cust_id,
        short_id=custom_json.short_id,
        ledger_type=ledger_type,
        group_id=f"{custom_json.group_id}-{ledger_type.value}",
        user_memo=user_memo,
        timestamp=custom_json.timestamp,
        description=f"Transfer {keepsats_transfer.from_account} -> {keepsats_transfer.to_account} {keepsats_transfer.sats:,} sats",
        op_type=custom_json.op_type,
        debit=LiabilityAccount(name="Customer Liability", sub=keepsats_transfer.from_account),
        debit_conv=custom_json.conv,
        debit_amount=debit_credit_amount,
        debit_unit=Currency.MSATS,
        credit=LiabilityAccount(name="Customer Liability", sub=keepsats_transfer.to_account),
        credit_conv=custom_json.conv,
        credit_unit=Currency.MSATS,
        credit_amount=debit_credit_amount,
    )
    # TODO: #144 need to look into where else `user_memo` needs to be used
    await transfer_ledger_entry.save()

    notification = KeepsatsTransfer(
        from_account=InternalConfig().config.hive.server_account.name,
        to_account=keepsats_transfer.from_account,
        memo=keepsats_transfer.log_str,
        notification=True,
        invoice_message=keepsats_transfer.invoice_message,
        parent_id=custom_json.group_id,
    )
    trx = await send_notification_custom_json(tracked_op=custom_json, notification=notification)
    return transfer_ledger_entry


# Last line of the file
# Last line of the file
# Last line of the file
