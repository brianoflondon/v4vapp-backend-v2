from nectar.amount import Amount

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.actions.hold_release_keepsats import hold_keepsats, release_keepsats
from v4vapp_backend_v2.actions.lnurl_decode import LnurlException, decode_any_lightning_string
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer, TrackedTransferWithCustomJson
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.hive_to_keepsats import conversion_hive_to_keepsats
from v4vapp_backend_v2.conversion.keepsats_to_hive import conversion_keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.service_fees import V4VMaximumInvoice, V4VMinimumInvoice
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson
from v4vapp_backend_v2.hive_models.return_details_class import HiveReturnDetails, ReturnAction
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    LNDPaymentError,
    LNDPaymentExpired,
    send_lightning_to_pay_req,
)
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.process.hive_notification import reply_with_hive


class HiveTransferError(Exception):
    """Custom exception for Hive transfer errors."""

    pass


async def follow_on_transfer(
    tracked_op: TrackedTransferWithCustomJson, nobroadcast: bool = False
) -> None:
    """
    Processes a tracked Hive-to-Lightning transfer operation, handling payment attempts and error scenarios.

    This function performs the following steps:
    1. Checks if the operation already has replies (e.g., a Lightning payment transaction) and skips processing if so.
    2. Validates required Hive and LND configuration details.
    3. Ensures the operation is directed to the server account before proceeding.
    4. Attempts to decode and pay a Lightning invoice if present, optionally holding Keepsats if requested.
    5. Handles various error scenarios:
        - HiveTransferError: Returns the full Hive amount to the sender.
        - LNDPaymentExpired: Returns the full Hive amount to the sender.
        - LNDPaymentError: Returns the full Hive amount to the sender.
        - Other exceptions: Logs the error and holds the transfer.
    6. Releases Keepsats hold if appropriate and returns Hive to the sender in case of payment failure.

    Args:
        tracked_op (TrackedTransferWithCustomJson): The tracked transfer operation containing all relevant details.
        nobroadcast (bool): If True, prevents broadcasting the Hive return transaction.

    Raises:
        HiveTransferError: If the operation already has replies or configuration is missing.

    """

    # MARK: 1. Checks
    # Check if the operation already has a lightning payment transaction
    # If it does, we skip processing
    reply_messages = []
    for reply in tracked_op.replies:
        if reply.reply_type != "ledger_error":
            message = f"Operation has a {reply.reply_type} reply, skipping processing."
            logger.info(
                message,
                extra={"notification": False, **tracked_op.log_extra},
            )
            reply_messages.append(message)
        else:
            logger.info(f"Ignoring {reply.reply_type} {reply.reply_id}.")
    if reply_messages:
        raise HiveTransferError(f"Operation already has replies: {', '.join(reply_messages)}")

    hive_config = InternalConfig().config.hive
    lnd_config = InternalConfig().config.lnd_config

    if (
        not hive_config
        or not lnd_config
        or not lnd_config.default
        or not hive_config.server_account
        or not hive_config.server_account.name
    ):
        # Log a warning if the configuration is missing
        return_hive_message = (
            f"Missing configuration details for Hive or LND: {hive_config}, {lnd_config}"
        )
        logger.warning(return_hive_message, extra={"notification": False})
        raise HiveTransferError(return_hive_message)

    server_id = InternalConfig().server_id

    # Only process if the operation is directed to the server account
    if tracked_op.to_account != server_id:
        logger.info(
            f"Operation is not directed to the server account: {server_id}",
            extra={"notification": False},
        )
        return
    cust_id = tracked_op.cust_id
    amount = Amount("0.001 HIVE")

    if tracked_op.keepsats and not isinstance(tracked_op, CustomJson):
        # This is a conversion of Hive/HBD and deposit Lightning Keepsats
        # use msats=0 to use all the funds sent (leaving only the amount for the return transaction)
        logger.info(
            f"Detected keepsats operation in memo: {tracked_op.d_memo}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        user_limits_text = await check_user_limits(tracked_op.conv.sats, tracked_op.cust_id)
        if user_limits_text:
            raise HiveTransferError(f"{user_limits_text}")
        await conversion_hive_to_keepsats(
            server_id=server_id,
            cust_id=cust_id,
            tracked_op=tracked_op,
            msats=0,
            nobroadcast=nobroadcast,
        )
        return

    return_details = HiveReturnDetails(
        tracked_op=tracked_op,
        original_memo=tracked_op.d_memo,
        reason_str="",
        action=ReturnAction.IN_PROGRESS,
        amount=AmountPyd(amount=amount),
        pay_to_cust_id=cust_id,
        nobroadcast=nobroadcast,
    )
    pay_req: PayReq | None = None
    lnd_config = InternalConfig().config.lnd_config
    lnd_client = LNDClient(connection_name=lnd_config.default)
    release_hold = True  # Default to releasing the hold at the end.
    try:
        pay_req = await decode_incoming_and_checks(tracked_op=tracked_op, lnd_client=lnd_client)
        # Important: we ignore Keepsats status for now, first we check amounts and try to pay a lightning invoice.
        # If there is no invoice we will skip to just depositing all the Hive.

        if (
            not pay_req
            and isinstance(tracked_op, CustomJson)
            and isinstance(tracked_op.json_data, KeepsatsTransfer)
        ):
            # This is a keepsats to Hive conversion.
            await conversion_keepsats_to_hive(
                server_id=server_id,
                cust_id=cust_id,
                tracked_op=tracked_op,
                nobroadcast=nobroadcast,
                msats=tracked_op.json_data.msats,
            )
            release_hold = False  #   There is no hold to release

        else:
            assert pay_req and isinstance(pay_req, PayReq), (
                "PayReq should be an instance of PayReq"
            )

            if tracked_op.paywithsats:
                await hold_keepsats(
                    amount_msats=pay_req.value_msat + pay_req.fee_estimate,
                    cust_id=cust_id,
                    tracked_op=tracked_op,
                )
            chat_message = f"Sending sats from v4v.app | § {tracked_op.short_id} |"
            payment = await send_lightning_to_pay_req(
                pay_req=pay_req,
                lnd_client=lnd_client,
                chat_message=chat_message,
                group_id=tracked_op.group_id_p,
                cust_id=tracked_op.cust_id,
                paywithsats=tracked_op.paywithsats,
                amount_msat=tracked_op.conv.msats - tracked_op.conv.msats_fee,
                fee_limit_ppm=lnd_config.lightning_fee_limit_ppm,
            )
            logger.info(
                f"Lightning payment sent for § {tracked_op.short_id} Payment: {payment.short_id}",
                extra={
                    "notification": True,
                    **tracked_op.log_extra,
                    **payment.log_extra,
                },
            )
            release_hold = False
            return

    except HiveTransferError as e:
        # Various problems with Hive or Keepsats. Send it all back.
        if tracked_op.op_type == "custom_json":
            return_details.action = ReturnAction.CUSTOM_JSON
        else:
            return_details.action = ReturnAction.REFUND
            return_details.amount = getattr(
                tracked_op, "amount", AmountPyd(amount=Amount("0.001 HIVE"))
            )
        return_details.reason_str = f"Error processing Hive to Lightning operation: {e}"
        logger.warning(
            return_details.reason_str,
            extra={"notification": False, **tracked_op.log_extra},
        )

    except LNDPaymentExpired as e:
        if tracked_op.op_type == "custom_json":
            return_details.action = ReturnAction.CUSTOM_JSON
        else:
            return_details.action = ReturnAction.REFUND
            return_details.amount = getattr(
                tracked_op, "amount", AmountPyd(amount=Amount("0.001 HIVE"))
            )
        return_details.reason_str = f"Lightning payment expired: {e}"
        logger.warning(
            return_details.reason_str,
            extra={"notification": False, **tracked_op.log_extra},
        )

    except LNDPaymentError as e:
        if tracked_op.op_type == "custom_json":
            return_details.action = ReturnAction.CUSTOM_JSON
        else:
            return_details.action = ReturnAction.REFUND
            return_details.amount = getattr(
                tracked_op, "amount", AmountPyd(amount=Amount("0.001 HIVE"))
            )
        return_details.reason_str = f"Lightning payment error: {e}"
        logger.error(
            return_details.reason_str,
            extra={"notification": False, **tracked_op.log_extra},
        )

    except Exception as e:
        # Unexpected error, log it but will not return Hive.
        return_details.action = ReturnAction.HOLD
        return_details.reason_str = f"Unexpected error occurred: {e}"
        logger.exception(
            return_details.reason_str,
            extra={
                "notification": False,
                **tracked_op.log_extra,
                **return_details.log_extra,
            },
        )
        # we don't release a keepsats hold if an unknown error occurred
        release_hold = False

    finally:
        if tracked_op.paywithsats and release_hold:
            await release_keepsats(tracked_op=tracked_op)

        if return_details.reason_str:
            try:
                # Arriving here we are usually returning the full amount sent.
                trx = await reply_with_hive(details=return_details, nobroadcast=nobroadcast)
                logger.info(
                    "Reply with Hive transfer successful after payment failure",
                    extra={
                        "notification": False,
                        "trx": trx,
                        **tracked_op.log_extra,
                        **return_details.log_extra,
                    },
                )
            except Exception as e:
                logger.exception(
                    f"Error returning Hive transfer: {e}",
                    extra={
                        "notification": False,
                        **tracked_op.log_extra,
                        **return_details.log_extra,
                    },
                )


async def decode_incoming_and_checks(
    tracked_op: TrackedTransferWithCustomJson, lnd_client: LNDClient
) -> PayReq | None:
    """
    This asynchronous function processes a Lightning payment request contained in the `d_memo` field of a `TrackedTransfer` object.
    It performs the following steps:
    - Logs the incoming payment request.
    - Initializes the LND client using internal configuration.
    - Ensures conversion details are present; attempts to update them if missing.
    - Decodes the Lightning payment request and validates its structure.
    - Checks conversion limits and user-specific payment limits.
    - Raises a `HiveTransferError` if any validation or decoding step fails.

    Args:
        hive_transfer (TrackedTransfer): The transfer operation containing the Lightning payment request in `d_memo`.
    Returns:
        PayReq: The decoded Lightning payment request.
    Raises:
        HiveTransferError: If conversion details are missing, invoice decoding fails, or limits are exceeded.
    hive_transfer (TrackedTransfer): The transfer operation containing the Lightning payment request in `d_memo`.

    """
    if tracked_op.conv is None or tracked_op.conv.is_unset():
        logger.warning(
            f"Conversion details missing for operation: {tracked_op.d_memo}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        await tracked_op.update_conv()
    if not tracked_op.conv:
        logger.error(
            "Conversion details not found for operation, failed to update conversion.",
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise HiveTransferError("Conversion details not found for operation")

    if not tracked_op.paywithsats:
        try:
            tracked_op.conv.limit_test()
        except (V4VMinimumInvoice, V4VMaximumInvoice) as e:
            logger.error(
                f"Conversion limits exceeded for operation {tracked_op.group_id_p}: {e}",
                extra={"notification": False, **tracked_op.log_extra},
            )
            raise HiveTransferError(f"Conversion limits: {e}")

    try:
        max_send_msats = tracked_op.max_send_amount_msats()

        if isinstance(tracked_op, CustomJson):
            invoice_comment = getattr(tracked_op.json_data, "invoice_message", "")
        else:
            invoice_comment = ""

        pay_req = await decode_any_lightning_string(
            input=tracked_op.d_memo,
            lnd_client=lnd_client,
            zero_amount_invoice_send_msats=max_send_msats,
            comment=invoice_comment,
            # TODO: THIS NEEDS TO BE TAKEN FROM THE MEMO
        )
        if not pay_req:
            if isinstance(tracked_op, CustomJson):
                # If we don't have a pay_req handle the case of a custom_json which has a conversion
                return None
            else:
                raise HiveTransferError("Failed to decode Lightning invoice")
    except LnurlException as e:
        message = f"Lightning decode error: {e}"
        logger.info(
            f"{message}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        # Here we process as a keepsats to Hive/HBD conversion
        logger.info("Lightning Invoice not found, processing as a Keepsats withdrawal")
        return None

    except Exception as e:
        message = f"Unexpected error decoding Lightning invoice: {e}"
        logger.exception(
            f"{message}",
            extra={"notification": False, **tracked_op.log_extra},
        )
        raise HiveTransferError(message)

    # NOTE: this will not use the limit tests (maybe that is OK?) this is not a conversion operation
    if (
        tracked_op.paywithsats
    ):  # Custom Json operations are always paywithsats if they have a memo.
        # get the sats balance for the sending account
        result = await check_keepsats_balance(pay_req.value, tracked_op.cust_id)
    else:  # both these tests are for conversions not paywithsats
        result = await check_amount_sent(pay_req=pay_req, tracked_op=tracked_op)  # type: ignore[assignment]
        if not result:
            result = await check_user_limits(pay_req.value, tracked_op.cust_id)

    if result:
        raise HiveTransferError(result)

    return pay_req


async def check_amount_sent(
    pay_req: PayReq,
    tracked_op: TrackedTransfer,
) -> str:
    """
    Asynchronously checks whether a payment attempt can be made for a given payment request.

    This function verifies the status of the invoice associated with the provided payment request
    by querying the database. If the invoice does not exist in the database, it is assumed that
    a payment attempt can be made. If the invoice exists but its status is not "open", payment
    cannot be attempted. Otherwise, payment can be attempted.

    Args:
        pay_req (PayReq): The payment request object containing the payment request string and logging context.

    Returns:
        bool: True if payment can be attempted, False otherwise.

    Raises:
        AssertionError: If the database client is not initialized.
    """
    if pay_req.is_zero_value:
        if tracked_op.conv.in_limits():
            return ""
        else:
            return "Payment request has zero value, but conversion limits exceeded."

    surplus_msats = tracked_op.max_send_amount_msats() - pay_req.value_msat
    if surplus_msats < -5_000:  # Allow a 5 sat buffer for rounding errors (5,000 msats, 5 sats)
        if tracked_op.unit == Currency.HIVE:
            surplus_hive = abs(round(surplus_msats / 1_000 / tracked_op.conv.sats_hive, 3))
            failure_reason = (
                f"Not enough sent to process this payment request: {surplus_hive:,.3f} HIVE"
            )
        elif tracked_op.unit == Currency.HBD:
            surplus_hbd = abs(round(surplus_msats / 1_000 / tracked_op.conv.sats_hbd, 3))
            failure_reason = (
                f"Not enough sent to process this payment request: {surplus_hbd:,.3f} HBD"
            )
        else:
            failure_reason = f"Not enough sent to process this payment request: {surplus_msats / 1_000:,.0f} sats"

        return failure_reason
    return ""


async def check_user_limits(extra_spend_sats: int, cust_id: str) -> str:
    """
    Asynchronously checks if the user associated with a Hive transfer has sufficient limits to process a Lightning payment request.

        pay_req (PayReq): The payment request object containing details of the Lightning payment.
        hive_transfer (TrackedTransfer): The Hive transfer object representing the user's transfer details.

        str: An empty string if the user has sufficient limits; otherwise, a message describing the limit violation.

    """
    limit_check = await check_hive_conversion_limits(
        hive_accname=cust_id, extra_spend_sats=extra_spend_sats
    )
    for limit in limit_check:
        if not limit.limit_ok:
            logger.warning(limit.output_text, extra={"notification": False})
            return limit.output_text
    return ""


async def check_keepsats_balance(extra_spend_sats: int, cust_id: str) -> str:
    """
    Asynchronously checks whether the user has sufficient Keepsats balance for a payment request.
    """
    net_sats, keepsats_balance = await keepsats_balance_printout(cust_id=cust_id)
    if not keepsats_balance.balances.get(Currency.MSATS):
        raise HiveTransferError("Pay with sats operation detected, but no Keepsats balance found.")
    # TODO: Need to account for routing fees in Keepsats payments
    if net_sats < extra_spend_sats:
        raise HiveTransferError(
            f"Insufficient Keepsats balance ({net_sats:,.0f}) to cover payment request: {extra_spend_sats:,.0f} sats"
        )
    return ""
