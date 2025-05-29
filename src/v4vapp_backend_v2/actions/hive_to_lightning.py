import asyncio
from typing import Dict

from nectar.amount import Amount

from v4vapp_backend_v2.accounting.ledger_entry import update_ledger_entry_op
from v4vapp_backend_v2.actions.lnurl_decode import LnurlException, decode_any_lightning_string
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.pub_key_alias import update_payment_route_with_alias
from v4vapp_backend_v2.hive.hive_extras import HiveTransferError, get_hive_client, send_transfer
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import LNDPaymentExpired, send_lightning_to_pay_req
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment

MEMO_FOOTER = " - Thank you for using v4v.app"


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


async def can_attempt_to_pay(pay_req: PayReq) -> bool:
    """
    Check the status of the invoice.
    """
    # Placeholder for actual implementation
    assert TransferBase.db_client, "Database client is not initialized"
    async with TransferBase.db_client as db_client:
        invoice = await db_client.find_one("invoices", {"payment_request": pay_req.pay_req_str})
        if not invoice:
            logger.info(
                f"Invoice {pay_req.pay_req_str} not found in the database.",
                extra={"notification": False, **pay_req.log_extra},
            )
            return True
        if invoice.status != "open":
            logger.warning(
                f"Invoice {pay_req.pay_req_str} is not open, current status: {invoice.status}.",
                extra={"notification": False, **pay_req.log_extra},
            )
            return False
    return True


async def process_hive_to_lightning(
    hive_transfer: TrackedTransfer, nobroadcast: bool = False
) -> None:
    """
    Process a transfer operation from Hive to Lightning.

    This asynchronous function handles the workflow for transferring funds from a Hive account to a Lightning Network destination. It performs the following steps:
    - Checks if the operation is already locked and skips processing if so.
    - Validates the presence of required Hive and Lightning configuration.
    - Ensures the operation is intended for the server account.
    - Checks if the operation is eligible for processing.
    - Decodes the Lightning invoice from the operation memo.
    - Ensures conversion details are present or attempts to update them.
    - Initiates the Lightning payment using the decoded invoice and conversion details.
    - Handles exceptions related to invoice decoding and payment expiration, logging and raising appropriate errors or triggering repayment as needed.

    Args:
        op (TransferBase): The transfer operation to process.
        nobroadcast (bool, optional): If True, prevents broadcasting the transaction. Defaults to False.

    Raises:
        HiveToLightningError: If configuration is missing, invoice decoding fails, or conversion details are unavailable.

    Flow for processing a Hive to Lightning operation:
        1. Checks
        2. Pay Lightning Invoice:
            - decode_incoming_payment_message
            - send_lightning_to_pay_req
            payment object created (but will not be in database or ledger yet)
        3. Move to Handle payment sent
            - lightning_payment_sent
        4. Update original hive_transfer with payment reply
        5. Calculate costs and change if needed.
        6. If change is needed or not, return_hive_transfer
            - return_hive_transfer
            return hive transfer object created (but will not be in database or ledger yet)
        7. Update original hive_transfer with new reply Hive transfer
        8. Update original Ledger Entry with operation



    """
    if hive_transfer.locked:
        logger.info(
            f"Operation is already locked, skipping processing. {hive_transfer.log_str}",
            extra={"notification": False, "op": hive_transfer.model_dump()},
        )
        raise HiveToLightningError(f"Operation is locked: {hive_transfer.locked}")
    # Now check if the operation already has a lightning payment transaction
    # If it does, we skip processing
    payment_reply = hive_transfer.get_replies_by_type("payment")
    transfer_reply = hive_transfer.get_replies_by_type("transfer")
    if payment_reply:
        logger.info(
            f"Operation already has a payment reply, skipping processing. {hive_transfer.log_str}, reply_id(s): {payment_reply}",
            extra={"notification": False, "op": hive_transfer.model_dump()},
        )
        raise HiveToLightningError(f"Operation already has a payment reply: {payment_reply}")
    if transfer_reply:
        logger.info(
            f"Operation already has a transfer reply, skipping processing. {hive_transfer.log_str}, reply_id(s): {transfer_reply}",
            extra={"notification": False, "op": hive_transfer.model_dump()},
        )
        raise HiveToLightningError(f"Operation already has a transfer reply: {transfer_reply}")

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
    async with hive_transfer:
        server_account = hive_config.server_account.name
        if hive_transfer.to_account == server_account:
            # Process the operation
            if await check_for_hive_to_lightning(hive_transfer):
                logger.info(
                    f"Processing operation to {server_account} ({hive_transfer.from_account} -> {hive_transfer.to_account})",
                    extra={"notification": False, "op": hive_transfer.model_dump()},
                )
                if hive_transfer.memo:
                    try:
                        pay_req = await decode_incoming_payment_message(
                            hive_transfer=hive_transfer
                        )
                        if not pay_req:
                            raise HiveToLightningError("Failed to decode Lightning invoice")
                        if hive_transfer.conv is None or hive_transfer.conv.is_unset():
                            logger.warning(
                                f"Conversion details missing for operation: {hive_transfer.memo}",
                                extra={"notification": False, **hive_transfer.log_extra},
                            )
                            await hive_transfer.update_conv()
                        if not hive_transfer.conv:
                            logger.error(
                                "Conversion details not found for operation, failed to update conversion.",
                                extra={"notification": False, **hive_transfer.log_extra},
                            )
                            raise HiveToLightningError(
                                "Conversion details not found for operation"
                            )
                        lnd_client = LNDClient(connection_name=lnd_config.default)
                        payment = await send_lightning_to_pay_req(
                            pay_req=pay_req,
                            lnd_client=lnd_client,
                            chat_message=hive_transfer.group_id_p,
                            group_id=hive_transfer.group_id_p,
                            amount_msat=hive_transfer.conv.msats - hive_transfer.conv.msats_fee,
                            fee_limit_ppm=500,
                        )
                        if payment:
                            await update_payment_route_with_alias(
                                db_client=TransferBase.db_client,
                                lnd_client=lnd_client,
                                payment=payment,
                            )
                            asyncio.create_task(
                                lightning_payment_sent(payment, hive_transfer, nobroadcast)
                            )
                    except LnurlException as e:
                        logger.info(
                            f"Error decoding Lightning invoice: {e}",
                            extra={"notification": False, "op": hive_transfer.model_dump()},
                        )
                        raise HiveToLightningError(f"Error decoding Lightning invoice: {e}")

                    except LNDPaymentExpired as e:
                        logger.warning(
                            f"Lightning payment expired: {e}",
                            extra={"notification": False, "op": hive_transfer.model_dump()},
                        )
                        asyncio.create_task(
                            return_hive_transfer(
                                hive_transfer=hive_transfer,
                                reason="Lightning invoice expired",
                                nobroadcast=nobroadcast,
                            )
                        )

                else:
                    logger.warning(
                        "Failed to decode Lightning invoice",
                        extra={"notification": False, "op": hive_transfer.model_dump()},
                    )


async def decode_incoming_payment_message(hive_transfer: TrackedTransfer) -> PayReq | None:
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
    logger.info(f"Processing payment request: {hive_transfer.memo}")
    lnd_client = LNDClient(connection_name=lnd_config.default)
    try:
        pay_req = await decode_any_lightning_string(
            input=hive_transfer.memo, lnd_client=lnd_client
        )
        return pay_req
    except Exception as e:
        logger.error(f"Error decoding Lightning invoice: {e}")
        return None


async def lightning_payment_sent(
    payment: Payment, hive_transfer: TrackedTransfer, nobroadcast: bool
) -> None:
    """
    Callback function to be called when a Lightning payment is sent.
    This will check that the payment matches the operation and check if a change
    transaction is needed

    Args:
        payment (Payment): The Payment object representing the sent payment.
        op (TrackedTransfer): The TrackedTransfer object representing the operation.

    Returns:
        None
    """
    # Placeholder for actual implementation
    async with hive_transfer:
        async with payment:
            if not confirm_payment_details(hive_transfer, payment):
                message = f"Payment group ID {payment.custom_records} does not match operation group ID {hive_transfer.group_id_p}"
                logger.warning(
                    message,
                    extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
                )
                raise HiveToLightningError(message)

            logger.info(
                f"Lightning payment sent: {payment.log_str}",
                extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
            )
            assert payment.custom_records and payment.custom_records.v4vapp_group_id, (
                "Payment must have a group ID set to be valid in v4vapp"
            )
            # Now calculate if change is needed.
            cost_of_payment_msat = payment.value_msat + payment.fee_msat
            message = f"Lightning payment sent for operation {hive_transfer.group_id_p}: {payment.payment_hash} {payment.route_str} {cost_of_payment_msat / 1000:,.0f} sats"
            hive_transfer.add_reply(
                reply_id=payment.payment_hash,
                reply_type="payment",
                reply_msat=cost_of_payment_msat,
                reply_error=None,
                reply_message=message,
            )
            ans = await hive_transfer.save(include={"replies"})
            logger.info(
                f"Updated operation with Lightning payment reply: {payment.payment_hash}",
                extra={
                    "notification": False,
                    **hive_transfer.log_extra,
                    **payment.log_extra,
                },
            )
            if not hive_transfer.conv or hive_transfer.conv.is_unset():
                await hive_transfer.update_conv()
            change = hive_transfer.conv.msats - cost_of_payment_msat - hive_transfer.conv.msats_fee
            if change > 1_100:
                # If change is more than 1.1 satoshis, we need to send a change transaction
                logger.info(
                    f"Change detected for operation {hive_transfer.group_id_p}: {change} msats",
                    extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
                )
                if hive_transfer.conv.conv_from == Currency.HIVE:
                    amount_to_return = round((change / 1000) / hive_transfer.conv.sats_hive, 3)
                    currency = "HIVE"
                else:
                    amount_to_return = round((change / 1000) / hive_transfer.conv.sats_hbd, 3)
                    currency = "HBD"
                amount = Amount(f"{amount_to_return:.3f} {currency}")
                # TODO: Work on reply message system
                reason = f"Change from Lightning payment {payment.payment_hash} for operation {hive_transfer.group_id_p}"
                trx = await return_hive_transfer(
                    hive_transfer=hive_transfer,
                    reason=reason,
                    amount=amount,
                    nobroadcast=nobroadcast,
                )
                logger.info(
                    f"Change transaction created for operation {hive_transfer.group_id_p}: {amount} {currency}",
                    extra={"notification": True, **hive_transfer.log_extra, **payment.log_extra},
                )

            else:
                # TODO: Work on reply message system
                reason = f"Lightning invoice paid {payment.payment_hash} for operation {hive_transfer.group_id_p}"
                trx = await return_hive_transfer(
                    hive_transfer=hive_transfer,
                    reason=reason,
                    nobroadcast=nobroadcast,
                    amount=Amount("0.001 HIVE"),
                )
                logger.info(
                    f"No change transaction needed for operation {hive_transfer.group_id_p}: {change} msats",
                    extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
                )
            if trx:
                # finally update the original Ledger Entry for this operation
                original_ledger_entry, ans = await update_ledger_entry_op(
                    group_id=hive_transfer.group_id_p, op=hive_transfer
                )
                if original_ledger_entry:
                    logger.info(
                        f"Updated original Ledger Entry {hive_transfer.group_id_p} with OP: {ans}",
                        extra={**hive_transfer.log_extra, **original_ledger_entry.log_extra},
                    )


def confirm_payment_details(op: TransferBase, payment: Payment) -> bool:
    """
    Checks if the payment's custom records contain a group ID that matches the operation's group ID.

    Args:
        op (TransferBase): The transfer operation containing the expected group ID.
        payment (Payment): The payment object containing custom records.

    Returns:
        bool: True if the payment's custom records contain a group ID matching the operation's group ID, False otherwise.
    """
    if payment.custom_records and payment.custom_records.v4vapp_group_id:
        if payment.custom_records.v4vapp_group_id == op.group_id_p:
            return True
    return False


async def return_hive_transfer(
    hive_transfer: TrackedTransfer,
    reason: str,
    amount: Amount | None = None,
    nobroadcast: bool = False,
) -> Dict[str, str]:
    """
    Repay a Hive to Lightning transfer by returning funds to the original sender.
    This asynchronous function is invoked when a Lightning payment associated with a Hive to Lightning operation fails or expires.
    It attempts to repay the original Hive sender by transferring the funds back to their account.
    Args:
        op (TransferBase): The original transfer operation containing details of the Hive to Lightning transaction.
        reason (str): The reason for repayment, included in the memo of the repayment transaction.
        nobroadcast (bool, optional): If True, the transaction will not be broadcast to the Hive network. Defaults to False.
    Raises:
        HiveToLightningError: If required Hive server account configuration or keys are missing, or if the repayment transfer fails.
    Side Effects:
        - Logs the repayment attempt and result.
        - Updates the original operation with the reply transaction ID or error.
        - Sends a Hive transfer to the original sender if possible.

    """
    # Placeholder for actual implementation
    logger.info(
        f"Repaying Hive to Lightning operation: {hive_transfer.log_str}",
        extra={"notification": False, "op": hive_transfer.model_dump()},
    )
    hive_config = InternalConfig().config.hive
    if not hive_config.server_account:
        raise HiveToLightningError("Missing Hive server account configuration for repayment")

    memo_key = hive_config.server_account.memo_key or ""
    active_key = hive_config.server_account.active_key or ""
    if not memo_key or not active_key:
        raise HiveToLightningError("Missing Hive server account keys for repayment")

    hive_client = get_hive_client(
        keys=[
            hive_config.server_account.memo_key,
            hive_config.server_account.active_key,
        ],
        nobroadcast=nobroadcast,
    )
    amount = amount or hive_transfer.amount.beam
    if not isinstance(amount, Amount):
        raise HiveToLightningError("Amount must be an instance of Amount")
    try:
        memo = f"{reason} - {hive_transfer.group_id}{MEMO_FOOTER}"
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=hive_config.server_account.name,
            to_account=hive_transfer.from_account,  # Repay to the original sender
            amount=amount,
            memo=memo,
        )
        if trx:
            # MARK: UPDATE ORIGINAL OPERATION
            logger.info(
                f"Successfully paid Hive to Lightning operation: {hive_transfer.replies[-1]}",
                extra={
                    "notification": True,
                    "trx": trx,
                    **hive_transfer.log_extra,
                },
            )
            try:
                return_amount = Amount(trx["operations"][0][1]["amount"])
            except (KeyError, IndexError):
                return_amount = Amount("0.001 HIVE")
            if not return_amount:
                return_amount = Amount("0.001 HIVE")
            return_amount_msat = CryptoConversion(
                conv_from=return_amount.symbol,
                amount=return_amount,
                quote=TransferBase.last_quote,
            ).msats
            # Now add the Hive reply to the original Hive transfer operation

            reason = f"Change transaction for operation {hive_transfer.group_id_p}: {trx.get('trx_id', '')}"
            hive_transfer.add_reply(
                reply_id=trx.get("trx_id", ""),
                reply_type="transfer",
                reply_msat=return_amount_msat,
                reply_error=None,
                reply_message=reason,
            )
            await hive_transfer.save(include={"replies"})
            return trx
        else:
            raise HiveTransferError("No transaction created during Hive to Lightning repayment")
    except HiveTransferError as e:
        message = f"Failed to repay Hive to Lightning operation: {e}"
        hive_transfer.add_reply(
            reply_id="", reply_type="transfer", reply_error=e, reply_message=message
        )
        await hive_transfer.save()
        logger.error(
            message,
            extra={"notification": False, "op": hive_transfer.model_dump()},
        )
        raise HiveToLightningError(message)

    except Exception as e:
        message = f"Unexpected error during Hive to Lightning repayment: {e}"
        hive_transfer.add_reply(
            reply_id="", reply_type="transfer", reply_error=e, reply_message=message
        )
        await hive_transfer.save()
        logger.error(
            message,
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(message)
