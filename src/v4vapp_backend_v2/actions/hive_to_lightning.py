import asyncio
from typing import Dict, List, Tuple

from nectar.amount import Amount
from nectar.hive import Hive

from v4vapp_backend_v2.accounting.account_balances import (
    check_hive_conversion_limits,
    get_keepsats_balance,
)
from v4vapp_backend_v2.actions.actions_errors import HiveToLightningError
from v4vapp_backend_v2.actions.cust_id_class import CustID
from v4vapp_backend_v2.actions.hive_to_keepsats import hive_to_keepsats_deposit
from v4vapp_backend_v2.actions.keepsats_ledger_entries import hold_keepsats, release_keepsats
from v4vapp_backend_v2.actions.lnurl_decode import decode_any_lightning_string
from v4vapp_backend_v2.actions.tracked_any import TrackedTransfer
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import HiveRoles, InternalConfig, logger
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.helpers.service_fees import V4VMaximumInvoice, V4VMinimumInvoice
from v4vapp_backend_v2.hive.hive_extras import HiveTransferError, get_hive_client, send_transfer
from v4vapp_backend_v2.hive_models.amount_pyd import AmountPyd
from v4vapp_backend_v2.hive_models.op_all import OpAny
from v4vapp_backend_v2.hive_models.op_transfer import TransferBase
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    LNDPaymentError,
    LNDPaymentExpired,
    send_lightning_to_pay_req,
)
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment

MEMO_FOOTER = " | Thank you for using v4v.app"


class HiveRefundNeeded(HiveToLightningError):
    """
    Exception raised when a refund is needed for a Hive to Lightning operation.
    """

    fee: Amount = Amount("0.001 HIVE")  # Default fee for refund operations
    reason: str = "Refund needed for Hive to Lightning operation"
    group_id: str = ""  # Group ID of the operation that needs a refund

    def __init__(
        self,
        group_id: str,
        reason: str = "Refund needed for Hive to Lightning operation",
        fee: Amount = Amount("0.001 HIVE"),
    ):
        """
        Initialize the HiveRefundNeeded exception.

        Args:
            group_id (str): The group ID of the operation that needs a refund.
            reason (str, optional): The reason for the refund. Defaults to None.
            fee (Amount, optional): The fee associated with the refund. Defaults to 0.
        """
        super().__init__(reason or self.reason)
        self.reason = reason
        self.group_id = group_id
        self.fee = fee


async def check_for_hive_to_lightning(op: OpAny) -> bool:
    """
    Check if the Hive to Lightning process is running.
    """
    # Placeholder for actual implementation
    return True


async def check_keepsats_balance(hive_transfer: TrackedTransfer, pay_req: PayReq) -> str:
    """
    Asynchronously checks whether the user has sufficient Keepsats balance for a payment request.
    """
    keepsats_balance, net_sats = await get_keepsats_balance(cust_id=hive_transfer.from_account)
    logger.info(
        f"Checking Keepsats balance for {hive_transfer.from_account}: {net_sats:,.0f} sats"
    )
    if not keepsats_balance.balances.get(Currency.MSATS):
        raise HiveToLightningError(
            "Pay with sats operation detected, but no Keepsats balance found."
        )
    # TODO: Need to account for routing fees in Keepsats payments
    if net_sats < pay_req.value:
        raise HiveToLightningError(
            f"Not enough Keepsats balance ({net_sats:,.0f}) to cover payment request: {pay_req.value:,.0f} sats"
        )
    return ""


async def check_amount_sent(hive_transfer: TrackedTransfer, pay_req: PayReq) -> str:
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
        if hive_transfer.conv.in_limits():
            return ""
        else:
            return "Payment request has zero value, but conversion limits exceeded."

    surplus_msats = hive_transfer.max_send_amount_msats() - pay_req.value_msat
    if surplus_msats < -5_000:  # Allow a 5 sat buffer for rounding errors (5,000 msats, 5 sats)
        if hive_transfer.conv.conv_from == Currency.HIVE:
            surplus_hive = round(surplus_msats / 1_000 / hive_transfer.conv.sats_hive, 3)
            failure_reason = (
                f"Not enough sent to process this payment request: {surplus_hive} HIVE"
            )
        elif hive_transfer.conv.conv_from == Currency.HBD:
            surplus_hbd = round(surplus_msats / 1_000 / hive_transfer.conv.sats_hbd, 3)
            failure_reason = f"Not enough sent to process this payment request: {surplus_hbd} HBD"
        else:
            failure_reason = f"Not enough sent to process this payment request: {surplus_msats / 1_000:,.0f} sats"

        return failure_reason
    return ""


async def check_user_limits(extra_spend_sats: int, hive_transfer: TrackedTransfer) -> str:
    """
    Asynchronously checks if the user associated with a Hive transfer has sufficient limits to process a Lightning payment request.

        pay_req (PayReq): The payment request object containing details of the Lightning payment.
        hive_transfer (TrackedTransfer): The Hive transfer object representing the user's transfer details.

        str: An empty string if the user has sufficient limits; otherwise, a message describing the limit violation.

    """
    limit_check = await check_hive_conversion_limits(
        hive_accname=hive_transfer.from_account, extra_spend_sats=extra_spend_sats
    )
    for limit in limit_check:
        if not limit.limit_ok:
            logger.warning(
                limit.output_text, extra={"notification": False, **hive_transfer.log_extra}
            )
            return limit.output_text
    return ""


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
        ------------------------ At this point we should pick up the payment via a new process.
        3. Move to Handle payment sent
            - lightning_payment_sent
        4. Fee costs and change if needed.
        5. Update original hive_transfer with payment reply
        6. If change is needed or not, return_hive_transfer
            - return_hive_transfer
            return hive transfer object created (but will not be in database or ledger yet)
        7. Update original hive_transfer with new reply Hive transfer
        8. Update original Ledger Entry with operation



    """
    # MARK: 1. Checks
    # Check if the operation already has a lightning payment transaction
    # If it does, we skip processing
    payment_reply = hive_transfer.get_replies_by_type("payment")
    transfer_reply = hive_transfer.get_replies_by_type("transfer")
    if payment_reply:
        logger.info(
            f"Operation already has a payment reply, skipping processing. {hive_transfer.log_str}, reply_id(s): {payment_reply}",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(f"Operation already has a payment reply: {len(payment_reply)}")
    if transfer_reply:
        logger.info(
            f"Operation already has a transfer reply, skipping processing. {hive_transfer.log_str}, reply_id(s): {transfer_reply}",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(
            f"Operation already has a transfer reply: {len(transfer_reply)}"
        )

    hive_config = InternalConfig().config.hive
    lnd_config = InternalConfig().config.lnd_config
    if (
        not hive_config
        or not lnd_config
        or not lnd_config.default
        or not hive_config.server_account
    ):
        # Log a warning if the configuration is missing
        return_hive_message = (
            f"Missing configuration details for Hive or LND: {hive_config}, {lnd_config}"
        )
        logger.warning(return_hive_message, extra={"notification": False})
        raise HiveToLightningError(return_hive_message)
    server_account = hive_config.server_account.name
    if hive_transfer.to_account == server_account:
        cust_id = CustID(hive_transfer.from_account)
        logger.info(f"LOCKING {cust_id} {__name__}")
        # Process the operation
        if await check_for_hive_to_lightning(hive_transfer):
            logger.info(
                f"Processing operation to {server_account} ({hive_transfer.from_account} -> {hive_transfer.to_account})",
                extra={"notification": False, **hive_transfer.log_extra},
            )
            # MARK: 2. Pay Lightning Invoice
            if hive_transfer.d_memo:
                return_hive_message = ""
                # MARK: 2a. Keepsats checks
                if hive_transfer.keepsats:
                    # This is a conversion of Hive/HBD into Lightning Keepsats
                    logger.info(
                        f"Detected keepsats operation in memo: {hive_transfer.d_memo}",
                        extra={"notification": False, **hive_transfer.log_extra},
                    )
                    user_limits_text = await check_user_limits(
                        hive_transfer.conv.sats, hive_transfer
                    )
                    if user_limits_text:
                        raise HiveToLightningError(f"{user_limits_text}")
                    try:
                        await convert_hive_to_keepsats(
                            hive_transfer=hive_transfer, nobroadcast=nobroadcast
                        )
                        return

                    except Exception as e:
                        message = f"Error converting Hive to Keepsats: {e}"
                        logger.error(
                            message,
                            extra={"notification": False, **hive_transfer.log_extra},
                        )
                        raise HiveToLightningError(message)

                release_hold = True  # Default to releasing the hold at the end.
                try:
                    pay_req, lnd_client = await decode_incoming_and_checks(
                        hive_transfer=hive_transfer
                    )
                    # MARK: 2b Pay with Keepsats
                    if hive_transfer.paywithsats:
                        logger.info(
                            f"Detected paywithsats operation in memo: {hive_transfer.d_memo}",
                            extra={"notification": False, **hive_transfer.log_extra},
                        )
                        # if we're using pay with keepsats, we must record the trial ledger entries
                        # HERE before attempting the payment and update them on success.
                        # This trial entry (signified by a prefix of hold_ in the group_id) will
                        # be updated to the final entry on success.
                        await hold_keepsats(
                            amount_msats=pay_req.value_msat + pay_req.fee_estimate,
                            cust_id=hive_transfer.from_account,
                            hive_transfer=hive_transfer,
                        )

                    chat_message = f"Sending sats from v4v.app | § {hive_transfer.short_id} |"
                    payment = await send_lightning_to_pay_req(
                        pay_req=pay_req,
                        lnd_client=lnd_client,
                        chat_message=chat_message,
                        group_id=hive_transfer.group_id_p,
                        cust_id=hive_transfer.cust_id,
                        paywithsats=hive_transfer.paywithsats,
                        amount_msat=hive_transfer.conv.msats - hive_transfer.conv.msats_fee,
                        fee_limit_ppm=lnd_config.lightning_fee_limit_ppm,
                    )
                    logger.info(
                        f"Lightning payment sent successfully {payment.group_id_p}",
                        extra={
                            "notification": True,
                            **hive_transfer.log_extra,
                            **payment.log_extra,
                        },
                    )
                    # If the payment succeeded we do not release the HOLD here, were release it when the payment ledger entries are made
                    release_hold = False
                    return

                except HiveToLightningError as e:
                    return_hive_message = f"Error processing Hive to Lightning operation: {e}"
                    logger.warning(
                        return_hive_message,
                        extra={"notification": False, **hive_transfer.log_extra},
                    )

                except LNDPaymentExpired as e:
                    return_hive_message = f"Lightning payment expired: {e}"
                    logger.warning(
                        return_hive_message,
                        extra={"notification": False, **hive_transfer.log_extra},
                    )

                except LNDPaymentError as e:
                    return_hive_message = f"Lightning payment error: {e}"
                    logger.error(
                        return_hive_message,
                        extra={"notification": False, **hive_transfer.log_extra},
                    )

                except Exception:
                    logger.exception(
                        "Unexpected error during Hive to Lightning processing",
                        extra={"notification": False, **hive_transfer.log_extra},
                    )
                    # we don't release a keepsats hold if an unknown error occurred
                    release_hold = False

                finally:
                    if hive_transfer.paywithsats and release_hold:
                        await release_keepsats(hive_transfer=hive_transfer)

                    if return_hive_message:
                        try:
                            await return_hive_transfer(
                                hive_transfer=hive_transfer,
                                reason=return_hive_message,
                                nobroadcast=nobroadcast,
                            )
                        except Exception as e:
                            logger.exception(
                                f"Error returning Hive transfer: {e}",
                                extra={
                                    "notification": False,
                                    "reason": return_hive_message,
                                    **hive_transfer.log_extra,
                                },
                            )

            else:
                # Any transfer that ends up here will be recorded as a liability in the
                # Customer Liability (Liability) account for the send of the transfer.
                # TODO: #127 Consider turning all empty memo deposits into Keepsats automatically
                logger.warning(
                    f"🟥 Failed to take action on Hive Transfer {hive_transfer.notification_str}",
                    extra={"notification": True, **hive_transfer.log_extra},
                )


async def decode_incoming_and_checks(
    hive_transfer: TrackedTransfer,
) -> Tuple[PayReq, LNDClient]:
    """
    This asynchronous function processes a Lightning payment request contained in the `d_memo` field of a `TrackedTransfer` object.
    It performs the following steps:
    - Logs the incoming payment request.
    - Initializes the LND client using internal configuration.
    - Ensures conversion details are present; attempts to update them if missing.
    - Decodes the Lightning payment request and validates its structure.
    - Checks conversion limits and user-specific payment limits.
    - Raises a `HiveToLightningError` if any validation or decoding step fails.

    Args:
        hive_transfer (TrackedTransfer): The transfer operation containing the Lightning payment request in `d_memo`.
    Returns:
        Tuple[PayReq, LNDClient]: A tuple containing the decoded Lightning payment request and the LND client.
    Raises:
        HiveToLightningError: If conversion details are missing, invoice decoding fails, or limits are exceeded.
    hive_transfer (TrackedTransfer): The transfer operation containing the Lightning payment request in `d_memo`.

    """

    logger.info(f"Processing payment request: {hive_transfer.d_memo}")
    lnd_config = InternalConfig().config.lnd_config
    lnd_client = LNDClient(connection_name=lnd_config.default)

    if hive_transfer.conv is None or hive_transfer.conv.is_unset():
        logger.warning(
            f"Conversion details missing for operation: {hive_transfer.d_memo}",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        await hive_transfer.update_conv()
    if not hive_transfer.conv:
        logger.error(
            "Conversion details not found for operation, failed to update conversion.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError("Conversion details not found for operation")

    if not hive_transfer.paywithsats:
        try:
            hive_transfer.conv.limit_test()
        except (V4VMinimumInvoice, V4VMaximumInvoice) as e:
            logger.error(
                f"Conversion limits exceeded for operation {hive_transfer.group_id_p}: {e}",
                extra={"notification": False, **hive_transfer.log_extra},
            )
            raise HiveToLightningError(f"Conversion limits: {e}")

    try:
        max_send_msats = hive_transfer.max_send_amount_msats()
        pay_req = await decode_any_lightning_string(
            input=hive_transfer.d_memo,
            lnd_client=lnd_client,
            zero_amount_invoice_send_msats=max_send_msats,
        )
        if not pay_req:
            raise HiveToLightningError("Failed to decode Lightning invoice")
    except Exception as e:
        logger.exception(
            f"Error decoding Lightning invoice: {e}",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(f"Error decoding Lightning invoice: {e}")

    # NOTE: this will not use the limit tests (maybe that is OK?) this is not a conversion operation
    if hive_transfer.paywithsats:
        # get the sats balance for the sending account
        result = await check_keepsats_balance(hive_transfer, pay_req)
    else:  # both these tests are for conversions not paywithsats
        result = await check_amount_sent(hive_transfer, pay_req)
        if not result:
            result = await check_user_limits(pay_req.value, hive_transfer)

    if result:
        raise HiveToLightningError(result)

    return pay_req, lnd_client


# MARK: 3. Handle Lightning payment
"""
After a payment is sent, this function will be called via the database trigger
"""


async def lightning_payment_sent(
    payment: Payment, hive_transfer: TrackedTransfer, nobroadcast: bool
) -> None:
    """
    Callback function to be called when a Lightning payment is sent.
    This will check that the payment matches the operation and check if a change
    transaction is needed.

    This SENDS HIVE BACK TO THE USER if the payment was successful.

    Args:
        payment (Payment): The Payment object representing the sent payment.
        op (TrackedTransfer): The TrackedTransfer object representing the operation.

    Returns:
        None
    """
    # This hive_transfer is passed in and should have the payment recorded in it but will not have been updated
    # in the database yet.
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
    # Now calculate if change is needed and record the FEE
    # MARK: 4. Fee costs and change
    change_amount = hive_transfer.change_amount.beam
    message = f"Lightning payment {payment.value_sat:,} hive: {hive_transfer.short_id} hash: {payment.short_id} {payment.route_str} change: {change_amount}"

    reason = f"Lightning {payment.value_sat:,} sats has been paid, returning change (hash: {payment.short_id} )"
    logger.info(
        f"{message}",
        extra={
            "notification": False,
            "change_amount": change_amount,
            "reason": reason,
            **hive_transfer.log_extra,
            **payment.log_extra,
        },
    )
    trx = await return_hive_transfer(
        hive_transfer=hive_transfer,
        reason=reason,
        amount=change_amount,
        nobroadcast=nobroadcast,
    )
    logger.info(
        f"Change transaction created for operation {hive_transfer.group_id_p}: {change_amount}",
        extra={"notification": True, "trx": trx, **hive_transfer.log_extra, **payment.log_extra},
    )

    # if trx:
    #     original_ledger_entry, ans = await update_ledger_entry_op(
    #         group_id=hive_transfer.group_id_p, op=hive_transfer
    #     )


async def calculate_hive_return_change(hive_transfer: TrackedTransfer, payment: Payment) -> Amount:
    """
    Calculate the change amount to return to the user after a Hive to Lightning transfer.
    This function computes the change based on the Hive transfer's conversion details and the payment amount.
    Args:
        hive_transfer (TrackedTransfer): The Hive transfer object containing conversion details.
        payment (Payment): The Payment object representing the sent payment.
    Returns:
        Amount: The calculated change amount to return to the user.
    """
    if hive_transfer.conv is None or hive_transfer.conv.is_unset():
        await hive_transfer.update_conv()
    if payment.conv is None or payment.conv.is_unset():
        await payment.update_conv()

    if not hive_transfer.conv or not payment.conv:
        logger.error(
            "Conversion details not found for operation, failed to update conversion.",
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError("Conversion details not found for operation")

    if hive_transfer.paywithsats:
        change_hive_amount = hive_transfer.amount.beam

    else:
        # payment.fee_msat is the lightning fee
        cost_of_payment_msat_pre_fee = payment.value_msat + payment.fee_msat
        # payment.conv.msats_fee is the Hive to Lightning conversion fee
        cost_of_payment_msat = cost_of_payment_msat_pre_fee + payment.conv.msats_fee

        # Value of payment and fee in Hive or HBD
        cost_of_payment_amount = Amount(
            "0.001 HIVE"
        )  # Default amount to return if no change is needed
        if hive_transfer.conv.conv_from == Currency.HIVE:
            cost_of_payment_hive_hbd = cost_of_payment_msat / 1_000 / hive_transfer.conv.sats_hive
            cost_of_payment_amount = Amount(f"{cost_of_payment_hive_hbd:.3f} HIVE")
        elif hive_transfer.conv.conv_from == Currency.HBD:
            cost_of_payment_hive_hbd = cost_of_payment_msat / 1_000 / hive_transfer.conv.sats_hbd
            cost_of_payment_amount = Amount(f"{cost_of_payment_hive_hbd:.3f} HBD")
        else:
            raise HiveToLightningError(
                f"Unknown currency: {hive_transfer.conv.conv_from} for change calculation"
            )

        logger.info(
            f"Cost of payment in {hive_transfer.conv.conv_from}: {cost_of_payment_amount} ({cost_of_payment_msat / 1_000:,.0f} sats)",
            extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
        )

        change_hive_amount = hive_transfer.amount.beam - cost_of_payment_amount

        # If change is less than 0.001 HIVE, no change transaction is needed just notification minimum
        if hive_transfer.conv.conv_from == Currency.HIVE and change_hive_amount < Amount(
            "0.001 HIVE"
        ):
            change_hive_amount = Amount("0.001 HIVE")
        elif hive_transfer.conv.conv_from == Currency.HBD and change_hive_amount < Amount(
            "0.001 HBD"
        ):
            change_hive_amount = Amount("0.001 HBD")

    hive_transfer.change_amount = AmountPyd(amount=change_hive_amount)
    hive_transfer.fee_conv = CryptoConversion(
        conv_from=Currency.MSATS,
        value=payment.conv.msats_fee,
        quote=await TrackedBaseModel.nearest_quote(timestamp=payment.timestamp),
    ).conversion

    # The conversion details await hive_transfer.update_conv()will be set when the
    logger.info(
        f"Change detected for operation {hive_transfer.group_id_p}: {change_hive_amount}",
        extra={"notification": False, **hive_transfer.log_extra, **payment.log_extra},
    )

    return change_hive_amount


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


async def convert_hive_to_keepsats(
    hive_transfer: TrackedTransfer, nobroadcast: bool = False
) -> None:
    """
    Converts a Hive transfer to Keepsats.

    Args:
        hive_transfer (TrackedTransfer): The Hive transfer to convert.
        nobroadcast (bool, optional): If True, the transaction will not be broadcast. Defaults to False.

    Returns:
        Amount | None: The converted amount in Keepsats, or None if conversion fails.
    """
    try:
        # Perform the conversion logic here
        keepsats_balance_before, net_sats = await get_keepsats_balance(
            cust_id=hive_transfer.from_account
        )
        logger.info(f"Keepsats balance for {hive_transfer.from_account}: {net_sats:,.0f} sats")
        ledger_entries, reason, amount_to_return = await hive_to_keepsats_deposit(hive_transfer)
        for entry in ledger_entries:
            logger.info(f"Ledger Entries for hive to keepsats conversion: {hive_transfer.log_str}")
            logger.info(entry)
            await entry.save()
        await asyncio.sleep(1)
        keepsats_balance_after, net_sats_after = await get_keepsats_balance(
            cust_id=hive_transfer.from_account
        )
        logger.info(
            f"Keepsats balance for {hive_transfer.from_account}: {net_sats_after:,.0f} sats "
            f"change: {net_sats_after - net_sats:,.0f} sats"
        )

        trx = await return_hive_transfer(
            hive_transfer=hive_transfer,
            reason=reason,
            amount=amount_to_return,
            nobroadcast=nobroadcast,
        )
        if trx:
            logger.info(
                f"Successfully converted Hive to Keepsats: {hive_transfer.log_str}",
                extra={"notification": True, **hive_transfer.log_extra},
            )
            return
        else:
            logger.error(
                f"Failed to create transaction during Hive to Keepsats conversion: {hive_transfer.log_str}",
                extra={"notification": False, **hive_transfer.log_extra},
            )
            raise HiveToLightningError("Failed to create transaction during conversion")

        return None
    except Exception as e:
        logger.exception(f"Failed to convert Hive to Keepsats: {e}")
        return None


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
        f"Processing return/change for: {hive_transfer.log_str}",
        extra={"notification": False, **hive_transfer.log_extra},
    )
    logger.info(
        f"Reason: {reason} amount: {amount}",
        extra={"reason": reason, "amount": amount, "nobroadcast": nobroadcast},
    )
    hive_client, server_account_name = await get_verified_hive_client(nobroadcast=nobroadcast)

    # We don't check the operation was already paid here because that is done in the processing function

    amount = amount or hive_transfer.amount.beam
    if not isinstance(amount, Amount):
        raise HiveToLightningError("Amount must be an instance of Amount")
    try:
        memo = f"{reason} | § {hive_transfer.short_id}{MEMO_FOOTER}"
        trx = await send_transfer(
            hive_client=hive_client,
            from_account=server_account_name,
            to_account=hive_transfer.from_account,  # Repay to the original sender
            amount=amount,
            memo=memo,
        )
        if trx:
            # MARK: 5. Update hive_transfer
            logger.info(
                f"Successfully paid reply to Hive to Lightning operation: {hive_transfer.log_str}",
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
            await TransferBase.update_quote()
            hive_transfer.change_conv = CryptoConversion(
                conv_from=return_amount.symbol,
                amount=return_amount,
                quote=TransferBase.last_quote,
            ).conversion
            return_amount_msat = hive_transfer.change_conv.msats
            # Now add the Hive reply to the original Hive transfer operation
            # MARK: 5. Update hive_transfer
            # TODO: Move this note of the reply id to the processing of the reply. complete_hive_to_lightning
            reason = f"Change transaction for operation {hive_transfer.group_id}: {trx.get('trx_id', '')}"
            hive_transfer.add_reply(
                reply_id=trx.get("trx_id", ""),
                reply_type="transfer",
                reply_msat=return_amount_msat,
                reply_error=None,
                reply_message=reason,
            )
            await hive_transfer.save()
            logger.info(
                f"Updated Hive transfer with reply: {hive_transfer.replies[-1]}",
                extra={"notification": False, **hive_transfer.log_extra},
            )
            return trx
        else:
            raise HiveTransferError("No transaction created during Hive to Lightning repayment")
    except HiveTransferError as e:
        message = f"Failed to repay Hive to Lightning operation: {e}"
        hive_transfer.add_reply(
            reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
        )
        await hive_transfer.save()
        logger.error(
            message,
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(message)

    except Exception as e:
        message = f"Unexpected error during Hive to Lightning repayment: {e}"
        hive_transfer.add_reply(
            reply_id="", reply_type="transfer", reply_error=str(e), reply_message=message
        )
        await hive_transfer.save()
        logger.error(
            message,
            extra={"notification": False, **hive_transfer.log_extra},
        )
        raise HiveToLightningError(message)


async def get_verified_hive_client(
    hive_role: HiveRoles = HiveRoles.server,
    nobroadcast: bool = False,
) -> Tuple[Hive, str]:
    """
    Asynchronously obtains a verified Hive client instance using server account credentials from the internal configuration.

    Args:
        nobroadcast (bool, optional): If True, disables broadcasting of transactions. Defaults to False.
        hive_role (HiveRoles, optional): The role to use for the Hive client. Defaults to HiveRoles.server.

    Returns:
        Tuple[Hive, str]: A tuple containing the initialized Hive client and the server account name.

    Raises:
        HiveToLightningError: If the server account configuration or required keys are missing.
    """
    hive_config = InternalConfig().config.hive

    hive_account = hive_config.get_hive_role_account(hive_role)

    if not hive_account:
        raise HiveToLightningError("Missing Hive server account configuration for repayment")

    memo_key = hive_account.memo_key or ""
    active_key = hive_account.active_key or ""
    if not memo_key or not active_key:
        raise HiveToLightningError("Missing Hive server account keys for repayment")

    hive_client = get_hive_client(
        keys=[
            hive_account.memo_key,
            hive_account.active_key,
        ],
        nobroadcast=nobroadcast,
    )
    return hive_client, hive_account.name


async def get_verified_hive_client_for_accounts(
    accounts: List[str],
    nobroadcast: bool = False,
) -> Hive:
    """
    Asynchronously obtains a verified Hive client instance for a list of accounts using server account credentials from the internal configuration.

    Args:
        accounts (List[str]): A list of Hive account names to verify.
        nobroadcast (bool, optional): If True, disables broadcasting of transactions. Defaults to False.

    Returns:
        Hive: An initialized Hive client instance.

    Raises:
        HiveToLightningError: If the server account configuration or required keys are missing.
    """
    hive_config = InternalConfig().config.hive
    hive_accounts = []
    keys = []
    for account in accounts:
        if hive_config.hive_accs.get(account):
            hive_account = hive_config.hive_accs[account]
            hive_accounts.append(hive_account)
            all_keys = hive_account.keys
            if all_keys:
                keys.extend(all_keys)

    hive_client = get_hive_client(
        keys=keys,
        nobroadcast=nobroadcast,
    )
    return hive_client
