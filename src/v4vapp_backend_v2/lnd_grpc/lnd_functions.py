import asyncio
import base64
from decimal import Decimal

from google.protobuf.json_format import MessageToDict
from grpc.aio import AioRpcError
from pydantic import ValidationError  # type: ignore

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.grpc_models.lnd_events_group import LndChannelName
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDConnectionError
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment


class LNDPaymentError(Exception):
    pass


class LNDPaymentExpired(LNDPaymentError):
    """
    Exception raised when a Lightning payment has expired.
    This is a custom exception to handle specific payment expiration scenarios.
    """

    pass


async def get_channel_name(
    channel_id: int,
    lnd_client: LNDClient | None = None,
    own_pub_key: str | None = None,
) -> LndChannelName:
    """
    Asynchronously retrieves the name of a channel given its ID and connection name.

    Args:
        channel_id (int): The ID of the channel to retrieve the name for.
        connection_name (str): The name of the connection to use for the LND client.

    Returns:
        ChannelName: An instance of ChannelName containing the channel ID and the
                     name of the channel. If the channel ID is invalid or an error
                     occurs, the name will be "Unknown".

    Raises:
        Exception: Logs any exceptions that occur during the process and returns
                   a ChannelName with "Unknown" name.
    """
    if not channel_id:
        return LndChannelName(channel_id=0, name="Unknown")

    if not lnd_client:
        lnd_config = InternalConfig().config.lnd_config
        lnd_client = LNDClient(connection_name=lnd_config.default)

    request = lnrpc.ChanInfoRequest(chan_id=channel_id)
    try:
        response = await lnd_client.call(
            lnd_client.lightning_stub.GetChanInfo,
            request,
        )
        chan_info = MessageToDict(response, preserving_proto_field_name=True)
        node1_pub = chan_info.get("node1_pub", "")
        node2_pub = chan_info.get("node2_pub", "")

        if not own_pub_key:
            own_pub_key = lnd_client.get_info.identity_pubkey
            # own_pub_key = await get_node_pub_key(connection_name)

        # Determine the partner node's public key
        partner_pub_key = node1_pub if own_pub_key != node1_pub else node2_pub

        # Get the node info of the partner node
        node_info = await get_node_info(partner_pub_key, lnd_client)
        # node_info = MessageToDict(response, preserving_proto_field_name=True)

        return LndChannelName(channel_id=channel_id, name=node_info.node.alias)
    except LNDConnectionError as e:
        try:
            if "edge not found" in str(e.details()).lower():
                logger.warning(
                    f"{lnd_client.icon} get_channel_name: channel {channel_id} not found"
                )
                return LndChannelName(channel_id=channel_id, name="Unknown")
        except Exception:
            pass
        logger.exception(e)
        return LndChannelName(channel_id=channel_id, name="Unknown")
    except Exception as e:
        logger.exception(e)
        return LndChannelName(channel_id=channel_id, name="Unknown")


async def get_node_info(pub_key: str, client: LNDClient) -> lnrpc.NodeInfo:
    """
    Fetches information about a node on the Lightning Network.

    Args:
        pub_key (str): The public key of the node to fetch information for.
        client (LNDClient): The LND client to use for making the request.

    Returns:
        lnrpc.NodeInfo: The information about the node, or an empty dictionary
        if an error occurs.

    Raises:
        Exception: If there is an error while fetching the node information.
    """

    try:
        logger.debug(f"get_node_info: {pub_key}")
        request = lnrpc.NodeInfoRequest(pub_key=pub_key)
        response = await client.lightning_stub.GetNodeInfo(request)
        logger.debug(f"get_node_info: {pub_key} {response.node.alias}")
        return response
    except AioRpcError as e:
        logger.debug(f"{client.icon} get_node_info {e.details()}", extra={"original_error": e})
        return lnrpc.NodeInfo()

    except LNDConnectionError as e:
        try:
            if e.args[1]._details == "unable to find node":
                logger.warning(f"{client.icon} get_node_info: {pub_key} not found")
                return lnrpc.NodeInfo()
        except Exception:
            pass
        logger.exception(e)
        return lnrpc.NodeInfo()

    except Exception as e:
        logger.info(f"{client.icon} Failure get_node_info: {pub_key}")
        logger.exception(e)
        return lnrpc.NodeInfo()


async def get_pay_req_from_pay_request(pay_request: str, lnd_client: LNDClient) -> lnrpc.PayReq:
    """
    Retrieve the invoice from a payment request.

    Args:
        pay_request (str): The payment request string.
        lnd_client (LNDClient): An instance of the LNDClient.

    Returns:
        lnrpc.PayReq: The PayReq object.
    """
    if not lnd_client:
        raise ValueError("LNDClient instance is required")

    try:
        # Decode the payment request
        if pay_request == "":
            logger.debug("Empty payment request", extra={"notification": False})
            return lnrpc.PayReq()
        decode_request = lnrpc.PayReqString(pay_req=pay_request)
        decode_response: lnrpc.PayReq = await lnd_client.call(
            lnd_client.lightning_stub.DecodePayReq,
            decode_request,
        )
        return decode_response
    except Exception as e:
        logger.exception(e)
        return lnrpc.PayReq()


async def get_node_alias_from_pay_request(pay_request: str, client: LNDClient) -> str:
    """
    Retrieve the node alias from a payment request.

    Args:
        pay_request (str): The payment request string.
        client (LNDClient): An instance of the LNDClient.

    Returns:
        str: The alias of the destination node.
    """
    try:
        # Decode the payment request
        if pay_request == "":
            logger.debug("Empty payment request", extra={"notification": False})
            return "Unknown"
        decode_request = lnrpc.PayReqString(pay_req=pay_request)
        decode_response: lnrpc.PayReq = await client.call(
            client.lightning_stub.DecodePayReq,
            decode_request,
        )

        decoded_pay_req = MessageToDict(decode_response, preserving_proto_field_name=True)
        destination_pub_key = decoded_pay_req.get("destination")

        if not destination_pub_key:
            raise ValueError("Destination public key not found in payment request")

        # Get the node info of the destination node
        node_info = await get_node_info(destination_pub_key, client)
        return node_info.node.alias or destination_pub_key[:10]
    except Exception as e:
        logger.exception(e)
        return "Unknown"


async def get_node_alias_from_pub_key(pub_key: str, lnd_client: LNDClient) -> str:
    """
    Retrieve the node alias from a public key.

    Args:
        pub_key (str): The public key of the node.
        lnd_client (LNDClient): An instance of the LNDClient.

    Returns:
        str: The alias of the node.
    """
    try:
        # Get the node info of the destination node
        node_info = await get_node_info(pub_key, lnd_client)
        return node_info.node.alias or pub_key[:10]
    except Exception as e:
        logger.exception(e)
        return "Unknown"


def b64_hex_transform(plain_str: str) -> str:
    """Returns the b64 transformed version of a hex string"""
    a_string = bytes.fromhex(plain_str)
    return base64.b64encode(a_string).decode()


def b64_transform(plain_str: str) -> str:
    """Returns the b64 transformed version of a string"""
    return base64.b64encode(plain_str.encode()).decode()


async def send_lightning_to_pay_req(
    pay_req: PayReq,
    lnd_client: LNDClient,
    group_id: str = "",
    cust_id: str = "",
    paywithsats: bool = False,
    chat_message: str = "",
    amount_msat: Decimal = Decimal(0),
    fee_limit_ppm: int = 0,
) -> Payment:
    """
    Send a payment to a Lightning Network invoice using the provided payment request.

    Args:
        pay_req (PayReq): The payment request object containing invoice details.
        lnd_client (LNDClient): An instance of the LNDClient to interact with the Lightning node.
        group_id (str, optional): Identifier for the group, used in custom records. Defaults to "".
        chat_message (str, optional): Chat message to attach to the payment, used in custom records. Defaults to "".
        amount_msat (int, optional): Amount to pay in millisatoshis, required for zero-value invoices.
                    Defaults to 0. If set will be ignored if the pay_req includes an amount
        fee_limit_ppm (int, optional): Fee limit in parts per million. Defaults to setting in config.
        use_keepsats (bool, optional): Whether to use Keepsats for the payment. Defaults to False.

    Raises:
        ValueError: If the LNDClient instance is not provided.
        LNDPaymentError: If the payment amount is zero or not specified, payment validation fails, or payment fails to send.
        ValidationError: If the payment response cannot be validated.

    Returns:
        Payment: The payment object containing details of the sent payment.
    """

    def simulate_error_for_testing():
        # Simulate an AioRpcError for testing: 'already paid' detail (matches tests)
        raise AioRpcError(
            code=1,
            initial_metadata=None,
            trailing_metadata=None,
            details="already paid",
            debug_error_string="invoice already paid",
        )

    if not lnd_client:
        raise ValueError("LNDClient instance is required")

    # for keysend we need a pre_image and to put it in 5482373484
    lnd_config = InternalConfig().config.lnd_config

    fee_limit_ppm = fee_limit_ppm or lnd_config.lightning_fee_limit_ppm

    zero_value_pay_req, payment_amount_msat = test_zero_value_pay_req(pay_req, amount_msat)

    pay_with_sats: str = str(paywithsats)

    dest_custom_records = {
        # 5482373484: b64_hex_transform(pre_image), # Used in keysend to carry the pre-image
        # 818818: b64_transform(hive_accname),   Used in V4Vapp podcasting
        34349334: chat_message.encode(),  # Used in V4Vapp
        1818181818: group_id.encode(),  # Used in V4Vapp
        1818181819: cust_id.encode(),  # Used in V4Vapp
        1818181820: pay_with_sats.encode(),  # Used in V4Vapp
    }
    pay_req.dest_alias = pay_req.dest_alias or await get_node_alias_from_pub_key(
        pay_req.destination, lnd_client
    )
    logger.info(pay_req.log_str)

    request_params = {}
    await lnd_client.node_get_info
    if pay_req.destination == lnd_client.get_info.identity_pubkey:
        logger.info(
            "Payment address is the same as the node's identity pubkey set fee limit to minimum"
        )
        fee_limit_msat = 100_000
        request_params["allow_self_payment"] = True
        request_params["outgoing_chan_ids"] = [800082725764071425]
        # TODO: replace this hard coded channel ID with a dynamic one
    else:
        fee_limit_msat = (
            int(payment_amount_msat * fee_limit_ppm / 1_000_000)
            + lnd_config.lightning_fee_base_msats
        )
    # Must prevent 0 fee limit which is an unlimited fee.
    fee_limit_msat = max(fee_limit_msat, 1000)
    logger.info(f"Fee limit: {fee_limit_msat / 1000:.0f} sats")
    failure_reason = "Unknown Failure"
    # Construct the SendPaymentRequest parameters
    request_params = request_params | {
        "payment_request": pay_req.pay_req_str,
        "timeout_seconds": 600,
        "fee_limit_msat": fee_limit_msat,
        "dest_custom_records": dest_custom_records,
    }

    # Add amount_msat if it's a zero-value invoice
    if zero_value_pay_req:
        request_params["amt_msat"] = amount_msat

    payment_dict = {}
    payment_id = f"{lnd_client.icon} {pay_req.pay_req_str[:14]}"
    error_message = ""
    response_queue = asyncio.Queue()
    logging_task = asyncio.create_task(log_payment_in_process(payment_id, response_queue))
    try:
        # simulate_error_for_testing()
        # Create the SendPaymentRequest object
        request = routerrpc.SendPaymentRequest(**request_params)
        async for payment_resp in lnd_client.router_stub.SendPaymentV2(request):
            payment_dict = MessageToDict(payment_resp, preserving_proto_field_name=True)
            await response_queue.put(payment_dict)
            failure_reason = payment_dict.get("failure_reason", "Unknown Failure")

    except AioRpcError as e:
        error_message = f"{payment_id} Failed to send payment: {e}"
        if e.details() and "invoice expired" in str(e.details()).lower():
            error_message = f"{payment_id} Payment expired: {e.details()}"
        elif e.details() and "already paid" in str(e.details()).lower():
            error_message = f"{payment_id} Payment already paid: {e.details()}"
        elif e.details():
            error_message = f"{payment_id} Payment failed: {e.details()}"
        raise LNDPaymentError(error_message)

    except Exception as e:
        error_message = f"{payment_id} Unexpected problem paying Lightning invoice"
        logger.exception(e)
        raise LNDPaymentError(error_message)

    finally:
        # Ensure the logging task is cancelled after payment processing
        if error_message:
            logger.warning(error_message, extra={"notification": False})
        logging_task.cancel()
        try:
            await logging_task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(
                f"{payment_id} Error during logging task: {e}", extra={"notification": False}
            )

    # NOTHING VITAL HAPPENS IF A PAYMENT IS A HOLD INVOICE AND THIS IS INTERRUPTED
    # The payment for the hold invoice, if it happens, will be found by db_monitor
    # Check if we received a payment_dict and if the payment status is valid
    if payment_dict:
        try:
            payment = Payment.model_validate(payment_dict)
            logger.info(
                f"{payment_id} {payment.log_str}",
                extra={
                    "notification": False,
                    **payment.log_extra,
                },
            )
        except ValidationError as e:
            logger.error(f"{payment_id} Payment validation error: {e}")
            raise LNDPaymentError(f"Payment validation error: {e}")

        if payment.status and payment.status.value == "SUCCEEDED":
            return payment

        elif payment.status and payment.status.value == "FAILED":
            raise LNDPaymentError(f"{payment_id} Payment failed: {payment.failure_reason}")

    logger.error(
        f"Failed to retrieve payment_dict or payment status paying {pay_req.pay_req_str}",
        extra={"notification": False},
    )
    raise LNDPaymentError(f"{payment_id} Payment failed {failure_reason}")


async def log_payment_in_process(payment_id: str, response_queue: asyncio.Queue) -> None:
    """
    Logs payment status and progress for the given payment ID.
    Processes payment dictionaries from the queue, logs status immediately, and logs periodic updates.

    Args:
        payment_id (str): The ID of the payment.
        response_queue (asyncio.Queue): Queue to receive payment dictionary objects.
    """
    status = "STATUS_UNSET"
    while True:
        try:
            # Wait for a payment dictionary or timeout after 30 seconds
            try:
                payment_dict = await asyncio.wait_for(response_queue.get(), timeout=30)
                status = payment_dict.get("status", "STATUS_UNSET")
                # Log the payment status from the payment dictionary
                failure_reason = payment_dict.get("failure_reason", "FAILURE_REASON_UNSET")
                logger.info(
                    f"{payment_id} Status: {status} - Failure {failure_reason}",
                    extra={
                        "notification": False,
                        "payment_dict": payment_dict,
                    },
                )
                response_queue.task_done()
            except asyncio.TimeoutError:
                # No new payment dictionary, log periodic update
                logger.info(
                    f"{payment_id} Payment still in process: {status}",
                    extra={
                        "notification": False,
                    },
                )
        except asyncio.CancelledError:
            logger.info(f"{payment_id} Payment logging ended by successful payment")
            raise
        except Exception as e:
            logger.error(f"{payment_id} Error logging payment in process: {e}")
            break


def test_zero_value_pay_req(pay_req: PayReq, amount_msat: Decimal) -> tuple[bool, Decimal]:
    """
    Checks if the given payment request (`pay_req`) is a zero-value invoice and determines
    the effective payment amount in millisatoshis.
    Args:
        pay_req (PayReq): The payment request object containing invoice details.
        amount_msat (int): The amount to pay in millisatoshis, provided externally.
    Returns:
        tuple[bool, int]:
            - A boolean indicating whether the payment request is a zero-value invoice.
            - The effective payment amount in millisatoshis.
    Raises:
        LNDPaymentError: If the payment request is zero-value and the provided amount is also zero.
    Logs:
        Logs an info message if the payment request is zero-value and a non-zero amount is provided.
    """

    zero_value_pay_req = pay_req.is_zero_value
    # Check the payment amount
    if zero_value_pay_req and amount_msat == 0:
        raise LNDPaymentError("Payment amount is zero or not specified")

    if zero_value_pay_req and amount_msat > 0:
        logger.info(f"Payment amount is zero in pay_req, using amount_msat: {amount_msat} msat")
    payment_amount_msat = max(pay_req.value_msat, (pay_req.value * Decimal(1000)), amount_msat)
    return zero_value_pay_req, Decimal(payment_amount_msat)
