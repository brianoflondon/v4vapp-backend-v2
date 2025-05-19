import asyncio
import base64
from collections.abc import Callable
from typing import Any, Mapping

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

node_alias_cache = {}
LIGHTNING_FEE_LIMIT_PPM = 1


class LNDPaymentError(Exception):
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
        # response: lnrpc.NodeInfo = await client.call(
        #     client.lightning_stub.GetNodeInfo,
        #     request,
        # )
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
    chat_message: str = "",
    amount_msat: int = 0,
    fee_limit_ppm: int = LIGHTNING_FEE_LIMIT_PPM,
    callback: Callable | None = None,
    async_callback: Callable | None = None,
    callback_args: Mapping[str, Any] = {},
) -> None:
    """
    Send a payment to a Lightning Network invoice.

    Args:
        pay_request (str): The payment request string.
        lnd_client (LNDClient): An instance of the LNDClient.

    Returns:
        lnrpc.SendResponse: The response from the send payment request.
    """
    if not lnd_client:
        raise ValueError("LNDClient instance is required")

    # for keysend we need a pre_image and to put it in 5482373484

    zero_value_pay_req = pay_req.value == 0 and pay_req.value_msat == 0
    # Check the payment amount
    if zero_value_pay_req and amount_msat == 0:
        raise LNDPaymentError("Payment amount is zero or not specified")

    if zero_value_pay_req and amount_msat > 0:
        logger.info(f"Payment amount is zero in pay_req, using amount_msat: {amount_msat} msat")

    dest_custom_records = {
        # 5482373484: b64_hex_transform(pre_image), # Used in keysend
        # 818818: b64_transform(hive_accname),   Used in V4Vapp podcasting
        34349334: chat_message.encode(),  # Used in V4Vapp
        1818181818: group_id.encode(),  # Used in V4Vapp
    }
    dest_alias = await get_node_alias_from_pay_request(
        pay_request=pay_req.pay_req_str,
        client=lnd_client,
    )
    logger.info(f"Destination alias: {dest_alias}")
    logger.info(f"Destination pubkey: {pay_req.destination}")
    await lnd_client.node_get_info
    if pay_req.destination == lnd_client.get_info.identity_pubkey:
        logger.info(
            "Payment address is the same as the node's identity pubkey set fee limit to minimum"
        )
        fee_limit_msat = 10
    else:
        fee_limit_msat = int(pay_req.value_msat * fee_limit_ppm / 1_000_000)
    # Must prevent 0 fee limit which is an unlimited fee.
    fee_limit_msat = max(fee_limit_msat, 1)
    logger.info(f"Fee limit: {fee_limit_msat} msat")

    try:
        request = routerrpc.SendPaymentRequest(
            payment_request=pay_req.pay_req_str,
            timeout_seconds=600,
            fee_limit_msat=fee_limit_msat,
            allow_self_payment=True,
            dest_custom_records=dest_custom_records,
            # first_hop_custom_records=first_hop_custom_records,
            outgoing_chan_ids=[800082725764071425],
        )
        if zero_value_pay_req:
            request.amt_msat = amount_msat

        payment_dict = {}
        async for payment_resp in lnd_client.router_stub.SendPaymentV2(request):
            payment_dict = MessageToDict(payment_resp, preserving_proto_field_name=True)
            logger.info(
                f"Status: {lnrpc.Payment.PaymentStatus.Name(payment_resp.status)} - "
                f"Failure {lnrpc.PaymentFailureReason.Name(payment_resp.failure_reason)}",
                extra={
                    "notification": False,
                    "payment": payment_dict,
                },
            )
        if payment_dict:
            try:
                payment = Payment.model_validate(payment_dict)
                logger.info(
                    f"{lnd_client.icon} {payment.log_str}",
                    extra={
                        "notification": False,
                        "payment": payment.model_dump(),
                    },
                )
                if callback:
                    callback(payment, **callback_args)
                if async_callback:
                    asyncio.create_task(async_callback(payment, **callback_args))
                else:
                    return
            except ValidationError as e:
                logger.error(f"{lnd_client.icon} Payment validation error: {e}")
                raise LNDPaymentError(f"Payment validation error: {e}")
        raise LNDPaymentError(
            f"{lnd_client.icon} Payment failed {lnrpc.PaymentFailureReason.Name(payment_resp.failure_reason)}"
        )
    except AioRpcError as e:
        logger.exception(
            f"{lnd_client.icon} Problem paying Lightning invoice", extra={"notification": False}
        )
        raise LNDPaymentError(f"{lnd_client.icon} Failed to send payment: {e}")

    except Exception as e:
        logger.exception(
            f"{lnd_client.icon} Problem paying Lightning invoice", extra={"notification": False}
        )
        raise LNDPaymentError(f"{lnd_client.icon} Failed to send payment: {e}")
