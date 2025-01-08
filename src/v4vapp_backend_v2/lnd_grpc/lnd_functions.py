import json
import pickle
from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDConnectionError
from v4vapp_backend_v2.models.htlc_event_models import ChannelName
from v4vapp_backend_v2.grpc_models.lnd_events_group import LndChannelName


async def get_channel_name(
    channel_id: int,
    client: LNDClient = None,
    own_pub_key: str = None,
) -> LndChannelName:
    """
    Asynchronously retrieves the name of a channel given its ID and connection name.

    Args:
        channel_id (int): The ID of the channel to retrieve the name for.
        connection_name (str): The name of the connection to use for the LND client.

    Returns:
        ChannelName: An instance of ChannelName containing the channel ID and the name of the
                     channel. If the channel ID is invalid or an error occurs, the name will be
                     "Unknown".

    Raises:
        Exception: Logs any exceptions that occur during the process and returns a ChannelName
                   with "Unknown" name.
    """
    if not channel_id:
        return LndChannelName(channel_id=0, name="Unknown")

    request = lnrpc.ChanInfoRequest(chan_id=channel_id)
    try:
        response = await client.call(
            client.lightning_stub.GetChanInfo,
            request,
        )
        chan_info = MessageToDict(response, preserving_proto_field_name=True)
        node1_pub = chan_info.get("node1_pub")
        node2_pub = chan_info.get("node2_pub")

        if not own_pub_key:
            own_pub_key = client.get_info.identity_pubkey
            # own_pub_key = await get_node_pub_key(connection_name)

        # Determine the partner node's public key
        partner_pub_key = node1_pub if own_pub_key != node1_pub else node2_pub

        # Get the node info of the partner node
        node_info = await get_node_info(partner_pub_key, client)
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
        lnrpc.NodeInfo: The information about the node, or an empty dictionary if an error occurs.

    Raises:
        Exception: If there is an error while fetching the node information.
    """

    try:
        logger.debug(f"get_node_info: {pub_key}")
        request = lnrpc.NodeInfoRequest(pub_key=pub_key)
        response: lnrpc.NodeInfo = await client.call(
            client.lightning_stub.GetNodeInfo,
            request,
        )
        logger.debug(f"get_node_info: {pub_key} {response.node.alias}")
        return response
    except LNDConnectionError as e:
        try:
            if e.args[1]._details == "unable to find node":
                logger.warning(f"get_node_info: {pub_key} not found")
                return lnrpc.NodeInfo()
        except Exception as e:
            pass
        logger.exception(e)
        return lnrpc.NodeInfo()

    except Exception as e:
        logger.info(f"Failure get_node_info: {pub_key}")
        logger.exception(e)
        return lnrpc.NodeInfo()


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
        decode_request = lnrpc.PayReqString(pay_req=pay_request)
        decode_response: lnrpc.PayReq = await client.call(
            client.lightning_stub.DecodePayReq,
            decode_request,
        )

        decoded_pay_req = MessageToDict(
            decode_response, preserving_proto_field_name=True
        )
        destination_pub_key = decoded_pay_req.get("destination")

        if not destination_pub_key:
            raise ValueError("Destination public key not found in payment request")

        # Get the node info of the destination node
        node_info = await get_node_info(destination_pub_key, client)
        return node_info.node.alias or destination_pub_key[:10]
    except Exception as e:
        logger.exception(e)
        return "Unknown"
