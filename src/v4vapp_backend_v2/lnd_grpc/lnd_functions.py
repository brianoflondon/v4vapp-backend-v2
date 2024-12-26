from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.htlc_event_models import ChannelName


async def get_channel_name(
    channel_id: int, connection_name: str, own_pub_key: str = None
) -> ChannelName:
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
        return ChannelName(channel_id=0, name="Unknown")
    async with LNDClient(connection_name=connection_name) as client:
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
                own_pub_key = await get_node_pub_key(connection_name)

            # Determine the partner node's public key
            partner_pub_key = node1_pub if own_pub_key != node1_pub else node2_pub

            # Get the node info of the partner node
            response = await client.call(
                client.lightning_stub.GetNodeInfo,
                lnrpc.NodeInfoRequest(pub_key=partner_pub_key),
            )
            node_info = MessageToDict(response, preserving_proto_field_name=True)
            return ChannelName(channel_id=channel_id, name=node_info["node"]["alias"])
        except Exception as e:
            logger.exception(e)
            return ChannelName(channel_id=channel_id, name="Unknown")


async def get_node_pub_key(connection_name: str) -> str:
    """
    Retrieve the public key of a node.

    This function establishes an asynchronous connection to an LND (Lightning Network
    Daemon) client using the provided connection name. It then calls the `GetInfo`
    method on the client's lightning stub to obtain information about the node, and
    returns the node's public key.

    Args:
        connection_name (str): The name of the connection to use for the LND client.

    Returns:
        str: The public key of the node.
    """
    try:
        async with LNDClient(connection_name=connection_name) as client:
            response = await client.call(
                client.lightning_stub.GetInfo,
                lnrpc.GetInfoRequest(),
            )
            return response.identity_pubkey
    except Exception as e:
        logger.error(f"Error getting node pub key {e}", exc_info=True)
        return ""
