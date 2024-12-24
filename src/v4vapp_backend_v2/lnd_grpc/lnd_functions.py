
from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.htlc_event_models import ChannelName


async def get_channel_name(channel_id: int) -> ChannelName:
    if not channel_id:
        return ChannelName(channel_id=0, name="Unknown")
    async with LNDClient() as client:
        request = ln.ChanInfoRequest(chan_id=channel_id)
        try:
            response = await client.call(
                client.lightning_stub.GetChanInfo,
                request,
            )
            chan_info = MessageToDict(response, preserving_proto_field_name=True)
            pub_key = chan_info.get("node2_pub")
            if pub_key:
                response = await client.call(
                    client.lightning_stub.GetNodeInfo,
                    ln.NodeInfoRequest(pub_key=pub_key),
                )
                node_info = MessageToDict(response, preserving_proto_field_name=True)
                return ChannelName(
                    channel_id=channel_id, name=node_info["node"]["alias"]
                )
            return ChannelName(channel_id=channel_id, name="Unknown")
        except Exception as e:
            logger.exception(e)
            pass
