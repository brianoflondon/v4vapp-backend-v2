import asyncio
import json
from typing import Any, AsyncGenerator

from google.protobuf.json_format import MessageToDict

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config

# Create a temporary file
db = MyDB()


async def subscribe_invoices(
    add_index: int, settle_index: int
) -> AsyncGenerator[LNDInvoice, None]:
    async with LNDClient() as client:
        request_sub = ln.InvoiceSubscription(
            add_index=add_index, settle_index=settle_index
        )
        try:
            async for inv in client.call_async_generator(
                client.lightning_stub.SubscribeInvoices,
                request_sub,
                call_name="SubscribeInvoices",
            ):
                inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
                invoice = LNDInvoice.model_validate(inv_dict)
                yield invoice
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeInvoices"
            )
            raise e
        except Exception as e:
            logger.error(e)
            raise e


async def get_channel_name(channel_id: int) -> str:
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
                return node_info["node"]["alias"]
            return chan_info
        except Exception as e:
            logger.error(e)
            pass


async def subscribe_invoices_loop() -> None:
    error_codes: set[str] = set()
    add_index = 0
    settle_index = 0
    while True:
        logger.info("Subscribing to invoices")
        logger.info(f"Add index: {add_index} - Settle index: {settle_index}")
        try:
            async for invoice in subscribe_invoices(add_index, settle_index):
                if error_codes:
                    logger.info(
                        f"✅ Error codes cleared {error_codes}",
                        extra={
                            "telegram": True,
                            "error_code_clear": error_codes,
                        },
                    )
                    error_codes.clear()
                if invoice.settled:
                    logger.info(
                        f"✅ Settled invoice {invoice.add_index} with memo "
                        f"{invoice.memo} {invoice.value:,.0f} sats",
                        extra={"telegram": True},
                    )
                    most_recent = invoice
                    settle_index = most_recent.settle_index
                else:
                    send_telegram = False if invoice.is_keysend else True
                    logger.info(
                        f"✅ Valid   invoice {invoice.add_index} with memo "
                        f"{invoice.memo} {invoice.value:,.0f} sats",
                        extra={"telegram": send_telegram},
                    )
                    most_recent = invoice
                    db.update_most_recent(invoice)
                    add_index = most_recent.add_index

        except LNDSubscriptionError as e:
            logger.warning(e)
            pass

        except Exception as e:
            logger.error(e)
            raise e


async def subscribe_htlc_events() -> AsyncGenerator[Any, None]:
    async with LNDClient() as client:
        request_sub = routerrpc.SubscribeHtlcEventsRequest()
        try:
            async for htlc_event in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request_sub,
                call_name="SubscribeHtlcEvents",
            ):
                htlc_dict = MessageToDict(htlc_event, preserving_proto_field_name=True)
                yield htlc_dict
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            raise e
        except Exception as e:
            logger.error(e)
            raise e


async def subscribe_htlc_events_loop() -> None:
    while True:
        logger.info("Subscribing to HTLC events")
        try:
            async for htlc_dict in subscribe_htlc_events():
                incoming_channel_name = htlc_dict.get("incoming_channel_id", "")
                outgoing_channel_name = htlc_dict.get("outgoing_channel_id", "")
                if htlc_dict.get("incoming_channel_id"):
                    incoming_channel_name = await get_channel_name(
                        int(htlc_dict["incoming_channel_id"])
                    )
                if htlc_dict.get("outgoing_channel_id"):
                    outgoing_channel_name = await get_channel_name(
                        int(htlc_dict["outgoing_channel_id"])
                    )

                logger.info(f"{incoming_channel_name} -> {outgoing_channel_name}")
                logger.info("\n" + json.dumps(htlc_dict, indent=2))

        except LNDSubscriptionError as e:
            logger.warning(e)
            pass
        except Exception as e:
            logger.error(e)
            raise e


async def main() -> None:
    logger.info("Starting LND gRPC client")

    try:
        async with LNDClient() as client:
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            logger.info(f"Balance: {balance.local_balance.sat:,.0f} sats")

            channels = await client.call(
                client.lightning_stub.ListChannels,
                ln.ListChannelsRequest(),
            )
            channels_dict = MessageToDict(channels, preserving_proto_field_name=True)
            tasks = []
            for channel in channels_dict.get("channels", []):
                tasks.append(get_channel_name(int(channel["chan_id"])))
                # name =  get_channel_name(int(channel["chan_id"]))
            names = await asyncio.gather(*tasks)
            for name in names:
                logger.info(name)

            tasks = [subscribe_invoices_loop(), subscribe_htlc_events_loop()]
            await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.warning("❌ LND gRPC client stopped keyboard")
    except LNDFatalError as e:
        logger.error("❌ LND gRPC client stopped fatal error")
        raise e
    except Exception as e:
        logger.error("❌ LND gRPC client stopped error")
        logger.error(e)
        raise e

    logger.info("❌ LND gRPC client stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
        logger.info("✅ LND gRPC client stopped")

    except KeyboardInterrupt:
        logger.warning(
            "✅ LND gRPC client stopped by keyboard", extra={"telegram": False}
        )

    except LNDFatalError as e:
        logger.error(
            "❌ LND gRPC client stopped by fatal error", extra={"telegram": False}
        )
        logger.error(e, extra={"telegram": False})

    except Exception as e:
        logger.error("❌ LND gRPC client stopped by error", extra={"telegram": False})
        logger.error(e, extra={"telegram": False})
        raise e
