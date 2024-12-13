import asyncio
import json
from pprint import pprint
from typing import Any, AsyncGenerator, Dict, Generator, List

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc
from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.database.db import MyDB
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.models.htlc_event_models import (
    EventType,
    HtlcEvent,
    HtlcTrackingList,
)
from v4vapp_backend_v2.models.lnd_models import LNDInvoice

config = InternalConfig().config

# Create a temporary file
db = MyDB()

tracking = HtlcTrackingList()


def read_last_50_lines(file_path: str) -> Generator[Dict[str, Any], None, None]:
    with open(file_path, "r") as file:
        # Read all lines in the file
        lines = file.readlines()

        # Get the last 50 lines
        last_50_lines = lines[-50:]

        # Parse each line as JSON and yield the htlc_event data
        for line in last_50_lines:
            try:
                log_entry = json.loads(line)
                if "htlc_event" in log_entry:
                    yield HtlcEvent.model_validate(log_entry["htlc_event"])

            except ValidationError as e:
                logger.error(e)
                continue
            except Exception as e:
                logger.error(e)
                continue


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


class ChannelName(BaseModel):
    channel_id: int
    name: str


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
                    settle_index = invoice.settle_index
                else:
                    add_index = invoice.add_index
                send_telegram = False if invoice.is_keysend else True
                invoice.invoice_log(logger.info, send_telegram)
                db.update_most_recent(invoice)

        except LNDSubscriptionError as e:
            logger.warning(e)
            pass

        except Exception as e:
            logger.error(e)
            raise e


async def subscribe_htlc_events() -> AsyncGenerator[HtlcEvent, None]:
    async with LNDClient() as client:
        request_sub = routerrpc.SubscribeHtlcEventsRequest()
        try:
            async for htlc in client.call_async_generator(
                client.router_stub.SubscribeHtlcEvents,
                request_sub,
                call_name="SubscribeHtlcEvents",
            ):
                htlc_data = MessageToDict(htlc, preserving_proto_field_name=True)
                logger.debug(
                    "RAW htlc_event_data object\n" + json.dumps(htlc_data, indent=2),
                    extra={"htlc_data": htlc_data},
                )
                try:
                    htlc_event = HtlcEvent.model_validate(htlc_data)
                except ValidationError as e:
                    logger.warning(
                        "htlc_event_data object\n" + json.dumps(htlc_data, indent=2),
                        extra={"htlc_data": htlc_data},
                    )
                    logger.error(e)
                    continue
                yield htlc_event
        except LNDSubscriptionError as e:
            await client.check_connection(
                original_error=e.original_error, call_name="SubscribeHtlcEvents"
            )
            raise e
        except Exception as e:
            logger.error(e)
            raise e


class HtlcLiveEvent(BaseModel):
    incoming_htlc_id: int | None = None
    outgoing_htlc_id: int | None = None
    htlc_event: HtlcEvent


class HtlcLiveEvents(BaseModel):
    live: Dict[EventType, List[HtlcLiveEvent]] = {}


async def subscribe_htlc_events_loop() -> None:

    while True:
        logger.info("Subscribing to HTLC events")
        try:
            async for htlc_event in subscribe_htlc_events():
                htlc_id = tracking.add_event(htlc_event)
                message = tracking.message(htlc_id)
                complete = tracking.complete_group(htlc_id)
                logger.info(
                    message,
                    extra={
                        "telegram": complete,
                        "htlc_event": htlc_event.model_dump(exclude_none=True),
                        "complete": complete,
                    },
                )
                logger.info(tracking.model_dump_json(indent=2))
                if complete:
                    logger.info("✅ Complete group")
                    logger.info(f"Delete group {htlc_id}")
                    tracking.delete_event(htlc_id)

                # print(tracking.message(htlc_id))
                # tasks = [
                #     get_channel_name(htlc_event.incoming_channel_id),
                #     get_channel_name(htlc_event.outgoing_channel_id),
                # ]
                # incoming_channel, outgoing_channel = await asyncio.gather(*tasks)
                # if htlc_event.has_forward_message:
                #     forward_message = htlc_event.forward_message(
                #         incoming_channel.name, outgoing_channel.name
                #     )
                #     logger.info(
                #         forward_message,
                #         extra={
                #             "telegram": True,
                #             "htlc_event": htlc_event.model_dump(exclude_none=True),
                #             "forward_message": forward_message,
                #         },
                #     )
                # elif htlc_event.is_forward_fail:
                #     logger.info(
                #         (
                #             f"💰 {htlc_event.incoming_htlc_id} Fail"
                #             f"from: {incoming_channel.name} "
                #             f"to: {outgoing_channel.name} "
                #             f"{htlc_event.event_type}"
                #         ),
                #         extra={
                #             "telegram": True,
                #             "htlc_event": htlc_event.model_dump(exclude_none=True),
                #         },
                #     )
                # else:
                #     logger.info(
                #         (
                #             f"htlc_event object {htlc_event.incoming_htlc_id} "
                #             f"from: {incoming_channel.name} "
                #             f"to: {outgoing_channel.name} "
                #             f"{htlc_event.event_type}"
                #         ),
                #         extra={"htlc_event": htlc_event.model_dump(exclude_none=True)},
                #     )
                #     logger.debug(
                #         htlc_event.model_dump_json(exclude_none=True, indent=2)
                #     )
        except LNDSubscriptionError as e:
            logger.warning(e)
            pass
        except Exception as e:
            logger.exception(e)
            pass


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
            names_list: List[ChannelName] = await asyncio.gather(*tasks)
            for channel_name in names_list:
                tracking.add_name(channel_name)
                logger.info(
                    f"Channel {channel_name.channel_id} -> {channel_name.name}",
                    extra={"channel_name": channel_name.model_dump()},
                )

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
