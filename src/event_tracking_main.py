import asyncio
import json

from pydantic import ValidationError
from google.protobuf.json_format import MessageToDict

import inspect
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
import v4vapp_backend_v2.lnd_grpc.router_pb2 as routerrpc

from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDFatalError, LNDSubscriptionError
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.models.htlc_event_models import HtlcEvent, HtlcTrackingList

config = InternalConfig().config
from v4vapp_backend_v2.events.async_event import async_publish
from v4vapp_backend_v2.events.event_models import Events


async def subscribe_to_htlc_events_from_lnd(connection_name: str):
    while True:
        async with LNDClient(connection_name=connection_name) as client:
            request_sub = routerrpc.SubscribeHtlcEventsRequest()
            try:
                async for htlc in client.call_async_generator(
                    client.router_stub.SubscribeHtlcEvents,
                    request_sub,
                    call_name="SubscribeHtlcEvents",
                ):
                    htlc_data = MessageToDict(htlc, preserving_proto_field_name=True)
                    if not htlc_data == {"subscribed_event": {}}:
                        try:
                            htlc_event = HtlcEvent.model_validate(htlc_data)
                        except ValidationError as e:
                            logger.warning(
                                "htlc_event_data object\n"
                                + json.dumps(htlc_data, indent=2),
                                extra={"htlc_data": htlc_data},
                            )
                            logger.error(e)
                            continue
                        print(htlc_event)
                        async_publish(Events.HTLC_EVENT, htlc_event)

            except LNDSubscriptionError as e:
                await client.check_connection(
                    original_error=e.original_error, call_name="SubscribeHtlcEvents"
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error in {inspect.currentframe().f_code.co_name}"
                )
                logger.error(e)
                raise e


async def main() -> None:
    try:
        logger.info("Starting Tasks")
        tasks = [subscribe_to_htlc_events_from_lnd("umbrel")]
        await asyncio.gather(*tasks)

    except KeyboardInterrupt:
        logger.warning("❌ LND Event Tracking stopped keyboard")
    except LNDFatalError as e:
        logger.error("❌ LND Event Tracking stopped fatal error")
        raise e
    except Exception as e:
        logger.error("❌ LND Event Tracking stopped error")
        logger.error(e)
        raise e

    logger.info("❌ LND Event Tracking stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
        logger.info("✅ LND Event Tracking stopped")

    except KeyboardInterrupt:
        logger.warning(
            "✅ LND Event Tracking stopped by keyboard", extra={"notification": False}
        )

    except LNDFatalError as e:
        logger.error(
            "❌ LND Event Tracking stopped by fatal error",
            extra={"notification": False},
        )
        logger.error(e, extra={"notification": False})
        raise e
    except Exception as e:
        logger.error(
            "❌ LND Event Tracking stopped by error", extra={"notification": False}
        )
        logger.error(e, extra={"notification": False})
        raise e
