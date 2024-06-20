import asyncio
import inspect
import sys
from typing import Any, AsyncGenerator, Callable

import backoff
import grpc
from grpc import (
    RpcError,
    StatusCode,
    composite_channel_credentials,
    metadata_call_credentials,
    ssl_channel_credentials,
)
from grpc.aio import AioRpcError, secure_channel

from v4vapp_backend_v2.config import logger
from v4vapp_backend_v2.lnd_grpc import lightning_pb2_grpc as lnrpc
from v4vapp_backend_v2.lnd_grpc.lnd_connection import (
    LNDConnectionError,
    LNDConnectionSettings,
    LNDConnectionStartupError,
)


class LNDClient:
    def __init__(self) -> None:
        self.connection = LNDConnectionSettings()
        self.channel = None
        self.stub = None
        self.error_state = False

    def metadata_callback(self, context, callback):
        # for more info see grpc docs
        callback([("macaroon", self.connection.macaroon)], None)

    async def connect(self):
        try:
            logger.info("Connecting to LND")

            cert_creds = ssl_channel_credentials(self.connection.cert)
            auth_creds = metadata_call_credentials(self.metadata_callback)
            combined_creds = composite_channel_credentials(cert_creds, auth_creds)

            self.channel = secure_channel(
                self.connection.address,
                combined_creds,
                options=self.connection.options,
            )

            self.stub = lnrpc.LightningStub(self.channel)

        except FileNotFoundError as e:
            logger.error(f"Macaroon and cert files missing: {e.code()}")
            sys.exit(1)
        except Exception as e:
            logger.error(e)
            raise LNDConnectionStartupError("Error starting LND connection")

    async def disconnect(self):
        if self.channel is not None:
            await self.channel.close()
            self.channel = None
            self.stub = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info("Disconnecting from LND")
        await self.disconnect()

    @backoff.on_exception(
        lambda: backoff.expo(base=2, factor=1),
        (LNDConnectionError),
        max_tries=20,
        logger=logger,
    )
    async def call(self, method: Callable[..., Any], *args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except AioRpcError as e:
            logger.warning(f"Error in {method} RPC call: {e.code()}")
            raise LNDConnectionError()

    async def call_async_generator(
        self, method: Callable[..., AsyncGenerator[Any, None]], *args, **kwargs
    ):
        backoff_counter = 0
        # check if call_name in kwargs
        if "call_name" in kwargs:
            call_name = kwargs.pop("call_name")
        else:
            call_name = __name__
        while True:
            try:
                async for response in method(*args, **kwargs):
                    logger.info(f"Received response from {call_name}")
                    yield response
                    backoff_counter = 0  # reset the counter after a successful call
            except AioRpcError as e:
                logger.warning(
                    f"Error in {method} RPC call: {e.code()}",
                    extra={
                        "telegram": True,
                        "call_name": call_name,
                        "error_code": e.code(),
                    },
                )
                backoff_counter += 1
                backoff_time = min(
                    2**backoff_counter, 60
                )  # cap the backoff time to 60 seconds
                logger.warning(
                    f"Retrying in {backoff_time} seconds",
                    extra={"telegram": False},
                )
                await asyncio.sleep(backoff_time)
            else:
                break

    @backoff.on_exception(
        lambda: backoff.expo(base=2, factor=1),
        (LNDConnectionError),
        max_tries=20,
        logger=logger,
    )
    async def call_retry(self, method_name, request):
        try:
            method = getattr(self.stub, method_name)
        except AttributeError:
            raise ValueError(f"Invalid method name: {method_name}")

        try:
            return await method(request)
        except AioRpcError as e:
            logger.error(f"Error in {method_name} RPC call: {e.code()}")
            raise LNDConnectionError(f"Error in {method_name} RPC call")

    @backoff.on_exception(
        lambda: backoff.expo(base=2, factor=1),
        (LNDConnectionError),
        max_tries=20,
        logger=logger,
    )
    async def call_async_generator_retry(self, method_name, request):
        try:
            method = getattr(self.stub, method_name)

            while True:
                async for response in method(request):
                    yield response
                break  # if the method call was successful, break the loop

        # except AttributeError:
        #     raise ValueError(f"Invalid method name: {method_name}")

        # except grpc.aio._call.AioRpcError as e:
        #     if e.code() == grpc.StatusCode.UNAVAILABLE:
        #         raise LNDConnectionError(f"Error in {method_name} RPC call") from e

        except Exception as e:
            logger.error(f"Error in {method_name} RPC call: {e}")
            raise LNDConnectionError(f"Error in {method_name} RPC call")

        except AioRpcError as e:
            logger.error(f"Error in {method_name} RPC call: {e.code()}")
            raise LNDConnectionError(f"Error in {method_name} RPC call")

            # if e.code() == grpc.StatusCode.UNAVAILABLE:
            #     logger.error(f"Connection lost, reconnecting...")
            #     self.reconnect()  # reconnect to the server
            # else:
            #     logger.error(f"Error in {method_name} RPC call: {e.code()}")
            #     raise LNDConnectionError(f"Error in {method_name} RPC call: {e.code()}")
