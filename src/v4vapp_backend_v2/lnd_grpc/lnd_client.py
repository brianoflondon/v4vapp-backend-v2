import asyncio
import sys
from typing import Any, AsyncGenerator, Callable

import backoff
from grpc import composite_channel_credentials  # type: ignore
from grpc import metadata_call_credentials  # type: ignore
from grpc import ssl_channel_credentials  # type: ignore
from grpc.aio import AioRpcError, secure_channel  # type: ignore

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.lnd_grpc import router_pb2_grpc as routerstub
from v4vapp_backend_v2.lnd_grpc import lightning_pb2_grpc as lightningstub
from v4vapp_backend_v2.lnd_grpc import invoices_pb2_grpc as invoicesstub

from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings
from v4vapp_backend_v2.lnd_grpc.lnd_errors import (
    LNDConnectionError,
    LNDFatalError,
    LNDStartupError,
    LNDSubscriptionError,
)

MAX_RETRIES = 20


def get_error_code(e: AioRpcError) -> str:
    try:
        return str(e.code())
    except AttributeError:
        return "Unknown"


def error_to_dict(e: Exception) -> dict:
    return {
        "type": type(e).__name__,
        "message": str(e),
        "args": e.args,
    }


class LNDClient:
    def __init__(self, connection_name: str) -> None:
        self.connection = LNDConnectionSettings(connection_name)
        self.channel = None
        self.lightning_stub: lightningstub.LightningStub = None
        self.router_stub: routerstub.RouterStub = None
        self.invoices_stub: invoicesstub.InvoicesStub = None
        self.error_state: bool = False
        self.error_code: str | None = None
        self.connection_check_task: asyncio.Task[Any] | None = None

    def metadata_callback(self, context, callback):
        # for more info see grpc docs
        callback([("macaroon", self.connection.macaroon)], None)

    async def connect(self):
        try:
            logger.debug("Connecting to LND")

            cert_creds = ssl_channel_credentials(self.connection.cert)
            auth_creds = metadata_call_credentials(self.metadata_callback)
            combined_creds = composite_channel_credentials(cert_creds, auth_creds)

            self.channel = secure_channel(
                self.connection.address,
                combined_creds,
                options=self.connection.options,
            )

            self.lightning_stub = lightningstub.LightningStub(self.channel)
            self.router_stub = routerstub.RouterStub(self.channel)
            self.invoices_stub = invoicesstub.InvoicesStub(self.channel)

        except FileNotFoundError as e:
            logger.error(f"Macaroon and cert files missing: {get_error_code(e)}")
            sys.exit(1)
        except Exception as e:
            logger.error(e)
            raise LNDStartupError("Error starting LND connection")

    async def disconnect(self):
        if self.channel is not None:
            await self.channel.close()
            self.channel = None
            self.lightning_stub = None

    async def check_connection(
        self,
        original_error: AioRpcError | None = None,
        call_name: str = "",
        max_tries: int = 200,
    ):
        error_count = 0
        back_off_time = 1
        if self.lightning_stub is None:
            await self.connect()
        while True:
            try:
                if self.lightning_stub is not None:
                    _ = await self.lightning_stub.WalletBalance(
                        lnrpc.WalletBalanceRequest()
                    )
                    logger.warning(
                        f"Connection to LND is OK Error "
                        f"cleared error_count: {error_count}",
                        extra={
                            "notification": True,
                            "error_code": str(original_error.code()),
                            "error_code_clear": True,
                            "error_count": error_count,
                            "original_error": original_error,
                        },
                    )
                    self.error_state = False
                    self.error_code = None
                    return
                else:
                    logger.warning("LNDClient stub is None")
            except AioRpcError as e:
                if original_error is not None:
                    e = original_error
                    message = e.debug_error_string()
                else:
                    message = f"Error in {call_name} RPC call: {get_error_code(e)}"
                    original_error = e
                logger.error(
                    message,
                    extra={
                        "notification": True,
                        "error_code": get_error_code(e),
                        "error_details": error_to_dict(e),
                    },
                )
                self.error_state = True
            error_count += 1
            if error_count >= max_tries:
                message = f"Too many errors in {call_name} RPC call ({error_count})"
                logger.error(
                    message,
                    extra={"notification": True},
                )
                raise LNDConnectionError(message, error_count)
            back_off_time = min((2**error_count), 60)
            logger.warning(
                f"Back off: {back_off_time}s Error {call_name}",
                extra={"notification": False},
            )
            await asyncio.sleep(back_off_time)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Disconnecting from LND")
        if self.connection_check_task is not None:
            self.connection_check_task.cancel()
            try:
                await self.connection_check_task
            except asyncio.CancelledError:
                pass
        await self.disconnect()

    # @backoff.on_exception(
    #     lambda: backoff.expo(base=2, factor=1),
    #     (LNDConnectionError),
    #     max_tries=2,
    #     logger=logger,
    # )
    async def call(self, method: Callable[..., Any], *args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except AioRpcError as e:
            if self.connection.use_proxy:
                message = f"Local proxy not running {self.connection.use_proxy}"
                logger.error(
                    message,
                    extra={
                        "notification": False,
                        "error_code": get_error_code(e),
                        "error_details": error_to_dict(e),
                    },
                )
                raise LNDFatalError(message)
            logger.warning(
                f"Error in {method} RPC call: {e.code()}",
                extra={
                    "notification": True,
                    "error_code": get_error_code(e),
                    "error_details": error_to_dict(e),
                },
            )
            raise LNDConnectionError(e)

    async def call_async_generator(
        self, method: Callable[..., AsyncGenerator[Any, None]], *args, **kwargs
    ):
        """
        Calls the specified asynchronous generator method and yields the responses.
        If the name of the `call_name` is passed as a keyword argument, it will be
        used in the error message. Otherwise, the name of the method will be used.

        Args:
            method (Callable[..., AsyncGenerator[Any, None]]): The asynchronous
                generator method to call.
            *args: Variable length argument list to be passed to the method.
            **kwargs: Arbitrary keyword arguments to be passed to the method.

        Yields:
            Any: The responses yielded by the asynchronous generator method.

        Raises:
            LNDSubscriptionError: If an error occurs during the RPC call.
        """
        if "call_name" in kwargs:
            call_name = kwargs.pop("call_name")
        else:
            call_name = __name__

        try:
            async for response in method(*args, **kwargs):
                yield response
        except AioRpcError as e:
            if self.error_state:
                logger.error(f"broken connection in {call_name} RPC call: {e.code()}")
            raise LNDSubscriptionError(
                message=f"Error in {call_name} RPC call",
                rpc_error_code=e.code(),
                rpc_error_details=e.details(),
                call_name=call_name,
                original_error=e,
            )
        except Exception as e:
            logger.error(f"Error in {call_name} RPC call: {e}")

    @backoff.on_exception(
        lambda: backoff.expo(base=2, factor=1),
        (LNDConnectionError),
        max_tries=MAX_RETRIES,
        logger=logger,
    )
    async def call_retry(self, method_name, request):
        try:
            method = getattr(self.lightning_stub, method_name)
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
        max_tries=MAX_RETRIES,
        logger=logger,
    )
    async def call_async_generator_retry(self, method_name, request):
        try:
            method = getattr(self.lightning_stub, method_name)

            while True:
                async for response in method(request):
                    yield response
                break  # if the method call was successful, break the loop

        # except AttributeError:
        #     raise ValueError(f"Invalid method name: {method_name}")

        # except grpc.aio._call.AioRpcError as e:
        #     if e.code() == grpc.StatusCode.UNAVAILABLE:
        #         raise LNDConnectionError(f"Error in {method_name} RPC call") from e
        except AioRpcError as e:
            logger.error(f"Error in {method_name} RPC call: {e.code()}")
            raise LNDConnectionError(f"Error in {method_name} RPC call")

        except Exception as e:
            logger.error(f"Error in {method_name} RPC call: {e}")
            raise LNDConnectionError(f"Error in {method_name} RPC call")
