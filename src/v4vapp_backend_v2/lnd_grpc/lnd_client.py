import asyncio
import os
import sys
from typing import Any, AsyncGenerator, Callable

import backoff
from google.protobuf.json_format import MessageToDict
from grpc import (
    composite_channel_credentials,  # type: ignore
    metadata_call_credentials,
    ssl_channel_credentials,
)
from grpc.aio import AioRpcError, secure_channel  # type: ignore

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.config.setup import logger
from v4vapp_backend_v2.lnd_grpc import invoices_pb2_grpc as invoicesstub
from v4vapp_backend_v2.lnd_grpc import lightning_pb2_grpc as lightningstub
from v4vapp_backend_v2.lnd_grpc import router_pb2_grpc as routerstub
from v4vapp_backend_v2.lnd_grpc.certificate_paths import get_ca_bundle_path
from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings
from v4vapp_backend_v2.lnd_grpc.lnd_errors import (
    LNDConnectionError,
    LNDFatalError,
    LNDStartupError,
    LNDSubscriptionError,
)

MAX_RETRIES = 20

ICON = "⚡"


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
        # self.get_info: lnrpc.GetInfoResponse | None = None
        self.setup()

    async def __aenter__(self):
        if os.getenv("TESTING") == "True" or getattr(self, "get_info", None) is not None:
            return self
        try:
            self.get_info = await self.node_get_info
        except LNDConnectionError as e:
            logger.warning(f"{ICON} Error getting node info {e}", exc_info=True)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.connection_check_task is not None:
            self.connection_check_task.cancel()
            try:
                await self.connection_check_task
            except asyncio.CancelledError:
                pass
        await self.disconnect()

    def metadata_callback(self, context, callback):
        # for more info see grpc docs
        callback([("macaroon", self.connection.macaroon)], None)

    def setup(self):
        try:
            logger.info(
                f"{ICON} Connecting to LND",
                extra={"connection": self.connection.name, "address": self.connection.address},
            )

            # Try to load system root certificates
            ca_bundle_path = get_ca_bundle_path()
            if ca_bundle_path:
                try:
                    with open(ca_bundle_path, "rb") as f:
                        root_certificates = f.read()
                    # Combine system root certificates with the server's certificate
                    combined_cert = root_certificates + b"\n" + self.connection.cert
                    cert_creds = ssl_channel_credentials(combined_cert)
                except (FileNotFoundError, PermissionError):
                    # Fallback to original method
                    cert_creds = ssl_channel_credentials(self.connection.cert)
            else:
                # Fallback to original method
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
            logger.error(f"{ICON} Macaroon and cert files missing: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"{ICON} {e}")
            raise LNDStartupError("Error starting LND connection")

    @property
    def icon(self) -> str:
        if self.connection.icon is None:
            return "-"
        return self.connection.icon

    @property
    async def node_get_info(self) -> lnrpc.GetInfoResponse:
        """
        Retrieve the information about the node.

        This function establishes a synchronous connection to an LND (Lightning Network
        Daemon) client using the provided connection name. It then calls the `GetInfo`
        method on the client's lightning stub to obtain information about the node.

        Returns:
            lnrpc.GetInfoResponse: The response from the `GetInfo` method.
        """
        try:
            if getattr(self, "get_info", None) is not None:
                return self.get_info
            self.get_info: lnrpc.GetInfoResponse = await self.lightning_stub.GetInfo(
                lnrpc.GetInfoRequest()
            )
            # always_print_fields_with_no_presence=True: forces serialization of fields that lack
            # presence (repeated, maps, scalars) so missing lists become [] instead of absent
            # (solves missing "invoices").
            get_info_dict = MessageToDict(
                self.get_info,
                preserving_proto_field_name=True,
                always_print_fields_with_no_presence=True,
            )
            logger.info(
                f"{ICON} {self.icon} Calling get_info {self.connection.name}",
                extra={"get_info": get_info_dict},
            )
            return self.get_info
        except Exception as e:
            logger.error(f"{ICON} Error getting node info {e}", exc_info=True)
            raise LNDConnectionError(f"Error getting node info {e}")

    async def disconnect(self):
        if self.channel is not None:
            await self.channel.close(grace=2)
            self.channel = None
            logger.info(f"{ICON} {self.icon} Disconnected from LND")

    async def check_connection(
        self,
        original_error: AioRpcError | None = None,
        call_name: str = "",
        max_tries: int = 200,
    ):
        error_count = 0
        back_off_time = 1
        if self.lightning_stub is None:
            self.setup()
        while True:
            self.setup()
            try:
                if self.lightning_stub is not None:
                    _ = await self.lightning_stub.WalletBalance(lnrpc.WalletBalanceRequest())
                    if original_error is not None:
                        logger.warning(
                            f"{ICON} {self.icon} Connection to LND is OK after Error "
                            f"cleared error_count: {error_count}",
                            extra={
                                "notification": True,
                                "error_code_clear": str(original_error.code()),
                                "error_count": error_count,
                                "original_error": original_error,
                            },
                        )
                    self.error_state = False
                    self.error_code = None
                    return
                else:
                    logger.warning(f"{ICON} {self.icon} LNDClient stub is None")
            except AioRpcError as e:
                if original_error is not None:
                    message = original_error.debug_error_string()
                else:
                    message = (
                        f"{ICON} {self.icon} Error in {call_name} RPC call: {get_error_code(e)}"
                    )
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
                message = (
                    f"{ICON} {self.icon} Too many errors in {call_name} RPC call ({error_count})"
                )
                logger.error(
                    message,
                    extra={"notification": True},
                )
                raise LNDConnectionError(message, error_count)
            back_off_time = min((2**error_count), 60)
            logger.warning(
                f"{ICON} Back off: {back_off_time}s Error {call_name}",
                extra={"notification": False},
            )
            await asyncio.sleep(back_off_time)

    # @backoff.on_exception(
    #     lambda: backoff.expo(base=2, factor=1),
    #     (LNDConnectionError),
    #     max_tries=2,
    #     logger=logger,
    # )
    async def call(self, method: Callable[..., Any], *args, **kwargs):
        try:
            # logger.debug(f"{ICON} Calling {method} with args: {str(args)[:50]}, kwargs: {str(kwargs)[:50]}")
            return await method(*args, **kwargs)
        except AioRpcError as e:
            if self.connection.use_proxy:
                message = f"{ICON} Local proxy not running {self.connection.use_proxy}"
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
                f"{ICON} {self.icon} Error in {method} RPC call: {e.code()}",
                extra={
                    "notification": True,
                    "error_code": get_error_code(e),
                    "error_details": error_to_dict(e),
                },
            )
            raise LNDConnectionError(f"{self.icon} Error in {method} RPC call", e)

    async def call_async_generator(
        self, method: Callable[..., AsyncGenerator[Any, None]], *args, **kwargs
    ) -> AsyncGenerator[Any, None]:
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
                logger.error(f"{ICON} broken connection in {call_name} RPC call: {e.code()}")
            raise LNDSubscriptionError(
                message=f"{self.icon} Error in {call_name} RPC call",
                rpc_error_code=e.code(),
                rpc_error_details=e.details(),
                call_name=call_name,
                original_error=e,
            )
        except Exception as e:
            logger.error(f"{ICON} Error in {call_name} RPC call: {e}")

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
            raise ValueError(f"{self.icon} Invalid method name: {method_name}")

        try:
            return await method(request)
        except AioRpcError as e:
            logger.error(f"{ICON} {self.icon} Error in {method_name} RPC call: {e.code()}")
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
            message = f"{ICON} {self.icon} Error in {method_name} RPC call: {e.code()}"
            logger.error(message)
            raise LNDConnectionError(f"{self.icon} Error in {method_name} RPC call")

        except Exception as e:
            message = f"{ICON} {self.icon} Error in {method_name} RPC call: {e}"
            logger.error(message)
            raise LNDConnectionError(f"Error in {method_name} RPC call")
            message = f"{ICON} {self.icon} Error in {method_name} RPC call: {e}"
            logger.error(message)
            raise LNDConnectionError(f"Error in {method_name} RPC call")
