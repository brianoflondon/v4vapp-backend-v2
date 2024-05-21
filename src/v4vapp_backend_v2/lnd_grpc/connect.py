import codecs
import os
import sys
from typing import Any, AsyncGenerator, Dict, Tuple

from google.protobuf.json_format import MessageToDict
from grpc import (
    composite_channel_credentials,
    metadata_call_credentials,
    ssl_channel_credentials,
)
from grpc.aio import AioRpcError, secure_channel
from pydantic import ValidationError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.config import logger
from v4vapp_backend_v2.lnd_grpc import lightning_pb2_grpc as lnrpc
from v4vapp_backend_v2.models.lnd_models import LNDInvoice


class LNDConnectionStartupError(Exception):
    pass


class LNDConnectionError(Exception):
    pass


LND_USE_LOCAL_NODE = "voltage"


if LND_USE_LOCAL_NODE == "local":
    LND_MACAROON_PATH = os.path.expanduser(".certs/umbrel-admin.macaroon")
    LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls.cert")
    LND_CONNECTION_ADDRESS = "100.97.242.92:10009"
    LND_CONNECTION_OPTIONS = [
        (
            "grpc.ssl_target_name_override",
            "umbrel.local",
        ),
    ]
else:
    LND_MACAROON_PATH = os.path.expanduser(".certs/readonly.macaroon")
    LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls-voltage.cert")
    LND_CONNECTION_ADDRESS = "v4vapp.m.voltageapp.io:10009"
    LND_CONNECTION_OPTIONS = [
        (
            "grpc.ssl_target_name_override",
            "v4vapp.m.voltageapp.io",
        ),
    ]

# Create a channel to the server
# Due to updated ECDSA generated tls.cert we need to let grpc know that
# we need to use that cipher suite otherwise there will be a handshake
# error when we communicate with the lnd rpc server.
os.environ["GRPC_SSL_CIPHER_SUITES"] = "HIGH+ECDSA"


# Open the macaroon file and read the macaroon and certs at this point

try:
    with open(LND_MACAROON_PATH, "rb") as f:
        macaroon_bytes = f.read()
    MACAROON_FROM_FILE = codecs.encode(macaroon_bytes, "hex")
    CERT_FROM_FILE = open(LND_CERTIFICATE_PATH, "rb").read()
except FileNotFoundError as e:
    logger.error(f"Macaroon and cert files missing: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(e)
    raise LNDConnectionStartupError(f"Error starting LND connection: {e}")


async def connect_to_lnd() -> lnrpc.LightningStub:
    """
    Returns:
        lnrpc.LightningStub: The gRPC stub for interacting with the LND server.
    """
    logger.info("Connecting to LND")

    def metadata_callback(context, callback):
        # for more info see grpc docs
        callback([("macaroon", MACAROON_FROM_FILE)], None)

    # build ssl credentials using the cert the same as before
    cert_creds = ssl_channel_credentials(CERT_FROM_FILE)

    # now build meta data credentials
    auth_creds = metadata_call_credentials(metadata_callback)

    # combine the cert credentials and the macaroon auth credentials
    # such that every call is properly encrypted and authenticated
    combined_creds = composite_channel_credentials(cert_creds, auth_creds)

    # now every call will be made with the macaroon already included
    # channel = grpc.secure_channel("umbrel.local:10009", creds)
    channel = secure_channel(
        LND_CONNECTION_ADDRESS,
        combined_creds,
        options=LND_CONNECTION_OPTIONS,
    )

    return lnrpc.LightningStub(channel)


async def wallet_balance() -> ln.WalletBalanceResponse:
    """
    Returns:
        ln.WalletBalanceResponse: The response from the LND server.
    """
    stub = await connect_to_lnd()
    try:
        response = await stub.WalletBalance(ln.WalletBalanceRequest())
        return response
    except AioRpcError as e:
        logger.warning(f"Error connecting to LND: {e}")
        raise LNDConnectionError(f"Error connecting to LND: {e}")

    except Exception as e:
        raise e


# async def list_invoices(reversed: boolindex_offset: int = 0, num_max_invoices: int = 1):
#     stub = await connect_to_lnd()
#     response_inv = await stub.ListInvoices(
#         ln.ListInvoiceRequest(
#             pending_only=False,
#             reversed=True,
#             index_offset=index_offset,
#             num_max_invoices=num_max_invoices,
#         )
#     )
#     for inv in response_inv.invoices:
#         inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
#         try:
#             invoice = LNDInvoice.model_validate(inv_dict)
#             print(f"✅ Valid invoice {invoice.add_index}")
#         except ValidationError as e:
#             print(e)
#             print(f"❌ Invalid invoice {inv.add_index}")


async def most_recent_invoice(
    stub: lnrpc.LightningStub | None = None,
) -> Tuple[LNDInvoice, LNDInvoice]:
    """
    Returns:
        LNDInvoice: The most recent invoice from the LND server.
    """
    if not stub:
        stub = await connect_to_lnd()
    response_inv = await stub.ListInvoices(
        ln.ListInvoiceRequest(
            pending_only=False,
            reversed=True,
            index_offset=0,
            num_max_invoices=100,
        )
    )
    highest_invoice: LNDInvoice = LNDInvoice.model_construct()
    highest_settled_invoice = LNDInvoice.model_construct()
    for inv in response_inv.invoices:
        inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
        invoice = LNDInvoice.model_validate(inv_dict)
        if invoice.add_index > highest_invoice.add_index:
            highest_invoice = invoice
        if invoice.settle_index > highest_settled_invoice.settle_index:
            highest_settled_invoice = invoice
    return highest_invoice, highest_settled_invoice


async def subscribe_invoices(
    count_back: int = 0, add_index: int = 0, settle_index: int = 0
) -> AsyncGenerator[LNDInvoice, None]:
    """
    Subscribe to invoices from the Lightning Network Daemon (LND).

    Args:
        count_back (int): The number of invoices to go back from the most recent
        invoice. Defaults to 0.

    Yields:
        LNDInvoice: A validated LNDInvoice object representing an invoice.

    Raises:
        AioRpcError: If there is an error with the RPC communication.
        Exception: If there is an unexpected error.

    Returns:
        AsyncGenerator[LNDInvoice, None]: An asynchronous generator that yields
        LNDInvoice objects.
    """

    # find the most recent highest add_index
    stub = await connect_to_lnd()
    if add_index == 0:
        most_recent, most_recent_settled = await most_recent_invoice(stub)
        add_index = most_recent.add_index
        settle_index = most_recent_settled.settle_index
        request_sub = ln.InvoiceSubscription(
            add_index=add_index - count_back, settle_index=settle_index
        )
    else:
        request_sub = ln.InvoiceSubscription(
            add_index=add_index, settle_index=settle_index
        )
    logger.info(
        f"Subscribing to invoices from add_index {add_index} settle_index {settle_index}"
    )
    try:
        async for inv in stub.SubscribeInvoices(request_sub):
            inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
            try:
                invoice = LNDInvoice.model_validate(inv_dict)
                yield invoice
            except ValidationError as e:
                logger.error(e)
                logger.warning(f"❌ Invalid invoice {inv.add_index}")
    except AioRpcError as e:
        raise e

    except Exception as e:
        raise e


# stub = await connect_to_lnd()
# response = await stub.WalletBalance(ln.WalletBalanceRequest())
# print(response)

# response_inv = await stub.ListInvoices(
#     ln.ListInvoiceRequest(
#         pending_only=False, reversed=True, index_offset=0, num_max_invoices=10
#     )
# )
# for inv in response_inv.invoices:
#     inv_dict = MessageToDict(inv, preserving_proto_field_name=True)
#     try:
#         invoice = LNDInvoice.model_validate(inv_dict)
#         print(f"✅ Valid invoice {invoice.add_index}")
#     except ValidationError as e:
#         print(e)
#         print(f"❌ Invalid invoice {inv.add_index}")

# response_payment = await stub.ListPayments(
#     ln.ListPaymentsRequest(reversed=True, index_offset=0, max_payments=1)
# )

# for pay in response_payment.payments:
#     print(pay)
#     print(MessageToDict(pay, preserving_proto_field_name=True))
#     print()
