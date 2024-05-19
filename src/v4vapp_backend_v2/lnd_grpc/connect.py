import codecs
import os

import grpc

from v4vapp_backend_v2.lnd_grpc import lightning_pb2_grpc as lnrpc

LND_USE_LOCAL_NODE = "local"

if LND_USE_LOCAL_NODE == "local":
    LND_MACAROON_PATH = os.path.expanduser(".certs/umbrel-admin.macaroon")
    LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls.cert")
    LND_CONNECTION_ADDRESS = "10.0.0.5:10009"
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


def lnd_stub() -> lnrpc.LightningStub:
    """
    Creates and returns a gRPC stub for interacting with the LND (Lightning Network Daemon) server.

    Returns:
        lnrpc.LightningStub: The gRPC stub for interacting with the LND server.
    """

    def metadata_callback(context, callback):
        # for more info see grpc docs
        callback([("macaroon", macaroon)], None)

    with open(LND_MACAROON_PATH, "rb") as f:
        macaroon_bytes = f.read()
    macaroon = codecs.encode(macaroon_bytes, "hex")
    cert = open(LND_CERTIFICATE_PATH, "rb").read()

    # build ssl credentials using the cert the same as before
    cert_creds = grpc.ssl_channel_credentials(cert)

    # now build meta data credentials
    auth_creds = grpc.metadata_call_credentials(metadata_callback)

    # combine the cert credentials and the macaroon auth credentials
    # such that every call is properly encrypted and authenticated
    combined_creds = grpc.composite_channel_credentials(cert_creds, auth_creds)

    # now every call will be made with the macaroon already included
    # channel = grpc.secure_channel("umbrel.local:10009", creds)
    channel = grpc.secure_channel(
        LND_CONNECTION_ADDRESS,
        combined_creds,
        options=LND_CONNECTION_OPTIONS,
    )

    return lnrpc.LightningStub(channel)
