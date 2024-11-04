import codecs
import os

from v4vapp_backend_v2.config import logger
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDStartupError

LND_USE_LOCAL_NODE = "voltage"

# Set the environment variables for the local Charles proxy
# os.environ["http_proxy"] = "http://home-imac:8888"
# os.environ["https_proxy"] = "http://home-imac:8888"


class LNDConnectionSettings:
    address: str
    options: list
    macaroon: bytes
    cert: bytes

    def __init__(self) -> None:
        if LND_USE_LOCAL_NODE == "local":
            LND_MACAROON_PATH = os.path.expanduser(".certs/umbrel-admin.macaroon")
            LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls.cert")
            # LND_CONNECTION_ADDRESS = "100.97.242.92:10009"
            self.address = "10.0.0.5:10009"
            self.options = [
                (
                    "grpc.ssl_target_name_override",
                    "umbrel.local",
                ),
            ]
        else:
            LND_MACAROON_PATH = os.path.expanduser(".certs/readonly.macaroon")
            LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls-voltage.cert")
            self.address = "v4vapp.m.voltageapp.io:10009"
            self.options = [
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
            self.macaroon = codecs.encode(macaroon_bytes, "hex")
            self.cert = open(LND_CERTIFICATE_PATH, "rb").read()
        except FileNotFoundError as e:
            logger.error(f"Macaroon and cert files missing: {e}")
            raise LNDStartupError(f"Missing files: {e}")
        except Exception as e:
            logger.error(e)
            raise LNDStartupError(f"Error starting LND connection: {e}")
