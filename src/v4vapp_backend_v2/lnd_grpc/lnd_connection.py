import codecs
import os
from pathlib import Path

from v4vapp_backend_v2.config import InternalConfig, logger
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDStartupError

LND_USE_LOCAL_NODE = "local"

# Set the environment variables for the local Charles proxy
# os.environ["http_proxy"] = "http://home-imac.tail400e5.ts.net:8888"
# os.environ["https_proxy"] = "http://home-imac:8888"


"""
Notes: tls cert for Umbrel /home/umbrel/umbrel/app-data/lightning/data/lnd

"""


class LNDConnectionSettings:
    name: str
    address: str
    options: list
    macaroon: bytes
    cert: bytes
    

    def __init__(self) -> None:
        lnd_config = InternalConfig().config.lnd_connection

        if lnd_config.use_proxy:
            os.environ["http_proxy"] = lnd_config.use_proxy
            logger.info(f"Using proxy: {lnd_config.use_proxy}")

        self.address = lnd_config.address
        options_dict = lnd_config.options
        options_tuples = [
            (key, value) for d in options_dict for key, value in d.items()
        ]
        self.options = options_tuples
        LND_MACAROON_PATH = Path(
            lnd_config.certs_path, lnd_config.macaroon_filename
        ).expanduser()
        LND_CERTIFICATE_PATH = Path(
            lnd_config.certs_path, lnd_config.cert_filename
        ).expanduser()

        # if LND_USE_LOCAL_NODE == "local":
        #     LND_MACAROON_PATH = os.path.expanduser(".certs/umbrel-admin.macaroon")
        #     LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls.cert")
        #     # LND_CONNECTION_ADDRESS = "100.97.242.92:10009"
        #     self.address = "10.0.0.5:10009"
        #     self.options = [
        #         (
        #             "grpc.ssl_target_name_override",
        #             "umbrel.local",
        #         ),
        #     ]
        # else:
        #     LND_MACAROON_PATH = os.path.expanduser(".certs/readonly.macaroon")
        #     LND_CERTIFICATE_PATH = os.path.expanduser(".certs/tls-voltage.cert")
        #     self.address = "v4vapp.m.voltageapp.io:10009"
        #     self.options = [
        #         (
        #             "grpc.ssl_target_name_override",
        #             "v4vapp.m.voltageapp.io",
        #         ),
        #     ]

        # Create a channel to the server
        # Due to updated ECDSA generated tls.cert we need to let grpc know that
        # we need to use that cipher suite otherwise there will be a handshake
        # error when we communicate with the lnd rpc server.
        os.environ["GRPC_SSL_CIPHER_SUITES"] = "HIGH+ECDSA"

        try:
            with open(LND_MACAROON_PATH, "rb") as f:
                macaroon_bytes = f.read()
            self.macaroon = codecs.encode(macaroon_bytes, "hex")
            with open(LND_CERTIFICATE_PATH, "rb") as f:
                self.cert = f.read()
            logger.info(
                f"Setting up for connection to LND: {lnd_config.name} {self.address}"
            )
        except FileNotFoundError as e:
            logger.error(f"Macaroon and cert files missing: {e}")
            raise LNDStartupError(f"Missing files: {e}")
        except Exception as e:
            logger.error(e)
            raise LNDStartupError(f"Error starting LND connection: {e}")
