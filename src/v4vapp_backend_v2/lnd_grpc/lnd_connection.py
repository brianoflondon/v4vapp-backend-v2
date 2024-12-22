import codecs
import os
from pathlib import Path

from v4vapp_backend_v2.config.setup import InternalConfig, logger
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
    use_proxy: str = ""

    def __init__(self) -> None:
        lnd_config = InternalConfig().config.lnd_connection

        if lnd_config.use_proxy:
            os.environ["http_proxy"] = lnd_config.use_proxy
            self.use_proxy = lnd_config.use_proxy
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

        os.environ["GRPC_SSL_CIPHER_SUITES"] = "HIGH+ECDSA"

        try:
            with open(LND_MACAROON_PATH, "rb") as f:
                macaroon_bytes = f.read()
            self.macaroon = codecs.encode(macaroon_bytes, "hex")
            with open(LND_CERTIFICATE_PATH, "rb") as f:
                self.cert = f.read()
            logger.debug(
                f"Setting up for connection to LND: {lnd_config.name} {self.address}"
            )
        except FileNotFoundError as e:
            logger.error(f"Macaroon and cert files missing: {e}")
            raise LNDStartupError(f"Missing files: {e}")
        except Exception as e:
            logger.error(e)
            raise LNDStartupError(f"Error starting LND connection: {e}")
