import os
from decimal import ROUND_UP, Decimal

from binance.error import ClientError  # type: ignore
from binance.spot import Spot as Client  # type: ignore

from v4vapp_backend_v2.config.setup import InternalConfig, logger


class BinanceErrorLowBalance(Exception):
    pass


class BinanceErrorBadConnection(Exception):
    pass


def get_client(testnet: bool = False) -> Client:
    """
    Get a Binance API client
    """
    internal_config = InternalConfig()
    api_key = internal_config.config.api_keys.binance_api_key
    api_secret = internal_config.config.api_keys.binance_api_secret
    try:
        if testnet:
            client = Client(
                api_key=internal_config.config.api_keys.binance_testnet_api_key,
                api_secret=internal_config.config.api_keys.binance_testnet_api_secret,
                base_url="https://testnet.binance.vision",
            )
        else:
            client = Client(
                api_key=internal_config.config.api_keys.binance_api_key,
                api_secret=internal_config.config.api_keys.binance_api_secret,
            )
        return client
    except Exception as e:
        logger.error(e)
        return None


def get_balances(symbols: list, testnet: bool = False) -> dict:
    """
    Get balances for a list of symbols.
    This will work always on testnet. If the IP address is not whitelisted on Binance
    then it will fail and raise BinanceErrorBadConnection
    """
    try:
        client = get_client(testnet)
        account = client.account()
        balances = {symbol: 0.0 for symbol in symbols}  # Initialize all balances to 0.0
        for balance in account["balances"]:
            if balance["asset"] in symbols:
                balances[balance["asset"]] = float(balance["free"])
        if "BTC" in balances and balances["BTC"] > 0.0:
            balances["SATS"] = int(balances["BTC"] * 100_000_000)
        return balances
    except ClientError as error:
        logger.error(
            "Found error. status: {}, error code: {}, error message: {}".format(
                error.status_code, error.error_code, error.error_message
            )
        )
        raise BinanceErrorBadConnection(error.error_message)
    except Exception as error:
        logger.error(error)
        raise BinanceErrorBadConnection(error)
