import argparse
import socket
import sys
from decimal import Decimal
from typing import Any, Dict

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, status
from fastapi.concurrency import asynccontextmanager

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.accounting.account_balances import keepsats_balance
from v4vapp_backend_v2.api.v1_legacy.api_classes import (
    KeepsatsConvertExternal,
    KeepsatsInvoice,
    KeepsatsTransferExternal,
    KeepsatsTransferResponse,
)
from v4vapp_backend_v2.config.setup import InternalConfig, StartupFailure, logger
from v4vapp_backend_v2.conversion.exchange_protocol import (
    ExchangeConnectionError,
    get_exchange_adapter,
)
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes
from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json

ICON = "🤖"


def create_lifespan(config_filename: str):
    """Factory function to create lifespan with the correct config filename"""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        v4v_config = V4VConfig(server_accname=InternalConfig().server_id)
        if not v4v_config.fetch():
            logger.warning("Failed to fetch V4V config")
            await v4v_config.put()
        db_conn = DBConn()
        await db_conn.setup_database()
        logger.info("API v2 started", extra={"notification": False})
        yield

    return lifespan


# This will be replaced when we parse command line arguments
app = None

crypto_v2_router = APIRouter(prefix="/v2/crypto")
crypto_v1_router = APIRouter(prefix="/cryptoprices")
lightning_v1_router = APIRouter(prefix="/lightning")
notifications_router = APIRouter(prefix="/send_notification")


@notifications_router.get("/")
async def send_notification(
    notify: str = Query(..., description="Notification message to send"),
    alert_level: int = Query(1, description="Alert level of the notification"),
):
    """
    Send a notification via the API v2 logging endpoint.

    Parameters
    ----------
    notify : str
        Notification message to send.
    alert_level : int, optional
        Alert level of the notification. Mapped as:
          - 1 -> 'info' (default)
          - 2 -> 'warning'
          - 3 -> 'error'
        Any other value defaults to 'info'.

    Returns
    -------
    dict
        A simple acknowledgement dictionary: {"message": "Notification sent"}.

    Side effects
    ------------
    Logs the notification using the appropriate logger method (logger.info, logger.warning, logger.error)
    with extra={"notification": True}. The logged message is formatted as:
        "<notify> (Alert Level: <alert_str> | From API v2)".

    Notes
    -----
    This is an async helper intended for use by API v2 request handlers. It does not raise on
    unknown alert_level values; they are treated as 'info'.
    """
    # map alter level 1 to 'info', 2 to 'warning', 3 to 'error'
    alert_map = {1: "info", 2: "warning", 3: "error"}
    alert_str = alert_map.get(alert_level, "info")
    # call logger with alert level
    notify_text = f"{notify} (Alert Level: {alert_str} | From API v2)"
    if alert_str == "info":
        logger.info(notify_text, extra={"notification": True})
    elif alert_str == "warning":
        logger.warning(notify_text, extra={"notification": True})
    elif alert_str == "error":
        logger.error(notify_text, extra={"notification": True})
    return {"message": "Notification sent"}


@crypto_v2_router.post("/quotes/")
async def cryptoprices() -> AllQuotes:
    """Asynchronously fetch and return cryptocurrency prices.

    This coroutine constructs an AllQuotes object, invokes its asynchronous
    get_all_quotes() method to populate price data, and returns the populated
    AllQuotes instance. The quotes include Hive/HBD and Bitcoin (USD and Satoshis)
    price information.

    The function asserts that the returned AllQuotes instance contains a truthy
    quote attribute; an AssertionError is raised if no quotes were retrieved.
    Any exceptions raised by AllQuotes.get_all_quotes() are propagated.

    Returns:
        AllQuotes: An AllQuotes instance with populated quote data.

    Raises:
        AssertionError: If no quote data was retrieved.
    """
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes()
    assert all_quotes.quote
    return all_quotes


# @crypto_v2_router.post("")
# MARK: Legacy v1 Cryptoprices calls


@crypto_v1_router.get("/")
async def get_prices(use_cache: bool = Query(True, description="Use cached quotes if available")):
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(store_db=False, use_cache=use_cache)
    return all_quotes.legacy_api_format()


@crypto_v1_router.post("/")
async def conversion(
    sats: int | None = Query(
        None, description="The amount of sats"
    ),  # Replace Field with Query for query parameters in a POST endpoint
    use_cache: bool = Query(True, description="Use cached quotes if available"),
) -> Dict[str, Any]:
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(store_db=False, use_cache=use_cache)
    answer = all_quotes.legacy_api_format()
    if sats:
        conv = CryptoConversion(
            conv_from=Currency.SATS, value=sats, quote=all_quotes.quote
        ).conversion
        answer["conversion"] = conv.model_dump()
        answer["conversion"]["HBD"] = conv.hbd
        answer["conversion"]["HIVE"] = conv.hive
        answer["conversion"]["USD"] = conv.usd
    return answer


@crypto_v1_router.get("/fixed_quote/")
async def fixed_quote(
    HIVE: float | None = Query(None, description="The amount of Hive to convert to sats"),
    HBD: float | None = Query(None, description="The amount of HBD to convert to sats"),
    USD: float | None = Query(None, description="The amount of USD to convert to sats"),
    cache_time: int = Query(600, description="Cache time in seconds"),
    use_cache: bool = Query(True, description="Use cached quotes if available"),
) -> FixedHiveQuote:
    """
    Returns a fixed quote for converting Hive, HBD, and USD amounts to Bitcoin Satoshis (sats).

    Args:
        HIVE (float | None): The amount of Hive to convert to sats.
        HBD (float | None): The amount of HBD to convert to sats.
        USD (float | None): The amount of USD to convert to sats.
        cache_time (int): Cache time in seconds. Default is 600.
        use_cache (bool): Whether to use cached quotes if available. Default is True.

    Returns:
        FixedHiveQuote: An object containing the fixed quote for Hive/HBD and BTC/Sats vs USD.
    """
    return await FixedHiveQuote.create_quote(
        hive=HIVE, hbd=HBD, usd=USD, cache_time=cache_time, use_cache=use_cache
    )


@crypto_v1_router.get("/binance/")
async def binance() -> Dict[str, str | int | float]:
    try:
        adapter = get_exchange_adapter()
        balances = adapter.get_balances(["BTC", "HIVE", "USDT"])
        logger.debug(f"{ICON} Binance balances: {balances}")
    except ExchangeConnectionError:
        return {"error": "Bad connection"}
    return {
        "BTC": float(balances.get("BTC", 0.0)),
        "HIVE": float(balances.get("HIVE", 0.0)),
        "USDT": float(balances.get("USDT", 0.0)),
        "SATS": int(balances.get("SATS", 0)),
    }


# MARK: /lightning


@lightning_v1_router.get("/keepsats")
async def keepsats(
    hive_accname: str = Query(..., description="Hive account name to check for keepsats"),
    age: int = Query(0, description="Age in hours to check for keepsats"),
    transactions: bool = Query(False, description="Whether to include transaction history"),
    admin: bool = Query(False, description="Whether the user is an admin"),
) -> Dict[str, Any]:
    """
    Retrieves the keepsats balance and related information for a specified Hive account.
    Args:
        hive_accname (str): Hive account name to check for keepsats.
        age (int): Age in hours to check for keepsats. Defaults to 0.
        transactions (bool): Whether to include transaction history. Defaults to False.
    Returns:
        Dict[str, Any]: A dictionary containing the Hive account name, net balances in various currencies,
        in-progress sats, and transaction history.
    Raises:
        Any exceptions raised by get_keepsats_balance.
    """
    line_items = transactions
    net_msats, account_balance = await keepsats_balance(
        cust_id=hive_accname, line_items=line_items
    )

    if line_items:
        if age > 0:
            account_balance = account_balance.remove_older_than(hours=age)
        else:
            account_balance = account_balance.remove_balances()

    return account_balance.to_api_response(hive_accname=hive_accname, line_items=line_items)


@lightning_v1_router.post("/keepsats/transfer")
async def transfer_keepsats(transfer: KeepsatsTransferExternal) -> KeepsatsTransferResponse:
    """
    Transfers satoshis from one user to another.

    Args:
        transfer (LightningTransfer): The transfer details.

    Returns:
        LightningTransferResponse: The transfer response.
    """
    net_msats, account_balance = await keepsats_balance(
        cust_id=transfer.hive_accname_from, line_items=False
    )

    message = ""
    if transfer.sats <= 0:
        message = "Minimum is 0 sats"

    if transfer.sats and net_msats // Decimal(1000) < transfer.sats:
        message = "Insufficient funds"

    if message:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "message": message,
                "balance": net_msats // Decimal(1000),
                "requested": transfer.sats,
                "deficit": transfer.sats - (net_msats // Decimal(1000)),
            },
        )

    transfer_internal = KeepsatsTransfer(
        hive_accname_from=transfer.hive_accname_from,
        hive_accname_to=transfer.hive_accname_to,
        sats=transfer.sats,
        memo=transfer.memo,
    )

    trx = await send_transfer_custom_json(transfer_internal)
    if trx is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transfer failed",
        )
    trx_id = trx.get("trx_id", "unknown")
    return KeepsatsTransferResponse(
        success=True,
        message="Transfer successful",
        trx_id=trx_id,
    )


@lightning_v1_router.post("/keepsats/convert")
async def convert_keepsats(convert: KeepsatsConvertExternal) -> KeepsatsTransferResponse:
    """
    Converts a specified amount of sats from a user's Keepsats account to the internal server account.

    Validates the minimum convert amount and checks if the user has sufficient funds before initiating the transfer.
    Raises HTTP exceptions for invalid amounts, insufficient funds, or transfer failures.

    Args:
        convert (KeepsatsConvertExternal): The conversion request containing the user's account name, amount in sats, and memo.

    Returns:
        KeepsatsTransferResponse: Response object indicating success, message, and transaction ID.

    Raises:
        HTTPException: If the convert amount is below the minimum, funds are insufficient, or the transfer fails.
    """
    config_data = V4VConfig().data
    if convert.sats < config_data.minimum_invoice_payment_sats:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "message": (
                    f"Minimum convert amount is {config_data.minimum_invoice_payment_sats:,.0f} sats"
                )
            },
        )
    net_msats, account_balance = await keepsats_balance(
        cust_id=convert.hive_accname, line_items=False
    )
    # Add one sat
    if net_msats + 1_000 < convert.sats * 1_000:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "message": "Insufficient funds",
                "balance": net_msats // Decimal(1000),
                "requested": convert.sats,
                "deficit": convert.sats - (net_msats // Decimal(1000)),
            },
        )
    if convert.memo:
        if convert.symbol == "HBD" and "#HBD" not in convert.memo:
            convert.memo += " | #HBD"
    else:  # Convert memo is empty
        if convert.symbol == "HBD":
            convert.memo = f"Converting {convert.sats:,.0f} sats to #HBD"
        else:
            convert.memo = f"Converting {convert.sats:,.0f} sats to #HIVE"

    transfer_internal = KeepsatsTransfer(
        hive_accname_from=convert.hive_accname,
        hive_accname_to=InternalConfig().server_id,
        sats=convert.sats,
        memo=convert.memo,
    )
    trx = await send_transfer_custom_json(transfer_internal)
    if trx is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transfer failed",
        )
    trx_id = trx.get("trx_id", "unknown")
    return KeepsatsTransferResponse(
        success=True,
        message="Transfer successful",
        trx_id=trx_id,
    )


@lightning_v1_router.post("/keepsats/invoice")
async def pay_invoice(
    invoice: KeepsatsInvoice,
) -> KeepsatsTransferResponse:
    """
    Process a payment invoice by creating a transfer transaction on the Hive blockchain.

    Args:
        invoice (KeepsatsInvoice): The invoice containing transfer details including
            source account, destination account, amount in satoshis, and memo.

    Returns:
        KeepsatsTransferResponse: Response object containing transfer status, success message,
            and transaction ID.

    Raises:
        HTTPException: Raised with 500 status code if the transfer transaction fails.
    """
    transfer_internal = KeepsatsTransfer(
        hive_accname_from=invoice.hive_accname_from,
        hive_accname_to=InternalConfig().server_id,
        sats=invoice.sats,
        memo=invoice.memo,
    )
    trx = await send_transfer_custom_json(transfer_internal)
    if trx is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Transfer failed",
        )
    trx_id = trx.get("trx_id", "unknown")
    return KeepsatsTransferResponse(
        success=True,
        message="Transfer successful",
        trx_id=trx_id,
    )


def create_app(config_file: str = "devhive.config.yaml") -> FastAPI:
    """Create FastAPI app with the specified config file"""
    app = FastAPI(
        lifespan=create_lifespan(config_file),
        title="V4VApp Lightning to Hive API",
        description="The API to generate a Lightning Invoice and start a payment to Hive.",
        version=__version__,
        redirect_slashes=False,
    )

    # Add proxy middleware to trust headers from reverse proxy
    # This allows FastAPI to correctly detect HTTPS when behind nginx proxy
    @app.middleware("http")
    async def proxy_middleware(request: Request, call_next):
        # Trust common proxy headers
        if "x-forwarded-proto" in request.headers:
            request.scope["scheme"] = request.headers["x-forwarded-proto"]
        if "x-forwarded-host" in request.headers:
            request.scope["server"] = (request.headers["x-forwarded-host"], None)

        response = await call_next(request)
        return response

    # Add root endpoint here
    @app.get("/")
    @app.get("/health")
    @app.get("/status")
    async def root():
        return {
            "message": "Welcome to V4VApp API v2",
            "version": __version__,
            "status": "OK",
            "server_id": InternalConfig().server_id,
            "dns_name": socket.getfqdn(),
            "local_machine_name": InternalConfig().local_machine_name,
            "documentation": "/docs",
        }

    app.include_router(crypto_v2_router, tags=["crypto"])
    app.include_router(crypto_v1_router, tags=["legacy"])
    app.include_router(lightning_v1_router, tags=["lightning"])
    app.include_router(notifications_router, tags=["notifications"])

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V4VApp API v2 Server")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Configuration filename (default: config.yaml)",
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes (default: 1)"
    )

    args = parser.parse_args()

    try:
        InternalConfig(config_filename=args.config)
    except StartupFailure as e:
        # Do not try to send notifications about a failure to load config since
        # notification infra may itself be unstable during startup (e.g., Redis down)
        logger.error(f"Failed to load config: {e}", extra={"notification": False})
        sys.exit(1)

    # Create the app with the specified config file
    app = create_app(config_file=args.config)
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        workers=args.workers,
        log_config=None,
        log_level="warning",
        access_log=False,
    )
else:
    # Create app with default config for module imports
    app = create_app()
