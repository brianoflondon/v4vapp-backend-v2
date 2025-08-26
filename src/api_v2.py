import argparse
from typing import Any, Dict

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, Query, status
from fastapi.concurrency import asynccontextmanager

from v4vapp_backend_v2 import __version__
from v4vapp_backend_v2.accounting.account_balances import (
    get_keepsats_balance,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.api.v1_legacy.api_classes import (
    KeepsatsConvertExternal,
    KeepsatsTransferExternal,
    KeepsatsTransferResponse,
)
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.binance_extras import BinanceErrorBadConnection, get_balances
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion

from v4vapp_backend_v2.helpers.currency_class import Currency
from v4vapp_backend_v2.hive.v4v_config import V4VConfig
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json

ICON = "🤖"

# Global variable to store config filename
config_filename = "devhive.config.yaml"


def create_lifespan(config_file: str):
    """Factory function to create lifespan with the correct config filename"""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        InternalConfig(config_filename=config_file, log_filename="api_v2.jsonl")
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


@crypto_v2_router.get("/")
async def root():
    return {"message": "Hello World 2"}


@crypto_v2_router.post("/quotes/")
async def cryptoprices() -> AllQuotes:
    """Returns the prices of Hive/HBD and BTC/Sats vs USD"""
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes()
    assert all_quotes.quote
    return all_quotes


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
async def binance() -> Dict[str, Any]:
    try:
        balances = get_balances(symbols=["BTC", "HIVE", "USDT"], testnet=False)
        logger.info(f"{ICON} Binance balances: {balances}")
    except BinanceErrorBadConnection:
        return {"error": "Bad connection"}
    return {
        "BTC": balances.get("BTC", 0.0),
        "HIVE": balances.get("HIVE", 0.0),
        "USDT": balances.get("USDT", 0.0),
        "SATS": balances.get("SATS", 0),
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
    net_msats, account_balance = await keepsats_balance_printout(
        cust_id=hive_accname, line_items=line_items
    )
    return {
        "hive_accname": hive_accname,
        "net_msats": account_balance.msats,
        "net_hive": account_balance.conv_total.hive,
        "net_usd": account_balance.conv_total.usd,
        "net_hbd": account_balance.conv_total.hbd,
        "net_sats": account_balance.conv_total.sats,
        "in_progress_sats": 0,
        "all_transactions": [],
    }


@lightning_v1_router.post("/keepsats/transfer")
async def transfer_keepsats(transfer: KeepsatsTransferExternal) -> KeepsatsTransferResponse:
    """
    Transfers satoshis from one user to another.

    Args:
        transfer (LightningTransfer): The transfer details.

    Returns:
        LightningTransferResponse: The transfer response.
    """
    net_msats, account_balance = await get_keepsats_balance(
        cust_id=transfer.hive_accname_from, line_items=False
    )

    message = ""
    if transfer.sats <= 0:
        message = "Minimum is 0 sats"

    if transfer.sats and net_msats // 1000 < transfer.sats:
        message = "Insufficient funds"

    if message:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "message": message,
                "balance": net_msats // 1000,
                "requested": transfer.sats,
                "deficit": transfer.sats - (net_msats // 1000),
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
    net_msats, account_balance = await keepsats_balance_printout(
        cust_id=convert.hive_accname, line_items=False
    )
    if net_msats < convert.sats * 1_000:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "message": "Insufficient funds",
                "balance": net_msats // 1000,
                "sats": convert.sats,
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


def create_app(config_file: str = "devhive.config.yaml") -> FastAPI:
    """Create FastAPI app with the specified config file"""
    app = FastAPI(
        lifespan=create_lifespan(config_file),
        title="V4VApp Lightning to Hive API",
        description="The API to generate a Lightning Invoice and start a payment to Hive.",
        version=__version__,
        redirect_slashes=False,
    )

    app.include_router(crypto_v2_router, tags=["crypto"])
    app.include_router(crypto_v1_router, tags=["legacy"])
    app.include_router(lightning_v1_router, tags=["lightning"])

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V4VApp API v2 Server")
    parser.add_argument(
        "--config",
        type=str,
        default="devhive.config.yaml",
        help="Configuration filename (default: devhive.config.yaml)",
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)"
    )
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to (default: 8000)")
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of worker processes (default: 1)"
    )

    args = parser.parse_args()

    # Create the app with the specified config file
    app = create_app(config_file=args.config)

    uvicorn.run(app, host=args.host, port=args.port, workers=args.workers)
else:
    # Create app with default config for module imports
    app = create_app()
