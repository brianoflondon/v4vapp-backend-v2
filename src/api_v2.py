from typing import Any, Dict

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, Query, status
from fastapi.concurrency import asynccontextmanager

from v4vapp_backend_v2.accounting.account_balances import (
    get_keepsats_balance,
    keepsats_balance_printout,
)
from v4vapp_backend_v2.api.v1_legacy.api_classes import (
    KeepsatsTransferExternal,
    KeepsatsTransferResponse,
)
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.process.hive_notification import send_transfer_custom_json

ICON = "🤖"


@asynccontextmanager
async def lifespan(app: FastAPI):
    InternalConfig(config_filename="devhive.config.yaml", log_filename="api_v2.jsonl")
    db_conn = DBConn()
    await db_conn.setup_database()
    logger.info("API v2 started", extra={"notification": False})
    yield


app = FastAPI(lifespan=lifespan, redirect_slashes=False)
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
    return {"BTC": 0.0, "HIVE": 100.0, "USDT": 0, "SATS": 9034258}


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

    if transfer.sats and net_msats // 1000 < transfer.sats:
        raise HTTPException(
            status_code=status.HTTP_402_PAYMENT_REQUIRED,
            detail={
                "message": "Insufficient funds",
                "balance": net_msats // 1000,
                "sats": transfer.sats,
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


app.include_router(crypto_v2_router, tags=["crypto"])
app.include_router(crypto_v1_router, tags=["legacy"])
app.include_router(lightning_v1_router, tags=["lightning"])


if __name__ == "__main__":
    uvicorn.run("api_v2:app", host="0.0.0.0", port=8000, workers=1)
