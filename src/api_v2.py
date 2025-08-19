from typing import Any, Dict

import uvicorn
from fastapi import APIRouter, FastAPI, Query  # Add Query import
from fastapi.concurrency import asynccontextmanager

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.fixed_quote.fixed_quote_class import FixedHiveQuote
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency

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
    """Returns the fixed quote for Hive/HBD and BTC/Sats vs USD"""
    return await FixedHiveQuote.create_quote(
        hive=HIVE, hbd=HBD, usd=USD, cache_time=cache_time, use_cache=use_cache
    )


app.include_router(crypto_v2_router, tags=["crypto"])
app.include_router(crypto_v1_router, tags=["legacy"])


if __name__ == "__main__":
    uvicorn.run("api_v2:app", host="0.0.0.0", port=8000, workers=1)
