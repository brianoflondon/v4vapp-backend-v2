from datetime import datetime, timezone
from typing import Any, Dict
from uuid import UUID, uuid4

import uvicorn
from fastapi import APIRouter, FastAPI, Query  # Add Query import
from fastapi.concurrency import asynccontextmanager
from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion, CryptoConvV1
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency


class FixedHiveQuote(BaseModel):
    """Holds a price quote in sats for fixed amount of Hive"""

    unique_id: UUID
    sats_send: int
    conv: CryptoConvV1
    timestamp: datetime = datetime.now(tz=timezone.utc)

    @classmethod
    async def create_quote(
        cls,
        hive: float | None = None,
        hbd: float | None = None,
        usd: float | None = None,
        cache_time: int = 600,
        use_cache: bool = True,
    ) -> "FixedHiveQuote":
        """
        Asynchronously creates a new fixed quote based on the provided currency and amount.

        Args:
            hive (float | None): Amount in HIVE currency. If provided, used as the quote currency.
            hbd (float | None): Amount in HBD currency. Used if `hive` is None and `hbd` is not None.
            usd (float | None): Amount in USD currency. Used if both `hive` and `hbd` are None and `usd` is not None.
            cache_time (int): Time in seconds to cache the quote in Redis. Defaults to 600.
            use_cache (bool): Whether to use cached quotes when fetching all quotes. Defaults to True.

        Returns:
            FixedHiveQuote: The newly created fixed quote instance.

        Raises:
            None explicitly, but may raise exceptions from underlying quote fetching or Redis operations.

        Side Effects:
            Stores the created quote in Redis with a key based on its unique ID.
        """
        all_quotes = AllQuotes()
        await all_quotes.get_all_quotes(store_db=False, use_cache=use_cache)

        # Determine currency and value
        currency = (
            Currency.HIVE
            if hive is not None
            else Currency.HBD
            if hbd is not None
            else Currency.USD
        )
        if hive is not None:
            value = hive
        elif hbd is not None:
            value = hbd
        elif usd is not None:
            value = usd
        else:
            value = 0.0

        # Create conversion
        conv = CryptoConversion(quote=all_quotes.quote, conv_from=currency, value=value).conversion

        # Create quote instance
        quote = cls(
            unique_id=uuid4(),
            sats_send=int(conv.sats),
            conv=conv.v1(),
        )

        # Cache in Redis
        redis_client = InternalConfig.redis
        redis_client.setex(
            f"fixed_quote:{quote.unique_id}", time=cache_time, value=quote.model_dump_json()
        )

        return quote
