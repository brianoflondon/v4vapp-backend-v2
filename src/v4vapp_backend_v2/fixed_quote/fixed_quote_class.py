import json
from datetime import datetime, timezone
from pprint import pprint
from uuid import uuid4

from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion, CryptoConvV1
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, Currency, HiveRatesDB, QuoteResponse


class FixedHiveQuote(BaseModel):
    """Holds a price quote in sats for fixed amount of Hive"""

    unique_id: str
    sats_send: int
    conv: CryptoConvV1
    timestamp: datetime = datetime.now(tz=timezone.utc)
    quote_record: HiveRatesDB | None = None

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
        quote_record = await all_quotes.db_store_quote()
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
            unique_id=str(uuid4())[:6],
            sats_send=int(conv.sats),
            conv=conv.v1(),
            quote_record=quote_record,
        )

        # Cache in Redis
        redis_client = InternalConfig.redis_decoded
        ok = redis_client.setex(
            f"fixed_quote:{quote.unique_id}",
            time=cache_time,
            value=quote.model_dump_json(exclude_none=True),
        )

        return quote

    @classmethod
    def check_quote(cls, unique_id: str, send_sats: int) -> QuoteResponse:
        """
        Checks if the quote is still valid (not expired).

        Returns:
            QuoteResponse: The response containing the quote details.

        Raises:
            ValueError: If the quote is invalid or expired.
        """
        redis_client = InternalConfig.redis_decoded
        quote_data_raw = redis_client.get(f"fixed_quote:{unique_id}")
        quote_data = json.loads(quote_data_raw) if quote_data_raw else None

        if quote_data and "quote_record" in quote_data:
            if quote_data.get("sats_send") != send_sats:
                raise ValueError("Sats amount does not match the quote.")

            quote = HiveRatesDB.model_validate(quote_data["quote_record"])
            if quote:
                quote_response = QuoteResponse(
                    hive_usd=quote.hive_usd,
                    hbd_usd=quote.hbd_usd,  # Assuming sats_hbd is used for hbd_us
                    btc_usd=quote.btc_usd,
                    hive_hbd=quote.hive_hbd,
                    raw_response={},
                    source="HiveRatesDB",
                    fetch_date=quote.timestamp,
                    error="",  # No error in this case
                    error_details={},
                )

                return quote_response
        raise ValueError("Invalid quote.")


if __name__ == "__main__":
    import asyncio

    async def main():
        quote = await FixedHiveQuote.create_quote(hive=0.100)
        pprint(quote.model_dump())
        is_valid = FixedHiveQuote.check_quote(quote.unique_id, send_sats=quote.sats_send)
        print(f"Quote valid: {is_valid}")
        await asyncio.sleep(10)

    InternalConfig(config_filename="devhive.config.yaml")
    asyncio.run(main())
