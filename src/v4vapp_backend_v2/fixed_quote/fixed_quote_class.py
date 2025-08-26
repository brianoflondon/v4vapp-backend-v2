import json
from datetime import datetime, timezone
from pprint import pprint
from uuid import uuid4

from pydantic import BaseModel

from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.conversion.calculate import ConversionResult, calc_keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion, CryptoConvV1
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, HiveRatesDB, QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency


class FixedHiveQuote(BaseModel):
    """Holds a price quote in sats for fixed amount of Hive"""

    unique_id: str
    sats_send: int
    conv: CryptoConvV1
    timestamp: datetime = datetime.now(tz=timezone.utc)
    quote_record: HiveRatesDB
    quote_response: QuoteResponse
    conversion_result: ConversionResult

    @classmethod
    async def create_quote(
        cls,
        hive: float | None = None,
        hbd: float | None = None,
        usd: float | None = None,
        cache_time: int = 600,
        use_cache: bool = True,
        store_db: bool = True,
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
        quote_record = await all_quotes.db_store_quote(store_db=store_db)
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

        conv = CryptoConversion(quote=all_quotes.quote, conv_from=currency, value=value).conversion
        quote_response = QuoteResponse(
            hive_usd=all_quotes.quote.hive_usd,
            hbd_usd=all_quotes.quote.hbd_usd,
            btc_usd=all_quotes.quote.btc_usd,
            hive_hbd=all_quotes.quote.hive_hbd,
            raw_response={},
            source="FixedRate",
            fetch_date=all_quotes.quote.fetch_date,
            error="",  # No error in this case
            error_details={},
        )

        if currency != Currency.HIVE:
            amount = conv.amount_hbd
        else:
            amount = conv.amount_hive

        conversion_result = await calc_keepsats_to_hive(
            to_currency=currency,
            amount=amount,
            quote=all_quotes.quote,
        )

        # Create conversion
        sats_send = (conv.msats + conv.msats_fee) // 1000
        # Create quote instance
        quote = cls(
            unique_id=str(uuid4())[:6],
            sats_send=sats_send,
            conv=conv.v1(),
            quote_record=quote_record,
            quote_response=quote_response,
            conversion_result=conversion_result,
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
    def check_quote(cls, unique_id: str, send_sats: int) -> "FixedHiveQuote":
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
            try:
                fixed_hive_quote = FixedHiveQuote.model_validate(quote_data)
                return fixed_hive_quote.model_copy()
            except Exception as e:
                logger.error(f"Error validating fixed hive quote: {e}")
        raise ValueError("Invalid quote.")


if __name__ == "__main__":
    import asyncio

    async def main():
        quote = await FixedHiveQuote.create_quote(usd=10, cache_time=3600)
        pprint(quote.model_dump())
        fixed_hive_quote = FixedHiveQuote.check_quote(quote.unique_id, send_sats=quote.sats_send)
        print(f"Quote valid: {fixed_hive_quote is not None}")
        print(fixed_hive_quote.conversion_result)

    InternalConfig(config_filename="devhive.config.yaml")
    asyncio.run(main())
