import pytest

from v4vapp_backend_v2.helpers.crypto_prices import CoinGeckoQuoteService


@pytest.mark.asyncio
async def test_coin_gecko_quote_service():
    service = CoinGeckoQuoteService()
    quote = await service.get_quote()
    assert quote is not None
    assert quote.sats_hive > 0
    assert quote.sats_hbd > 0
    assert quote.hive_usd > 0
    assert quote.hbd_usd > 0
    assert quote.btc_usd > 0
    assert quote.hive_hbd > 0
    assert quote.fetch_date is not None
    assert quote.raw_response is not None
    assert quote.quote_age < 10
