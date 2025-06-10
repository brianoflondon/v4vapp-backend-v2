from datetime import datetime, timezone

from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse


def last_quote() -> QuoteResponse:
    """
    Loads a mock last quote for testing purposes.

    Returns:
        QuoteResponse: A mock last quote object.
    """
    fetch_date = datetime.now(timezone.utc)
    all_quotes = AllQuotes(
        quotes={
            "CoinGecko": QuoteResponse(
                hive_usd=0.2711,
                hbd_usd=1.015,
                btc_usd=110706,
                hive_hbd=0.2671,
                raw_response={},
                source="CoinGecko",
                fetch_date=fetch_date,
                error="",
                error_details={},
                sats_hive=244.8828,
                sats_hbd=916.8428,
                age=271.796425,
            ),
            "Binance": QuoteResponse(
                hive_usd=0.2707,
                hbd_usd=1,
                btc_usd=110776.11,
                hive_hbd=0.2707,
                raw_response={},
                source="Binance",
                fetch_date=fetch_date,
                error="",
                error_details={},
                sats_hive=244.3668,
                sats_hbd=902.7217,
                age=211.338774,
            ),
            "HiveInternalMarket": QuoteResponse(
                hive_usd=0,
                hbd_usd=0,
                btc_usd=0,
                hive_hbd=0.2707,
                raw_response={},
                source="HiveInternalMarket",
                fetch_date=fetch_date,
                error="",
                error_details={},
                sats_hive=0.0,
                sats_hbd=0.0,
                age=210.988639,
            ),
        },
        fetch_date=fetch_date,
        source="Binance",
    )
    return all_quotes.get_binance_quote()
