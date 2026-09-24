from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from v4vapp_backend_v2.dash.amounts import dash_amount_string, duffs_from_sats
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.models.quote import Quote
from v4vapp_backend_v2.helpers.crypto_prices import AllQuotes, QuoteResponse

__all__ = [
    "duffs_from_sats",
    "fetch_quote",
    "quote_for_sats",
]


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def quote_from_response(qr: QuoteResponse) -> Quote:
    dash_btc = qr.dash_btc_p
    if qr.dash_usd <= 0 or qr.btc_usd <= 0 or dash_btc <= 0:
        raise ApiError(422, "quote_unavailable", "DASH price unavailable")
    duffs = duffs_from_sats(1, dash_btc)
    fetch_at = qr.fetch_date
    if fetch_at.tzinfo is None:
        fetch_at = fetch_at.replace(tzinfo=UTC)
    fetched_at = fetch_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return Quote(
        source=qr.source or "AllQuotes",
        fetched_at=fetched_at or _now_iso(),
        btc_usd=qr.btc_usd,
        dash_usd=qr.dash_usd,
        dash_btc=dash_btc,
        sats_per_dash=qr.sats_per_dash_p,
        ttl_s=60,
        duffs_quoted=duffs,
        dash_quoted=dash_amount_string(duffs),
    )


def quote_for_sats(sats: Decimal, quote: Quote) -> Quote:
    duffs = duffs_from_sats(sats, quote.dash_btc)
    return quote.model_copy(
        update={"duffs_quoted": duffs, "dash_quoted": dash_amount_string(duffs)}
    )


async def fetch_quote() -> Quote:
    """DASH/BTC quote from AllQuotes (Binance, CoinGecko, CMC). Uses Redis cache."""
    all_quotes = AllQuotes()
    await all_quotes.get_all_quotes(use_cache=True, store_db=False)
    qr = all_quotes.quote
    if qr.error and qr.dash_usd <= 0:
        raise ApiError(422, "quote_unavailable", qr.error)
    return quote_from_response(qr)
