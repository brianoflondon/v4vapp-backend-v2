from __future__ import annotations

import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import httpx

from v4vapp_backend_v2.config.setup import DashConnectionConfig, InternalConfig
from v4vapp_backend_v2.dash.amounts import SATS_PER_BTC, dash_amount_string, duffs_from_sats

__all__ = [
    "duffs_from_sats",
    "fetch_quote",
    "quote_for_sats",
    "reset_quote_cache",
]
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.models.quote import Quote

CMC_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
DEFAULT_COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

_cache: Quote | None = None
_cache_until: float = 0.0


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_quote(source: str, btc_usd: Decimal, dash_usd: Decimal, dash_btc: Decimal) -> Quote:
    duffs = duffs_from_sats(1, dash_btc)
    return Quote(
        source=source,
        fetched_at=_now_iso(),
        btc_usd=btc_usd,
        dash_usd=dash_usd,
        dash_btc=dash_btc,
        sats_per_dash=SATS_PER_BTC * dash_btc,
        ttl_s=60,
        duffs_quoted=duffs,
        dash_quoted=dash_amount_string(duffs),
    )


def quote_for_sats(sats: Decimal, quote: Quote) -> Quote:
    duffs = duffs_from_sats(sats, quote.dash_btc)
    return quote.model_copy(
        update={"duffs_quoted": duffs, "dash_quoted": dash_amount_string(duffs)}
    )


def _dec(value: Any) -> Decimal:
    return Decimal(str(value))


def _from_coingecko(payload: dict[str, Any]) -> Quote:
    dash = payload["dash"]
    btc = payload["bitcoin"]
    dash_btc = _dec(dash["btc"])
    dash_usd = _dec(dash["usd"])
    btc_usd = _dec(btc["usd"])
    return _build_quote("coingecko", btc_usd, dash_usd, dash_btc)


def _from_cmc(payload: dict[str, Any]) -> Quote:
    data = payload["data"]
    btc_usd = _dec(data["BTC"]["quote"]["USD"]["price"])
    dash_usd = _dec(data["DASH"]["quote"]["USD"]["price"])
    dash_btc = dash_usd / btc_usd
    return _build_quote("coinmarketcap", btc_usd, dash_usd, dash_btc)


def _quote_sources() -> tuple[str, str]:
    coingecko = DEFAULT_COINGECKO_URL
    cmc_key = ""
    try:
        config = InternalConfig().config
        conn = config.dash_config.connection_config()
        if conn and conn.coingecko_url:
            coingecko = conn.coingecko_url
        cmc_key = config.api_keys.coinmarketcap or ""
    except Exception:
        return coingecko, cmc_key
    return coingecko, cmc_key


def fetch_quote(
    *,
    client: httpx.Client | None = None,
    force: bool = False,
    conn: DashConnectionConfig | None = None,
) -> Quote:
    global _cache, _cache_until
    now = time.monotonic()
    if not force and _cache is not None and now < _cache_until:
        return _cache

    coingecko_url, cmc_key = _quote_sources()
    if conn is not None and conn.coingecko_url:
        coingecko_url = conn.coingecko_url

    own_client = client is None
    http = client or httpx.Client(timeout=8.0)
    last_error: Exception | None = None
    try:
        try:
            resp = http.get(
                coingecko_url,
                params={"ids": "dash,bitcoin", "vs_currencies": "btc,usd"},
            )
            resp.raise_for_status()
            quote = _from_coingecko(resp.json())
            _cache, _cache_until = quote, now + quote.ttl_s
            return quote
        except Exception as exc:
            last_error = exc

        if cmc_key:
            try:
                resp = http.get(
                    CMC_URL,
                    params={"symbol": "DASH,BTC", "convert": "USD"},
                    headers={"X-CMC_PRO_API_KEY": cmc_key},
                )
                resp.raise_for_status()
                quote = _from_cmc(resp.json())
                _cache, _cache_until = quote, now + quote.ttl_s
                return quote
            except Exception as exc:
                last_error = exc
    finally:
        if own_client:
            http.close()

    raise ApiError(
        422,
        "quote_unavailable",
        f"All price sources failed: {last_error}",
    )


def reset_quote_cache() -> None:
    global _cache, _cache_until
    _cache = None
    _cache_until = 0.0
