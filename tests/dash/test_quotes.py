from datetime import UTC, datetime
from decimal import Decimal

from v4vapp_backend_v2.dash.amounts import msats_to_sats, rpc_dash_to_duffs
from v4vapp_backend_v2.dash.errors import ApiError
from v4vapp_backend_v2.dash.quotes.service import quote_for_sats, quote_from_response
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse


def test_worked_example_25000_sats() -> None:
    from v4vapp_backend_v2.dash.amounts import duffs_from_sats

    duffs = duffs_from_sats(25_000, Decimal("0.0005"))
    assert duffs == 50_000_000


def test_ceil_rounding() -> None:
    from v4vapp_backend_v2.dash.amounts import duffs_from_sats

    assert duffs_from_sats(1, Decimal("0.0005")) == 2000
    assert duffs_from_sats(3, Decimal("0.0007")) == 4286


def test_rpc_dash_float_to_duffs() -> None:
    assert rpc_dash_to_duffs("0.50000000") == Decimal(50_000_000)
    assert rpc_dash_to_duffs(0.5) == Decimal(50_000_000)


def test_msats_to_sats_rounds_up() -> None:
    assert msats_to_sats("25000500") == Decimal(25001)
    assert msats_to_sats(1000) == Decimal(1)
    assert msats_to_sats(1001) == Decimal(2)


def test_quote_from_allquotes_response() -> None:
    qr = QuoteResponse(
        btc_usd=Decimal(65000),
        dash_usd=Decimal("32.5"),
        source="Binance",
        fetch_date=datetime(2026, 8, 13, 12, 0, tzinfo=UTC),
    )
    quote = quote_from_response(qr)
    assert quote.source == "Binance"
    assert quote.dash_btc == qr.dash_btc_p
    priced = quote_for_sats(Decimal(25_000), quote)
    # 25000 sats at dash_btc = 32.5/65000 = 0.0005 → 0.5 DASH = 50_000_000 duffs
    assert priced.duffs_quoted == Decimal(50_000_000)
    assert priced.dash_quoted == "0.50000000"


def test_quote_from_response_unavailable() -> None:
    qr = QuoteResponse(btc_usd=Decimal(65000), dash_usd=Decimal(0), source="Binance")
    try:
        quote_from_response(qr)
    except ApiError as exc:
        assert exc.code == "quote_unavailable"
        assert exc.status_code == 422
    else:
        raise AssertionError("expected quote_unavailable")
