from decimal import Decimal

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency


def test_hive_rounding_half_up():
    """Ensure hive values are rounded using ROUND_HALF_UP to 10 decimals."""
    # Construct a quote where sats_hive_p == 1 so hive == msats / 1000
    # Set quote so sats_hive_p computes to 1.000000 (quantized to 6 dp)
    quote = QuoteResponse(hive_usd=Decimal("1"), btc_usd=Decimal("100000000"))
    # msats chosen so that hive = 1.00000000005 which should round to 1.0000000001
    # msats = hive * sats_hive_p * 1000 -> 1.00000000005 * 1.000000 * 1000
    msats = Decimal("1000.000000050000")

    conv = CryptoConversion(conv_from=Currency.MSATS, value=msats, quote=quote)

    # Expected hive after rounding half up to 10 decimals
    expected = Decimal("1.0000000001")
    assert conv.hive == expected
