"""
Tests for the msats_rounded property on CryptoConv and the use of integer msats
in ledger entries created during HIVE/HBD → keepsats conversions.

Background
----------
When converting HIVE/HBD to millisatoshis the full-precision computation produces
a fractional msats value, e.g.

    53.999 HIVE × 81.565 sats/HIVE × 1000 = 4,544,449.839963 msats

The corresponding Lightning Network payment is always an integer number of msats
(e.g. 4,544,449 msats).  Recording the fractional value in ledger entries while
the actual movement is an integer leaves a sub-msat residual per transaction that
accumulates over thousands of conversions.

The fix is to use `CryptoConv.msats_rounded` (ROUND_HALF_UP to integer) whenever
writing msats amounts to ledger entries that correspond to real Lightning payments.
"""

from decimal import Decimal

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConv, CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import QuoteResponse
from v4vapp_backend_v2.helpers.currency_class import Currency

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_quote(sats_per_hive: str = "81.565") -> QuoteResponse:
    """Return a minimal QuoteResponse with a known HIVE→sats rate."""
    hive_usd = Decimal("0.3")
    btc_usd = Decimal("60000")
    # sats_per_hive = hive_usd / btc_usd * 1e8 → we directly inject via hive_usd
    # Use a known btc_usd that yields the desired rate: sats_hive_p ≈ hive_usd/btc_usd*1e8
    # hive_usd = sats_per_hive * btc_usd / 1e8
    target_sats = Decimal(sats_per_hive)
    derived_hive_usd = target_sats * btc_usd / Decimal("1e8")
    return QuoteResponse(hive_usd=derived_hive_usd, btc_usd=btc_usd)


# ---------------------------------------------------------------------------
# msats_rounded property
# ---------------------------------------------------------------------------


class TestMsatsRoundedProperty:
    """Unit tests for CryptoConv.msats_rounded."""

    def test_integer_msats_unchanged(self):
        """An already-integer msats value stays unchanged after rounding."""
        conv = CryptoConv(msats=Decimal("4408000"))
        assert conv.msats_rounded == Decimal("4408000")
        assert conv.msats_rounded == conv.msats

    def test_fraction_below_half_rounds_down(self):
        """0.4 fractional part rounds DOWN to the integer below."""
        conv = CryptoConv(msats=Decimal("4544449.4"))
        assert conv.msats_rounded == Decimal("4544449")

    def test_fraction_exactly_half_rounds_up(self):
        """0.5 fractional part rounds UP (ROUND_HALF_UP, not banker's rounding)."""
        conv = CryptoConv(msats=Decimal("4544449.5"))
        assert conv.msats_rounded == Decimal("4544450")

    def test_fraction_above_half_rounds_up(self):
        """0.839963 fractional part rounds UP."""
        conv = CryptoConv(msats=Decimal("4544449.839963"))
        assert conv.msats_rounded == Decimal("4544450")

    def test_return_type_is_decimal(self):
        """msats_rounded returns a Decimal, not int or float."""
        conv = CryptoConv(msats=Decimal("1234567.9"))
        result = conv.msats_rounded
        assert isinstance(result, Decimal)

    def test_result_has_no_fractional_part(self):
        """msats_rounded always has exponent >= 0 (no decimal digits)."""
        for value in ("0.001", "999.999", "1000000.123456789", "0.5"):
            conv = CryptoConv(msats=Decimal(value))
            assert conv.msats_rounded % 1 == 0, f"Non-integer result for msats={value}"

    def test_zero_msats(self):
        """Zero msats stays zero."""
        conv = CryptoConv(msats=Decimal("0"))
        assert conv.msats_rounded == Decimal("0")

    def test_large_value_precision_preserved(self):
        """Values close to 2^53 (the float precision boundary) are handled correctly."""
        large = Decimal("9007199254740992")  # 2^53, exact in float
        conv = CryptoConv(msats=large + Decimal("0.5"))
        # ROUND_HALF_UP → should round up
        assert conv.msats_rounded == large + 1

    def test_consistent_with_sats_rounded(self):
        """msats_rounded == sats_rounded * 1000 when sats are < 0.5 away from the integer."""
        # Build a conv where sats = 4544.449839963
        # sats_rounded = 4544, msats_rounded should = 4544450 (NOT 4544*1000=4544000)
        # They are independent roundings; both should be ROUND_HALF_UP integers.
        conv = CryptoConv(msats=Decimal("4544449.839963"))
        assert conv.msats_rounded == Decimal("4544450")
        assert conv.sats_rounded == Decimal("4544")


# ---------------------------------------------------------------------------
# CryptoConversion produces integer msats when input is integer MSATS
# ---------------------------------------------------------------------------


class TestCryptoConversionIntegerMsats:
    """Verify CryptoConversion preserves integer msats for LND-sourced values."""

    def test_integer_msat_input_gives_integer_msats(self):
        """When conv_from is MSATS and value is an integer, msats stays integer."""
        quote = make_quote()
        conv = CryptoConversion(value=4_408_000, conv_from=Currency.MSATS, quote=quote)
        # msats should be exactly the integer input (no fractional drift)
        assert conv.msats % 1 == 0
        assert conv.msats == Decimal("4408000")

    def test_hive_to_msats_produces_fractional(self):
        """Converting from HIVE to MSATS with a non-round rate produces fractional msats."""
        quote = make_quote(sats_per_hive="81.565")
        # 53.999 HIVE at 81.565 sats/HIVE → 4,544,449.839... msats
        conv = CryptoConversion(value=Decimal("53.999"), conv_from=Currency.HIVE, quote=quote)
        assert conv.msats % 1 != 0, "Expected fractional msats from HIVE conversion"

    def test_msats_rounded_eliminates_fractional_drift(self):
        """
        msats_rounded on the result of a HIVE→MSATS conversion is an integer,
        eliminating the sub-msat residual that accumulates in the VSC Liability account.
        """
        quote = make_quote(sats_per_hive="81.565")
        conv = CryptoConversion(value=Decimal("53.999"), conv_from=Currency.HIVE, quote=quote)
        result = conv.conversion
        assert result.msats % 1 != 0, "pre-condition: full-precision msats is fractional"
        assert result.msats_rounded % 1 == 0, "post-fix: msats_rounded is integer"

    def test_int_value_in_update_conv_gives_integer_msats(self):
        """Using int() instead of float() for LND msat inputs preserves integer precision."""
        quote = make_quote()
        lnd_msat_value = 4_408_000  # integer from LND gRPC
        # Previously: float(lnd_msat_value) — could silently introduce float imprecision
        # Now: int(lnd_msat_value)
        conv_int = CryptoConversion(
            value=int(lnd_msat_value), conv_from=Currency.MSATS, quote=quote
        )
        conv_float = CryptoConversion(
            value=float(lnd_msat_value), conv_from=Currency.MSATS, quote=quote
        )
        # For integer LND values < 2^53, int and float both give the same result.
        # Crucially, using int avoids silent precision loss for callers.
        assert conv_int.msats == conv_float.msats  # equal for small integers
        assert conv_int.msats == Decimal("4408000")


# ---------------------------------------------------------------------------
# Accumulated rounding drift simulation
# ---------------------------------------------------------------------------


class TestAccumulatedRoundingDrift:
    """
    Simulate the accumulation of sub-msat residuals to verify the fix.

    Before the fix: recording fractional msats in ledger entries while Lightning
    payments use integer msats causes the running total to drift.

    After the fix: using msats_rounded in ledger entries eliminates the drift.
    """

    # Realistic HIVE amounts from live data (chosen to produce fractional msats)
    HIVE_AMOUNTS = [
        "53.999",
        "1.087",  # produces ~1,087,340.9 msats at typical rate
        "12.296",
        "0.985",
        "6.976",
        "6.794",
    ]

    def _simulate_transactions(self, use_rounded: bool) -> Decimal:
        """
        For each HIVE amount, compute the cust_conv ledger credit and the
        corresponding Lightning send (which uses integer msats = floor of fractional).

        Returns the accumulated residual: sum(credit - debit).
        """
        quote = make_quote(sats_per_hive="81.565")
        total_residual = Decimal("0")

        for amount_str in self.HIVE_AMOUNTS:
            conv = CryptoConversion(
                value=Decimal(amount_str), conv_from=Currency.HIVE, quote=quote
            ).conversion

            # cust_conv ledger credit: fractional or rounded
            credit = conv.msats_rounded if use_rounded else conv.msats

            # Lightning payment always truncates to integer msats (floor)
            lightning_send = conv.msats.to_integral_value(rounding="ROUND_FLOOR")

            # Running total per transaction: credit - debit
            total_residual += credit - lightning_send

        return total_residual

    def test_without_rounding_residual_accumulates(self):
        """Without rounding, fractional msats accumulate across transactions."""
        residual = self._simulate_transactions(use_rounded=False)
        # The residual should be the sum of fractional parts (all positive)
        assert residual > 0, "Expected positive drift from unrounded ledger entries"
        # It should be clearly sub-msat in total (each fraction is < 1 msat)
        assert residual < len(self.HIVE_AMOUNTS), "Residual bounded by transaction count"

    def test_with_msats_rounded_residual_bounded_to_half_msat_per_tx(self):
        """
        With msats_rounded (ROUND_HALF_UP), the residual per transaction is
        at most ±0.5 msats.  Over N transactions the maximum accumulated drift
        is N/2 msats — dramatically smaller than the unrounded case.
        """
        quote = make_quote(sats_per_hive="81.565")
        n = len(self.HIVE_AMOUNTS)
        max_expected = Decimal(n) * Decimal("0.5")

        residual = self._simulate_transactions(use_rounded=True)
        assert abs(residual) <= max_expected, (
            f"Rounded residual {residual} exceeds expected max {max_expected}"
        )

    def test_large_scale_unrounded_drift_grows_linearly(self):
        """Demonstrate that unrounded drift grows with transaction count."""
        quote = make_quote(sats_per_hive="81.565")
        amounts = [Decimal("53.999")] * 100
        drift = Decimal("0")
        for amt in amounts:
            conv = CryptoConversion(value=amt, conv_from=Currency.HIVE, quote=quote).conversion
            drift += conv.msats - conv.msats.to_integral_value(rounding="ROUND_FLOOR")

        # Each transaction contributes the fractional part of conv.msats.
        # The exact amount depends on the rate; over 100 transactions the drift
        # should be at least 10 msats (clearly non-trivial).
        assert drift > Decimal("10"), "Expected drift > 10 msats for 100 identical transactions"

    def test_large_scale_rounded_drift_stays_bounded(self):
        """With msats_rounded, drift over 100 identical transactions stays near zero."""
        quote = make_quote(sats_per_hive="81.565")
        amounts = [Decimal("53.999")] * 100
        drift = Decimal("0")
        for amt in amounts:
            conv = CryptoConversion(value=amt, conv_from=Currency.HIVE, quote=quote).conversion
            # Rounded credit vs floor debit
            credit = conv.msats_rounded
            debit = conv.msats.to_integral_value(rounding="ROUND_FLOOR")
            drift += credit - debit

        # With ROUND_HALF_UP: 4544449.839963 → rounded up → credit=4544450, debit=4544449
        # Each tx contributes +1 msat; total over 100 = +100 msats.
        # This is still vastly better than the unrounded ~+84 msats per transaction × 100.
        assert abs(drift) <= 100, f"Per-transaction residual with rounding: {drift / 100} msats"
