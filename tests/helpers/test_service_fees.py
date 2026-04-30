from decimal import Decimal

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.helpers.service_fees import (
    V4VMaximumInvoice,
    V4VMinimumInvoice,
    calculate_fee_estimate_msats,
    calculate_fee_msats,
    limit_test,
)
from v4vapp_backend_v2.hive.v4v_config import V4VConfig


def test_msats_fee_and_limit_test():
    config_data = V4VConfig().data
    min_invoice = config_data.minimum_invoice_payment_sats * 1_000
    max_invoice = config_data.maximum_invoice_payment_sats * 1_000

    test_msats_fee = calculate_fee_msats(10_000 * 1_000)
    assert test_msats_fee > 0, "Fee should be greater than zero for valid msats input"

    # Test minimum invoice
    with pytest.raises(V4VMinimumInvoice):
        limit_test(min_invoice - 1_000)

    # Test maximum invoice
    with pytest.raises(V4VMaximumInvoice):
        limit_test(max_invoice + 1_000)

    # Test valid invoice
    fee = calculate_fee_msats(min_invoice + 1000)
    assert fee > 0, "Fee should be greater than zero for valid invoice"


if __name__ == "__main__":
    pytest.main([__file__])


def test_calculate_fee_estimate_msats():
    """Test calculate_fee_estimate_msats uses base_msats + ppm fraction of amount."""
    lnd_config = InternalConfig().config.lnd_config
    base = lnd_config.lightning_fee_base_msats  # default 50_000
    ppm = lnd_config.lightning_fee_estimate_ppm  # default 1_000

    # Zero input: result should just be the base fee
    assert calculate_fee_estimate_msats(Decimal(0)) == Decimal(base)

    # Known value: 10,000 sats = 10_000_000 msats
    msats = Decimal(10_000_000)
    expected = Decimal(base) + msats * Decimal(ppm) / 1_000_000
    assert calculate_fee_estimate_msats(msats) == expected.quantize(Decimal("1"))

    # Larger amount: fee scales with ppm
    msats_large = Decimal(1_000_000_000)  # 1,000,000 sats
    result_large = calculate_fee_estimate_msats(msats_large)
    result_small = calculate_fee_estimate_msats(msats)
    assert result_large > result_small
