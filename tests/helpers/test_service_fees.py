import pytest

from v4vapp_backend_v2.helpers.service_fees import (
    V4VMaximumInvoice,
    V4VMinimumInvoice,
    limit_test,
    msats_fee,
)
from v4vapp_backend_v2.hive.v4v_config import V4VConfig


def test_msats_fee_and_limit_test():
    config_data = V4VConfig().data
    min_invoice = config_data.minimum_invoice_payment_sats * 1_000
    max_invoice = config_data.maximum_invoice_payment_sats * 1_000

    test_msats_fee = msats_fee(10_000 * 1_000)
    assert test_msats_fee > 0, "Fee should be greater than zero for valid msats input"

    # Test minimum invoice
    with pytest.raises(V4VMinimumInvoice):
        limit_test(min_invoice - 1_000)

    # Test maximum invoice
    with pytest.raises(V4VMaximumInvoice):
        limit_test(max_invoice + 1_000)

    # Test valid invoice
    fee = msats_fee(min_invoice + 1000)
    assert fee > 0, "Fee should be greater than zero for valid invoice"


if __name__ == "__main__":
    pytest.main([__file__])
