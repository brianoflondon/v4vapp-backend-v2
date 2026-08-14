"""Invoice min/max limits use Lightning principal, not Hive deposit conversion."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from v4vapp_backend_v2.helpers.service_fees import (
    V4VMaximumInvoice,
    V4VMinimumInvoice,
    limit_test,
)
from v4vapp_backend_v2.process.process_transfer import invoice_principal_msats


def test_invoice_principal_fixed_amount_uses_value_msat():
    pay_req = MagicMock()
    pay_req.is_zero_value = False
    pay_req.value_msat = 174_195_000  # 174,195 sats — omztech-style invoice
    max_send = Decimal(176_000_000)  # deposit net of fees, larger than principal
    assert invoice_principal_msats(pay_req, max_send) == Decimal(174_195_000)


def test_invoice_principal_zero_value_uses_max_send():
    pay_req = MagicMock()
    pay_req.is_zero_value = True
    pay_req.value_msat = 0
    max_send = Decimal(150_000_000)
    assert invoice_principal_msats(pay_req, max_send) == Decimal(150_000_000)


@patch("v4vapp_backend_v2.helpers.service_fees.V4VConfig")
def test_omztech_style_deposit_would_fail_but_principal_passes(mock_v4v_config):
    """
    Regression: 117.201 HBD ≈ 181,726 sats deposit (with fee headroom) exceeded a
    180k max when limits used deposit conv; invoice principal 174,195 sats is fine.
    """
    mock_v4v_config.return_value.data = SimpleNamespace(
        minimum_invoice_payment_sats=Decimal(1),
        maximum_invoice_payment_sats=Decimal(180_000),
    )
    deposit_msats = Decimal(181_726_000)
    principal_msats = Decimal(174_195_000)

    with pytest.raises(V4VMaximumInvoice):
        limit_test(deposit_msats)

    assert limit_test(principal_msats) is True


@patch("v4vapp_backend_v2.helpers.service_fees.V4VConfig")
def test_limit_test_still_rejects_oversized_principal(mock_v4v_config):
    mock_v4v_config.return_value.data = SimpleNamespace(
        minimum_invoice_payment_sats=Decimal(1_000),
        maximum_invoice_payment_sats=Decimal(180_000),
    )
    with pytest.raises(V4VMaximumInvoice):
        limit_test(Decimal(180_001_000))
    with pytest.raises(V4VMinimumInvoice):
        limit_test(Decimal(999_000))
    assert limit_test(Decimal(180_000_000)) is True
