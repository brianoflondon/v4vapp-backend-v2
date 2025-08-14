from cmath import isclose
from datetime import datetime, timezone
from pathlib import Path
from pprint import pprint

import pytest
from nectar.amount import Amount

from tests.get_last_quote import last_quote
from tests.utils import fake_trx_id, latest_block_num
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.conversion.calculate import hive_to_keepsats, keepsats_to_hive
from v4vapp_backend_v2.helpers.crypto_prices import Currency
from v4vapp_backend_v2.hive_models.op_transfer import Transfer


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


async def test_keepsats_to_hive_convert_from_msats():
    TrackedBaseModel.last_quote = last_quote()
    for sats in [500, 4_000, 50_234, 201_000]:
        for currency in [Currency.HIVE, Currency.HBD]:
            msats = sats * 1_000
            conversion_result = await keepsats_to_hive(
                msats=msats, quote=last_quote(), to_currency=currency
            )
            print(conversion_result)
            print(conversion_result.log_str)
            assert isclose(conversion_result.balance, msats, abs_tol=0.001), (
                "Conversion result is not as expected"
            )
            assert conversion_result.to_currency == currency, (
                f"Expected to_currency {currency}, got {conversion_result.to_currency}"
            )


async def test_keepsats_to_hive_convert_from_hive():
    TrackedBaseModel.last_quote = last_quote()
    for amount_value in [1.0, 3.0, 55.5, 102.0]:
        for currency in [Currency.HIVE, Currency.HBD]:
            amount = Amount(f"{amount_value} {currency.upper()}")

            conversion_result = await keepsats_to_hive(
                amount=amount,
                quote=last_quote(),
            )
            print(conversion_result)
            assert isclose(
                conversion_result.net_to_receive_conv.value_in(currency),
                amount.amount,
                abs_tol=0.001,
            ), "Conversion result is not as expected"
            assert conversion_result.to_currency == currency, (
                f"Expected to_currency {currency}, got {conversion_result.to_currency}"
            )


async def test_hive_to_keepsats():
    """
    Test the `hive_to_keepsats` conversion function.

    This test verifies the following scenarios:
    - Conversion from a HIVE amount to keepsats using a valid quote.
    - Ensures the conversion result is not None and the net amount to receive is positive.
    - Checks that the resulting balance after conversion is as expected (approximately 12.000).
    - Performs a second conversion with a slightly reduced amount (msats - 2,123) and validates:
        - The conversion result is not None and net amount to receive is positive.
        - The resulting balance remains as expected.
        - The converted msats amount matches the requested value within a small tolerance.

    Dependencies:
    - Mocks or fixtures for `last_quote`, `Amount`, `Transfer`, `fake_trx_id`, `latest_block_num`, and `hive_to_keepsats`.
    - Uses `isclose` for floating point comparison and `pprint` for debugging output.
    """
    # Example test case for conversion_hive_to_keepsats
    TrackedBaseModel.last_quote = last_quote()
    pprint(TrackedBaseModel.last_quote)
    original_amount = Amount("12.000 HIVE")
    server_account = "v4vapp_server"
    convert_amount = original_amount

    tracked_op = Transfer(
        from_account="customer_account",
        to_account=server_account,
        memo="Deposit #sats",
        amount=convert_amount,
        timestamp=datetime.now(timezone.utc),
        trx_id=fake_trx_id(),
        op_type="transfer",
        block_num=latest_block_num(),
    )

    # Test with valid conversion amount
    conversion_result = await hive_to_keepsats(tracked_op, quote=TrackedBaseModel.last_quote)
    assert conversion_result is not None
    assert conversion_result.net_to_receive > 0
    print(conversion_result)
    assert isclose(conversion_result.balance, 12.000, abs_tol=0.001), (
        "Conversion result is not as expected"
    )

    # Now use a different amount for a second conversion.
    msats = 2_500_000

    # Test with valid conversion amount
    conversion_result = await hive_to_keepsats(
        tracked_op, quote=TrackedBaseModel.last_quote, msats=msats
    )
    assert conversion_result is not None
    assert conversion_result.net_to_receive > 0
    print(conversion_result)
    assert isclose(conversion_result.balance, 12.000, abs_tol=0.001), (
        "Conversion result is not as expected"
    )

    assert isclose(conversion_result.net_to_receive_conv.msats, msats, abs_tol=0.001), (
        "Conversion amount is not as expected"
    )
