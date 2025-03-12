from pathlib import Path

import pytest
from beem.amount import Amount

from v4vapp_backend_v2.helpers.crypto_conversion import CryptoConversion
from v4vapp_backend_v2.helpers.crypto_prices import Currency


@pytest.fixture(autouse=True)
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.mark.asyncio
async def test_crypto_conversion():
    amount = Amount("10 HBD")
    conv = CryptoConversion(amount=amount)
    await conv.get_quote()
    assert conv.quote is not None
    assert "sats" in conv.model_dump()

    assert conv.sats == conv.c_dict[Currency.SATS]

    conv2 = CryptoConversion(conv_from=Currency.SATS, value=conv.sats, quote=conv.quote)

    assert conv2.c_dict[Currency.HBD] == conv.c_dict[Currency.HBD]
    assert conv2.c_dict[Currency.HIVE] == conv.c_dict[Currency.HIVE]
    assert conv2.c_dict[Currency.USD] == conv.c_dict[Currency.USD]
    assert conv2.c_dict[Currency.SATS] == conv.c_dict[Currency.SATS]
