import json
from pathlib import Path

import pytest
from beem.amount import Amount  # type: ignore

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
    amount = Amount("10.0 HBD")
    conv = CryptoConversion(amount=amount)
    await conv.get_quote()
    assert conv.quote is not None
    assert "sats" in conv.model_dump()
    print(json.dumps(conv.c_dict, indent=2))


@pytest.mark.parametrize(
    "conv_from, value",
    [
        (Currency.HBD, 1000.0),
        (Currency.HIVE, 10.0),
        (Currency.USD, 10.0),
        (Currency.SATS, 1000000000),
    ],
)
@pytest.mark.asyncio
async def test_crypto_conversion_parameterized(conv_from, value):
    conv = CryptoConversion(conv_from=conv_from, value=value)
    await conv.get_quote()
    assert conv.quote is not None
    assert "sats" in conv.model_dump()

    assert conv.sats == conv.c_dict[Currency.SATS]

    conv2 = CryptoConversion(conv_from=Currency.SATS, value=conv.sats, quote=conv.quote)
    print(f"{conv_from} {value}")
    for currency in Currency.__members__.values():
        print(
            currency,
            conv.c_dict[currency],
            conv2.c_dict[currency],
            abs(conv2.c_dict[currency] - conv.c_dict[currency]),
        )

    assert abs(conv2.c_dict[Currency.HBD] - conv.c_dict[Currency.HBD]) < 0.01
    assert abs(conv2.c_dict[Currency.HIVE] - conv.c_dict[Currency.HIVE]) < 0.01
    assert abs(conv2.c_dict[Currency.USD] - conv.c_dict[Currency.USD]) < 0.01
    assert abs(conv2.c_dict[Currency.MSATS] - conv.c_dict[Currency.MSATS]) < 1000
    assert abs(conv2.c_dict[Currency.BTC] - conv.c_dict[Currency.BTC]) < 0.00001
