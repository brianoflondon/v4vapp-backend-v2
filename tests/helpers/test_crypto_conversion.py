import asyncio
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
    print(conv.conversion)
    print(conv.conversion.source)
    print(conv.quote.log)


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

    assert abs(conv2.conversion.hbd - conv.conversion.hbd) < 0.01
    assert abs(conv2.conversion.hive - conv.conversion.hive) < 0.01
    assert abs(conv2.conversion.usd - conv.conversion.usd) < 0.01
    assert abs(conv2.conversion.msats - conv.conversion.msats) < 1000
    assert abs(conv2.conversion.btc - conv.conversion.btc) < 0.00001
    assert conv2.conversion.source == conv.conversion.source
    assert conv2.conversion.sats_hbd == conv.conversion.sats_hbd
    assert conv2.conversion.sats_hive == conv.conversion.sats_hive
    assert conv2.conversion.source == conv.conversion.source

    assert conv.conversion.model_dump()
    assert conv2.conversion.model_dump()


@pytest.mark.asyncio
async def test_fetch_date():
    conv = CryptoConversion(conv_from=Currency.HBD, value=1000.0)
    await conv.get_quote(use_cache=False)
    assert conv.quote is not None
    fetch_date = conv.quote.fetch_date
    assert fetch_date is not None
    await asyncio.sleep(1)
    await conv.get_quote(use_cache=False)
    fetch_date2 = conv.quote.fetch_date
    assert fetch_date2 is not None
    assert fetch_date2 > fetch_date
