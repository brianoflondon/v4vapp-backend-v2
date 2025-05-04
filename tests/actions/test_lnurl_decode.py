from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from v4vapp_backend_v2.actions.lnurl_decode import (
    LnurlException,
    decode_any_lnurp_or_lightning_address,
)
from v4vapp_backend_v2.actions.lnurl_models import strip_lightning


def test_strip_lightning():
    test_word = "lightning:" + uuid4().hex
    assert "lightning:" not in strip_lightning(test_word)
    test_word = "lig" + uuid4().hex
    assert "lightning:" not in strip_lightning(test_word)
    test_word = "⚡️" + uuid4().hex
    assert "lightning:" not in strip_lightning(test_word)


# Note this test relies on walletofsatoshi.com getalby.com
# coincorner.io being up and running and the LNURLp endpoint being enabled.
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data, result",
    [
        ("yelpingparade74@walletofsatoshi.com", "LightningAddress"),
        ("brianoflondon@coincorner.io", "LightningAddress"),
        ("v4vapp.dev@v4v.app", "LightningAddress"),
        ("adam@getalby.com", "LightningAddress"),
        (
            "lightning:LNURL1DP68GURN8GHJ7A35WCHXZURS9UH8WETVDSKKKMN0WAHZ7MRWW4EXCUP0WC68VCTSWQHXGETKM68ZVR",  # noqa
            "bech32Lnurl",
        ),
        ("brianoflondon@sats.v4v.app", "LightningAddress"),
        ("brianoflondon@failure.v4v.app", "LightningAddress"),
    ],
    ids=[
        "wallet of satoshi",
        "sendsats",
        "v4vapp",
        "getalby",
        "bech32",
        "v4vapp sats",
        "v4vapp failure",
    ],
)
async def test_lnurlp_proxy_lightning_addresses(
    mocker: MockerFixture, request, data: str, result: str
):
    # mocker.patch(
    #     "v4vapp_api_ext.helpers.lightning_node.read_hiveconfig_from_hive",
    #     new_callable=AsyncMock,
    #     return_value={
    #         "maximum_invoice_payment_sats": 250_000,
    #         "minimum_invoice_payment_sats": 500,
    #     },
    # )
    for param in ["LightningAddress", "bech32Lnurl", "decodedUrl", "anything"]:
        json_data = {param: data}

        if (result == param or param == "anything") and "failure" not in request.node.name:
            answer = await decode_any_lnurp_or_lightning_address(json_data)
            assert answer.tag == "payRequest"

        elif "failure" in request.node.name:
            with pytest.raises(LnurlException) as ex:
                await decode_any_lnurp_or_lightning_address(json_data)
            print(ex)
        else:
            with pytest.raises(LnurlException) as ex:
                await decode_any_lnurp_or_lightning_address(json_data)
            print(ex)
