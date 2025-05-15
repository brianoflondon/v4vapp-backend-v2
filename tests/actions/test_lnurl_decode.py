import os
from uuid import uuid4

import pytest
from pytest_mock import MockerFixture

from v4vapp_backend_v2.actions.lnurl_decode import (
    LnurlException,
    decode_any_lightning_string,
    decode_any_lnurp_or_lightning_address,
)
from v4vapp_backend_v2.actions.lnurl_models import strip_lightning
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.models.pay_req import PayReq

# @pytest.fixture(autouse=True)
# def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
#     test_config_path = Path("tests/data/config")
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
#     test_config_logging_path = Path(test_config_path, "logging/")
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
#         test_config_logging_path,
#     )
#     monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
#     yield
#     monkeypatch.setattr(
#         "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
#     )  # Resetting InternalConfig instance


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


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
@pytest.mark.asyncio
async def test_decode_any_lightning_string():
    """
    Test the decode_any_lightning_string function with various inputs.
    """
    # Test with a valid Lightning Address

    lnd_client = LNDClient(connection_name="voltage")
    input = "lightning:lnbc20720n1p5pwchppp5cc7umgmnekpym25sss7tpld8dgn3f8ymcj5wt7hk8xevdsa8myzsdzawc68vctswqhxgetkyp7zqa35wckhsjjstfzjqlpqydf5z4znyqerqdejyp7zqg6ng929xgprgdxy2s2wyq3hvdrkv9c8qcqzzsxqzxgsp57gv9xfay4lmgqgkrtydews0kr88qajj84gf4x4lraz38966rs2yq9qxpqysgqwjqhkuj0g5anqxe0tqun2hckw504q5q9cej6j4vsvav0alkrp3er06qgtxkq8v0d0s0d8jx0ucme5dlu4m77qxlllq5fy0qn3k0ameqp69a6cs"
    result = await decode_any_lightning_string(input, lnd_client=lnd_client)
    assert isinstance(result, PayReq)
    assert (
        result.destination == "0266ad2656c7a19a219d37e82b280046660f4d7f3ae0c00b64a1629de4ea567668"
    )

    input = "brianoflondon@sats.v4v.app"
    sats = 1356
    result = await decode_any_lightning_string(
        input=input, lnd_client=lnd_client, sats=sats, comment="test comment"
    )
    assert isinstance(result, PayReq)
    assert (
        result.destination == "0266ad2656c7a19a219d37e82b280046660f4d7f3ae0c00b64a1629de4ea567668"
    )
