from pathlib import Path
import pytest

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.lightning_pb2_grpc as lightningstub

from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    get_node_alias_from_pay_request,
    get_node_info,
)
from unittest.mock import AsyncMock, MagicMock, patch

from google.protobuf.json_format import Parse


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
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
    # Also mock the get_node_info_function
    with open("tests/data/lnd_functions/get_node_info_response.json") as f:
        json_data = f.read()

    mock_response = Parse(json_data, lnrpc.NodeInfo())
    mock_method = AsyncMock(return_value=mock_response)
    with patch.object(
        lightningstub,
        "LightningStub",
        return_value=MagicMock(GetNodeInfo=mock_method),
    ):
        yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.mark.asyncio
async def test_get_node_info(
    set_base_config_path: None, monkeypatch: pytest.MonkeyPatch
):
    async with LNDClient(connection_name="example") as client:
        pub_key = "0396693dee59afd67f178af392990d907d3a9679fa7ce00e806b8e373ff6b70bd8"
        result: lnrpc.NodeInfo = await get_node_info(pub_key, client)
        assert result.node.alias == "V4VAPP Hive GoPodcasting!"
        assert result.node.pub_key == pub_key


@pytest.mark.skip(
    reason="Only use this test interactively when a connection to voltage is available"
)
@pytest.mark.asyncio
async def test_get_node_info_live():
    async with LNDClient(connection_name="voltage") as client:
        pub_key = "0396693dee59afd67f178af392990d907d3a9679fa7ce00e806b8e373ff6b70bd8"
        result: lnrpc.NodeInfo = await get_node_info(pub_key, client)
        assert result.node.alias == "V4VAPP Hive GoPodcasting!"
        assert result.node.pub_key == pub_key


@pytest.mark.asyncio
async def test_get_node_alias_from_pay_request(
    set_base_config_path: None, monkeypatch: pytest.MonkeyPatch
):
    with open("tests/data/lnd_functions/get_node_info_response.json") as f:
        json_data = f.read()

    mock_node_info = Parse(json_data, lnrpc.NodeInfo())

    # Test with a valid payment request
    with open("tests/data/lnd_functions/decode_pay_req_response.json") as f:
        json_data = f.read()
    mock_response = Parse(json_data, lnrpc.PayReq())
    mock_method = AsyncMock(return_value=mock_response)
    with patch.object(
        lightningstub,
        "LightningStub",
        return_value=MagicMock(DecodePayReq=mock_method),
    ):
        with patch(
            "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_info",
            new=AsyncMock(return_value=mock_node_info),
        ):
            async with LNDClient(connection_name="example") as client:
                payment_request = "lnbc1u1pnhykavpp5egz3h400vl9dh5s3ck6g7w5dhy34hmnq3c34ydnt9d8phzkpj42qdqqcqzpgxqyz5vqsp5z97q3e70kgzn4094ywks9hrvj7msz8xxujd5phkhg7vs72w4cc7s9qxpqysgqmld4mgxw74v9vkg3l8hx2a3afk4xzhl9merh7gup9h9j5rnq0q64c2vx9vktztwlajc9kkluaaelzefyk0c0spcvtzhpcamex6qxwscp6u9hp8"
                result = await get_node_alias_from_pay_request(payment_request, client)
                assert result == "V4VAPP Hive GoPodcasting!"
