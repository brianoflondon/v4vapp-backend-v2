from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import backoff
import pytest
from grpc.aio import AioRpcError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
import v4vapp_backend_v2.lnd_grpc.lightning_pb2_grpc as lnrpc
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient, LNDConnectionError
from v4vapp_backend_v2.lnd_grpc.lnd_errors import LNDConnectionError


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
    yield

    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
def set_base_config_path_bad(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )

    monkeypatch.setattr(
        "v4vapp_backend_v2.lnd_grpc.lnd_connection.InternalConfig._instance",
        None,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_lnd_client(set_base_config_path: None):
    lnd_client = LNDClient(connection_name="example")
    assert not lnd_client.error_state
    assert lnd_client.error_code is None
    assert lnd_client.connection_check_task is None
    assert lnd_client.channel is None
    assert lnd_client.lightning_stub is None
    assert lnd_client.router_stub is None
    assert lnd_client.connection.address == "example.com:10009"


@pytest.mark.asyncio
async def test_lnd_client_connect(set_base_config_path: None):
    lnd_client = LNDClient(connection_name="example")
    await lnd_client.connect()
    assert lnd_client.channel is not None
    assert lnd_client.lightning_stub is not None
    assert lnd_client.router_stub is not None
    assert not lnd_client.error_state
    assert lnd_client.error_code is None
    assert lnd_client.connection_check_task is None
    assert lnd_client.connection.address == "example.com:10009"


@pytest.mark.asyncio
async def test_check_connection_fails(set_base_config_path: None):
    lnd_client = LNDClient(connection_name="example")
    await lnd_client.connect()
    with pytest.raises(LNDConnectionError) as e:
        await lnd_client.check_connection(call_name="test_connection", max_tries=2)
    print(f"Error: {e.value}")
    assert "Too many errors" in str(e.value)
    assert e.value.args[1] == 2
    assert lnd_client.error_state


@pytest.mark.asyncio
async def test_get_balance(set_base_config_path: None, monkeypatch: pytest.MonkeyPatch):
    # monkey patch the lnd_client.call to return {'balance': 100}
    mock_response = ln.ChannelBalanceResponse(balance=100)
    mock_method = AsyncMock(return_value=mock_response)

    with patch.object(LNDClient, "call", mock_method):
        async with LNDClient(connection_name="example") as client:
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            assert balance == mock_response

            # Assert that the mock method was called
            mock_method.assert_called_once()


@pytest.mark.asyncio
async def test_call_method(set_base_config_path: None):
    # Create a mock method to replace the actual call method
    mock_method = AsyncMock(return_value="mocked response")

    # Patch the call method in the LNDClient class
    with patch.object(LNDClient, "call", mock_method):
        async with LNDClient(connection_name="example") as client:
            # Call the patched method
            response = await client.call(mock_method)

            # Assert that the mock method was called
            mock_method.assert_called_once()

            # Assert the response is as expected
            assert response == "mocked response"


@pytest.mark.asyncio
async def test_channel_balance_with_retries(set_base_config_path: None):
    # Mock response and error
    mock_response = ln.ChannelBalanceResponse(balance=100)
    mock_error = AioRpcError(
        code=1,
        initial_metadata=None,
        trailing_metadata=None,
        details="Mock Test Data Error",
        debug_error_string="Mock Test Data Error Debug String",
    )

    # Create a mock method with side effects
    retries = 2
    mock_method = AsyncMock(
        side_effect=[mock_response] + [mock_error] * retries + [mock_response]
    )

    mock_client = LNDClient(connection_name="example")
    mock_client.connect()

    with patch.object(
        lnrpc, "LightningStub", return_value=MagicMock(ChannelBalance=mock_method)
    ):
        async with LNDClient(connection_name="example") as client:
            # First call should succeed
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            assert balance == mock_response

            for _ in range(retries):
                with pytest.raises(LNDConnectionError) as e:
                    balance = await client.call(
                        client.lightning_stub.ChannelBalance,
                        ln.ChannelBalanceRequest(),
                    )
                    print(e)
                    print(mock_method.call_count)

            # Final call should succeed again
            balance: ln.ChannelBalanceResponse = await client.call(
                client.lightning_stub.ChannelBalance,
                ln.ChannelBalanceRequest(),
            )
            assert balance == mock_response

            # Assert that the mock method was called 5 times
            assert mock_method.call_count == retries + 2
