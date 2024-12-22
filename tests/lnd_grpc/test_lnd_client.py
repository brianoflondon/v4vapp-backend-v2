from pathlib import Path
from unittest.mock import AsyncMock, patch
import v4vapp_backend_v2.config

import pytest

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as ln
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient, LNDConnectionError
from v4vapp_backend_v2.lnd_grpc.lnd_errors import (
    LNDConnectionError,
    LNDFatalError,
    LNDStartupError,
    LNDSubscriptionError,
)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):

    # Save the original values
    original_base_config_path = getattr(
        v4vapp_backend_v2.config, "BASE_CONFIG_PATH", None
    )
    original_base_logging_config_path = getattr(
        v4vapp_backend_v2.config, "BASE_LOGGING_CONFIG_PATH", None
    )

    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.BASE_LOGGING_CONFIG_PATH", test_config_logging_path
    )
    yield

    # Teardown: Restore the original values
    if original_base_config_path is not None:
        monkeypatch.setattr(
            "v4vapp_backend_v2.config.BASE_CONFIG_PATH", original_base_config_path
        )
    if original_base_logging_config_path is not None:
        monkeypatch.setattr(
            "v4vapp_backend_v2.config.BASE_LOGGING_CONFIG_PATH",
            original_base_logging_config_path,
        )


def test_lnd_client(set_base_config_path: None):
    lnd_client = LNDClient()
    assert not lnd_client.error_state
    assert lnd_client.error_code is None
    assert lnd_client.connection_check_task is None
    assert lnd_client.channel is None
    assert lnd_client.lightning_stub is None
    assert lnd_client.router_stub is None
    assert lnd_client.connection.address == "example.com:10009"


@pytest.mark.asyncio
async def test_lnd_client_connect(set_base_config_path: None):
    lnd_client = LNDClient()
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
    lnd_client = LNDClient()
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
        async with LNDClient() as client:
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
        async with LNDClient() as client:
            # Call the patched method
            response = await client.call(mock_method)

            # Assert that the mock method was called
            mock_method.assert_called_once()

            # Assert the response is as expected
            assert response == "mocked response"


@pytest.mark.asyncio
async def test_lnd_client_connect_error(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.BASE_LOGGING_CONFIG_PATH", test_config_logging_path
    )

    test_config_path_bad = Path("tests/data/config-bad")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.BASE_CONFIG_PATH", test_config_path_bad
    )


    try:
        lnd_client = LNDClient()
        print("Connection:")
        print(lnd_client.connection)
        print("Address:")
        print(lnd_client.connection.address)
    except Exception as e:
        print(f"Error: {e}")

    with pytest.raises(LNDStartupError):
        _ = LNDClient()
