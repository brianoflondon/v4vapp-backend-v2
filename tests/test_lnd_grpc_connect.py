import os

import pytest

from v4vapp_backend_v2.lnd_grpc.connect import (  # replace with the actual import
    connect_to_lnd,
    subscribe_invoices,
)
from v4vapp_backend_v2.lnd_grpc.lnd_connection import LNDConnectionSettings


@pytest.mark.skip(reason="This test is not implemented yet.")
@pytest.mark.asyncio
async def test_connect_to_lnd():
    stub = await connect_to_lnd()
    assert stub is not None


@pytest.mark.skip(reason="This test is not implemented yet.")
@pytest.fixture(autouse=True)
def set_up_and_tear_down():
    # Backup the original environment variable
    original_lnd_use_local_node = os.environ.get("LND_USE_LOCAL_NODE")
    yield
    # Restore the original environment variable
    if original_lnd_use_local_node is not None:
        os.environ["LND_USE_LOCAL_NODE"] = original_lnd_use_local_node
    else:
        del os.environ["LND_USE_LOCAL_NODE"]


@pytest.mark.skip(reason="This test is not implemented yet.")
def test_local_node_settings(monkeypatch):
    monkeypatch.setenv("LND_USE_LOCAL_NODE", "local")
    settings = LNDConnectionSettings()

    assert settings.address == "10.0.0.5:10009"
    assert settings.options == [("grpc.ssl_target_name_override", "umbrel.local")]
    assert isinstance(settings.macaroon, bytes)  # Check if macaroon is bytes
    assert isinstance(settings.cert, bytes)  # Check if cert is bytes


@pytest.mark.skip(reason="This test is not implemented yet.")
def test_remote_node_settings(monkeypatch):
    monkeypatch.setenv("LND_USE_LOCAL_NODE", "remote")
    settings = LNDConnectionSettings()

    assert settings.address == "v4vapp.m.voltageapp.io:10009"
    assert settings.options == [
        ("grpc.ssl_target_name_override", "v4vapp.m.voltageapp.io")
    ]
    assert isinstance(settings.macaroon, bytes)  # Check if macaroon is bytes
    assert isinstance(settings.cert, bytes)  # Check if cert is bytes
