import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ["TESTING"] = "True"
from google.protobuf.json_format import Parse
from grpc.aio import AioRpcError

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
import v4vapp_backend_v2.lnd_grpc.lightning_pb2_grpc as lightningstub
from v4vapp_backend_v2.lnd_grpc.lnd_client import LNDClient
from v4vapp_backend_v2.lnd_grpc.lnd_functions import (
    LNDPaymentError,
    LNDPaymentStreamError,
    is_lnd_stream_transport_error,
    lookup_outgoing_payment,
    resolve_payment_after_stream_disconnect,
    send_lightning_to_pay_req,
)
from v4vapp_backend_v2.models.pay_req import PayReq
from v4vapp_backend_v2.models.payment_models import Payment


@pytest.fixture(scope="session", autouse=True)
def disable_redis_for_session():
    """Keep Redis disabled through session teardown (conftest closes db after tests)."""
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig.setup_redis",
        lambda self: None,
    )
    yield
    # Do not undo: conftest session teardown instantiates InternalConfig after tests.


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig.setup_redis",
        lambda self: None,
    )
    yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    with open("tests/data/node_get_info_response.json") as f:
        mock_get_info = Parse(f.read(), lnrpc.GetInfoResponse())
    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_client.LNDClient.node_get_info",
        new_callable=AsyncMock,
        return_value=mock_get_info,
    ):
        yield mock_get_info


def _stream_reset_error() -> AioRpcError:
    return AioRpcError(
        code=1,
        initial_metadata=None,
        trailing_metadata=None,
        details="Stream removed (recvmsg:Connection reset by peer (104))",
        debug_error_string="Stream removed (recvmsg:Connection reset by peer (104))",
    )


def _sample_pay_req() -> PayReq:
    return PayReq(
        destination="03" + "a" * 64,
        payment_hash="9e1436f4a7f568a01297a2931bf0b6dea812fb76b37dec741602eed586e23704",
        value=1952,
        value_msat=1952000,
        pay_req_str="lnbc19520n1p49testinvoice",
        memo="Send to L-BTC address",
    )


def _mock_lnd_client(node_info, router_stub) -> MagicMock:
    async def _node_get_info():
        return node_info

    client = MagicMock()
    client.icon = "🆅"
    client.get_info = node_info
    client.node_get_info = _node_get_info()
    client.router_stub = router_stub
    client.call = AsyncMock(side_effect=lambda method, *args, **kwargs: method(*args, **kwargs))
    return client


def _sample_payment(status: str) -> Payment:
    with open("tests/data/hive_models/mongodb/payment_dict_success.json") as f:
        payment_data = json.load(f)["payment"]
    payment_data["payment_hash"] = "9e1436f4a7f568a01297a2931bf0b6dea812fb76b37dec741602eed586e23704"
    payment_data["status"] = status
    return Payment.model_validate(payment_data)


def test_is_lnd_stream_transport_error_matches_stream_reset():
    assert is_lnd_stream_transport_error(_stream_reset_error())


def test_is_lnd_stream_transport_error_rejects_real_failure():
    error = AioRpcError(
        code=1,
        initial_metadata=None,
        trailing_metadata=None,
        details="FAILURE_REASON_NO_ROUTE",
        debug_error_string="route not found",
    )
    assert not is_lnd_stream_transport_error(error)


@pytest.mark.asyncio
async def test_lookup_outgoing_payment_finds_matching_hash(set_base_config_path):
    payment = _sample_payment("IN_FLIGHT")
    list_response = lnrpc.ListPaymentsResponse()
    list_response.payments.add(
        payment_hash=payment.payment_hash,
        value=1952,
        status=lnrpc.Payment.PaymentStatus.IN_FLIGHT,
    )

    mock_list_payments = AsyncMock(return_value=list_response)
    with patch.object(
        lightningstub,
        "LightningStub",
        return_value=MagicMock(ListPayments=mock_list_payments),
    ):
        async with LNDClient(connection_name="example") as client:
            result = await lookup_outgoing_payment(client, payment.payment_hash)

    assert result is not None
    assert result.payment_hash == payment.payment_hash


@pytest.mark.asyncio
async def test_resolve_payment_after_stream_disconnect_returns_succeeded(set_base_config_path):
    payment = _sample_payment("SUCCEEDED")
    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.lookup_outgoing_payment",
        new=AsyncMock(return_value=payment),
    ):
        result = await resolve_payment_after_stream_disconnect(
            lnd_client=MagicMock(),
            pay_req=_sample_pay_req(),
            payment_id="test payment",
            error=_stream_reset_error(),
            payment_dict={"status": "IN_FLIGHT"},
        )

    assert result.status.value == "SUCCEEDED"


@pytest.mark.asyncio
async def test_resolve_payment_after_stream_disconnect_raises_stream_error_for_in_flight(
    set_base_config_path,
):
    payment = _sample_payment("IN_FLIGHT")
    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.lookup_outgoing_payment",
        new=AsyncMock(return_value=payment),
    ):
        with pytest.raises(LNDPaymentStreamError):
            await resolve_payment_after_stream_disconnect(
                lnd_client=MagicMock(),
                pay_req=_sample_pay_req(),
                payment_id="test payment",
                error=_stream_reset_error(),
                payment_dict={"status": "IN_FLIGHT"},
            )


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_stream_reset_succeeded_lookup(
    set_base_config_path,
):
    payment = _sample_payment("SUCCEEDED")

    async def failing_send_payment_v2(_request):
        raise _stream_reset_error()
        yield  # pragma: no cover

    mock_router_stub = MagicMock()
    mock_router_stub.SendPaymentV2 = failing_send_payment_v2

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.lookup_outgoing_payment",
        new=AsyncMock(return_value=payment),
    ):
        with patch(
            "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
            new=AsyncMock(return_value="Boltz Mini"),
        ):
            client = _mock_lnd_client(set_base_config_path, mock_router_stub)
            result = await send_lightning_to_pay_req(
                pay_req=_sample_pay_req(),
                lnd_client=client,
            )

    assert result.status.value == "SUCCEEDED"


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_stream_reset_raises_stream_error(
    set_base_config_path,
):
    payment = _sample_payment("IN_FLIGHT")

    async def failing_send_payment_v2(_request):
        raise _stream_reset_error()
        yield  # pragma: no cover

    mock_router_stub = MagicMock()
    mock_router_stub.SendPaymentV2 = failing_send_payment_v2

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.lookup_outgoing_payment",
        new=AsyncMock(return_value=payment),
    ):
        with patch(
            "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
            new=AsyncMock(return_value="Boltz Mini"),
        ):
            client = _mock_lnd_client(set_base_config_path, mock_router_stub)
            with pytest.raises(LNDPaymentStreamError):
                await send_lightning_to_pay_req(
                    pay_req=_sample_pay_req(),
                    lnd_client=client,
                )


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_real_failure_still_raises_payment_error(
    set_base_config_path,
):
    async def failing_send_payment_v2(_request):
        raise AioRpcError(
            code=1,
            initial_metadata=None,
            trailing_metadata=None,
            details="FAILURE_REASON_NO_ROUTE",
            debug_error_string="no route",
        )
        yield  # pragma: no cover

    mock_router_stub = MagicMock()
    mock_router_stub.SendPaymentV2 = failing_send_payment_v2

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
        new=AsyncMock(return_value="Boltz Mini"),
    ):
        client = _mock_lnd_client(set_base_config_path, mock_router_stub)
        with pytest.raises(LNDPaymentError):
            await send_lightning_to_pay_req(
                pay_req=_sample_pay_req(),
                lnd_client=client,
            )