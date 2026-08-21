import base64
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


def _capturing_router_stub(captured: dict) -> MagicMock:
    async def send_payment_v2(request):
        captured["request"] = request
        return
        yield  # pragma: no cover

    stub = MagicMock()
    stub.SendPaymentV2 = send_payment_v2
    return stub


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


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_default_sends_payment_request(
    set_base_config_path,
):
    captured: dict = {}
    mock_router_stub = _capturing_router_stub(captured)
    pay_req = _sample_pay_req()

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
        new=AsyncMock(return_value="Boltz Mini"),
    ):
        client = _mock_lnd_client(set_base_config_path, mock_router_stub)
        with pytest.raises(LNDPaymentError):
            await send_lightning_to_pay_req(pay_req=pay_req, lnd_client=client)

    request = captured["request"]
    assert request.payment_request == pay_req.pay_req_str
    assert request.dest == b""
    assert request.payment_hash == b""
    assert request.amt_msat == 0


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_probe_only_uses_dummy_hash(
    set_base_config_path,
):
    captured: dict = {}
    mock_router_stub = _capturing_router_stub(captured)
    pay_req = _sample_pay_req()

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
        new=AsyncMock(return_value="Boltz Mini"),
    ):
        client = _mock_lnd_client(set_base_config_path, mock_router_stub)
        with pytest.raises(LNDPaymentError):
            await send_lightning_to_pay_req(
                pay_req=pay_req,
                lnd_client=client,
                probe_only=True,
            )

    request = captured["request"]
    assert request.payment_request == ""
    assert request.dest == bytes.fromhex(pay_req.destination)
    assert len(request.payment_hash) == 32
    assert request.payment_hash != bytes.fromhex(pay_req.payment_hash)
    assert request.amt_msat == 1_952_000
    assert request.final_cltv_delta == 40
    assert request.max_parts == 1
    assert request.timeout_seconds == 600


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_probe_only_copies_invoice_routing_fields(
    set_base_config_path,
):
    captured: dict = {}
    mock_router_stub = _capturing_router_stub(captured)
    pay_req = PayReq(
        destination="03" + "a" * 64,
        payment_hash="9e1436f4a7f568a01297a2931bf0b6dea812fb76b37dec741602eed586e23704",
        value=1952,
        value_msat=1952000,
        pay_req_str="lnbc19520n1p49testinvoice",
        memo="Send to L-BTC address",
        cltv_expiry=80,
        payment_addr="EXwI58+yBTq8tSOtAtxsl7cBHMbkm0De10eZDynVxj0=",
        route_hints=[
            {
                "hop_hints": [
                    {
                        "node_id": "03" + "b" * 64,
                        "chan_id": "123456789",
                        "fee_base_msat": 1000,
                        "fee_proportional_millionths": 100,
                        "cltv_expiry_delta": 40,
                    }
                ]
            }
        ],
        features={
            "8": {"name": "tlv-onion", "is_required": True, "is_known": True},
            "14": {"name": "payment-addr", "is_required": True, "is_known": True},
        },
    )

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
        new=AsyncMock(return_value="Boltz Mini"),
    ):
        client = _mock_lnd_client(set_base_config_path, mock_router_stub)
        with pytest.raises(LNDPaymentError):
            await send_lightning_to_pay_req(
                pay_req=pay_req,
                lnd_client=client,
                probe_only=True,
            )

    request = captured["request"]
    assert request.final_cltv_delta == 80
    assert request.payment_addr == base64.b64decode(pay_req.payment_addr)
    assert len(request.route_hints) == 1
    hop = request.route_hints[0].hop_hints[0]
    assert hop.node_id == "03" + "b" * 64
    assert hop.chan_id == 123456789
    assert hop.fee_base_msat == 1000
    assert hop.fee_proportional_millionths == 100
    assert hop.cltv_expiry_delta == 40
    assert [int(bit) for bit in request.dest_features] == [8, 14]


@pytest.mark.asyncio
async def test_send_lightning_to_pay_req_probe_only_rejects_bad_destination(
    set_base_config_path,
):
    called = False

    async def send_payment_v2(_request):
        nonlocal called
        called = True
        return
        yield  # pragma: no cover

    mock_router_stub = MagicMock()
    mock_router_stub.SendPaymentV2 = send_payment_v2
    pay_req = PayReq(
        destination="not-a-pubkey",
        payment_hash="9e1436f4a7f568a01297a2931bf0b6dea812fb76b37dec741602eed586e23704",
        value=1952,
        value_msat=1952000,
        pay_req_str="lnbc19520n1p49testinvoice",
        memo="Send to L-BTC address",
    )

    with patch(
        "v4vapp_backend_v2.lnd_grpc.lnd_functions.get_node_alias_from_pub_key",
        new=AsyncMock(return_value="Boltz Mini"),
    ):
        client = _mock_lnd_client(set_base_config_path, mock_router_stub)
        with pytest.raises(LNDPaymentError, match="hex destination pubkey"):
            await send_lightning_to_pay_req(
                pay_req=pay_req,
                lnd_client=client,
                probe_only=True,
            )

    assert called is False
