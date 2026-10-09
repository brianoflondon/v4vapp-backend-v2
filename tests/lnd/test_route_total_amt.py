"""Route.total_amt stays valid after LND 0.22 stops sending the satoshi field."""

import json

import v4vapp_backend_v2.lnd_grpc.lightning_pb2 as lnrpc
from v4vapp_backend_v2.models.payment_models import Payment, Route


def _route_without_total_amt(total_amt_msat: int) -> lnrpc.Route:
    route = lnrpc.Route(total_time_lock=898808, total_amt_msat=total_amt_msat)
    hop = route.hops.add()
    hop.chan_id = 800082725764071425
    hop.expiry = 898808
    hop.amt_to_forward_msat = total_amt_msat
    hop.pub_key = "0266ad2656c7a19a219d37e82b280046660f4d7f3ae0c00b64a1629de4ea567668"
    return route


def _payment_with_route(route: lnrpc.Route) -> Payment:
    raw = lnrpc.Payment(
        payment_hash="3e5e24bb3cd5c77a6aff3991c57edc473f1c4381fe0c1423e16ac200a8dfd91d",
        status=lnrpc.Payment.SUCCEEDED,
    )
    attempt = raw.htlcs.add()
    attempt.attempt_id = 90085
    attempt.route.CopyFrom(route)
    return Payment(raw)


def test_payment_derives_total_amt_when_lnd_omits_it():
    payment = _payment_with_route(_route_without_total_amt(2_233_500))

    route = payment.htlcs[0].route
    assert route is not None
    assert int(route.total_amt_msat) == 2_233_500
    assert int(route.total_amt) == 2233


def test_payment_keeps_total_amt_while_lnd_still_sends_it():
    route = _route_without_total_amt(2_233_000)
    route.total_amt = 2233
    payment = _payment_with_route(route)

    assert int(payment.htlcs[0].route.total_amt) == 2233


def test_route_dict_without_total_amt_uses_string_msat():
    route = Route.model_validate(
        {
            "total_time_lock": 898808,
            "total_amt_msat": "2233500",
            "hops": [
                {
                    "chan_id": "800082725764071425",
                    "expiry": 898808,
                    "amt_to_forward_msat": "2233500",
                    "pub_key": "0266ad",
                }
            ],
        }
    )

    assert int(route.total_amt) == 2233


def test_stored_payment_keeps_its_total_amt():
    with open("tests/data/hive_models/mongodb/payment_dict_success.json") as handle:
        payment_data = json.load(handle)["payment"]

    payment = Payment.model_validate(payment_data)

    assert int(payment.htlcs[0].route.total_amt) == 2233
