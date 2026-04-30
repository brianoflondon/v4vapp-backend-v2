from decimal import ROUND_CEILING, Decimal

import pytest
from fastapi.testclient import TestClient

from api_v2 import create_app
from v4vapp_backend_v2.helpers.service_fees import (
    calculate_fee_estimate_msats,
    calculate_fee_msats,
)


@pytest.fixture
def client():
    return TestClient(create_app())


def expected_total_sats(magisats: int) -> int:
    """Replicate the endpoint calculation for comparison in tests."""
    msats = Decimal(magisats) * Decimal(1000)
    fee_msats = calculate_fee_msats(msats)
    forwarding_fee_estimate_msats = calculate_fee_estimate_msats(msats)
    to_send_msats = msats + fee_msats + forwarding_fee_estimate_msats
    return int((to_send_msats / Decimal(1000)).quantize(Decimal("1"), rounding=ROUND_CEILING))


def test_magisats_to_sats_basic(client):
    """A valid request returns expected fields and values."""
    magisats = 10_000
    r = client.get("/v2/crypto/to_keepsats/", params={"keepsats": magisats})
    assert r.status_code == 200
    data = r.json()

    assert data["receive_sats"] == magisats
    assert data["fee_msats"] > 0
    assert data["forwarding_fee_estimate_msats"] > 0
    assert data["total_to_send_sats"] > magisats  # total must exceed original
    assert data["total_to_send_sats"] == expected_total_sats(magisats)


def test_magisats_to_sats_fee_components(client):
    """fee_msats and forwarding_fee_estimate_msats are calculated correctly."""
    magisats = 50_000
    msats = Decimal(magisats) * Decimal(1000)

    r = client.get("/v2/crypto/to_keepsats/", params={"keepsats": magisats})
    assert r.status_code == 200
    data = r.json()

    assert data["fee_msats"] == int(calculate_fee_msats(msats))
    assert data["forwarding_fee_estimate_msats"] == int(calculate_fee_estimate_msats(msats))


def test_magisats_to_sats_total_rounds_up(client):
    """total_to_send_sats always rounds up (ceiling), never down."""
    for magisats in [1, 100, 9_999, 100_000]:
        r = client.get("/v2/crypto/to_keepsats/", params={"keepsats": magisats})
        assert r.status_code == 200
        data = r.json()
        assert data["total_to_send_sats"] >= expected_total_sats(magisats)


def test_magisats_to_sats_larger_amount_has_higher_fee(client):
    """Fees scale with amount — larger magisats produces a larger total."""
    r_small = client.get("/v2/crypto/to_keepsats/", params={"keepsats": 10_000})
    r_large = client.get("/v2/crypto/to_keepsats/", params={"keepsats": 1_000_000})
    assert r_small.status_code == 200
    assert r_large.status_code == 200
    assert r_large.json()["total_to_send_sats"] > r_small.json()["total_to_send_sats"]
    assert r_large.json()["fee_msats"] > r_small.json()["fee_msats"]
    assert (
        r_large.json()["forwarding_fee_estimate_msats"]
        > r_small.json()["forwarding_fee_estimate_msats"]
    )
