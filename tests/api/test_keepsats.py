from decimal import Decimal

from fastapi.testclient import TestClient

from api_v2 import create_app


def make_client(monkeypatch, balance_msats: Decimal):
    """Helper to create a TestClient with keepsats_balance patched."""

    async def fake_keepsats_balance(cust_id, line_items=False):
        # return (net_msats, account_balance)
        return balance_msats, None

    monkeypatch.setattr("api_v2.keepsats_balance", fake_keepsats_balance)
    app = create_app()
    return TestClient(app)


def test_transfer_keepsats_insufficient(monkeypatch):
    # balance of 0 sats
    client = make_client(monkeypatch, Decimal(0))

    payload = {
        "hive_accname_from": "alice",
        "hive_accname_to": "bob",
        "sats": 65,
        "memo": "",
    }
    r = client.post("/lightning/keepsats/transfer", json=payload)
    assert r.status_code == 402
    data = r.json()
    detail = data.get("detail")
    assert detail["message"] == "Insufficient funds"
    # balance should be serialized as a plain int
    assert isinstance(detail["balance"], int)
    assert detail["balance"] == 0
    assert detail["requested"] == 65
    assert detail["deficit"] == 65


def test_convert_keepsats_insufficient(monkeypatch):
    # patch minimum invoice and other config to avoid additional logic
    client = make_client(monkeypatch, Decimal(0))

    payload = {
        "hive_accname": "alice",
        "sats": 1000,
        "symbol": "HIVE",
        "memo": "",
    }
    r = client.post("/lightning/keepsats/convert", json=payload)
    assert r.status_code == 402
    data = r.json()
    detail = data.get("detail")
    assert detail["message"] == "Insufficient funds"
    assert isinstance(detail["balance"], int)
    assert detail["balance"] == 0
    assert detail["requested"] == 1000
    assert detail["deficit"] == 1000
