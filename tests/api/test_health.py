from fastapi.testclient import TestClient

from api_v2 import create_app


def test_health_endpoint():
    app = create_app()
    client = TestClient(app)

    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data.get("status") == "running"
    assert "server_id" in data
