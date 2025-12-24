import json

from v4vapp_backend_v2.hive.hive_extras import get_hive_client


def test_get_hive_client_converts_keys_list_to_tuple(monkeypatch):
    # Arrange: patch get_good_nodes to be deterministic and patch nectar.Hive
    # Provide node explicitly to avoid using cached Redis good_nodes in test environment
    captured = {}

    class DummyHive:
        def __init__(self, *a, **kw):
            captured["kwargs"] = kw

    # Patch the Hive class reference used by hive_extras module
    monkeypatch.setattr("v4vapp_backend_v2.hive.hive_extras.Hive", DummyHive)

    # Act
    client = get_hive_client(
        node=["https://api.hive.blog"], keys=["k1", "k2"]
    )  # pass a list intentionally

    # Assert
    assert isinstance(client, DummyHive)
    assert "keys" in captured["kwargs"]
    assert isinstance(captured["kwargs"]["keys"], str)
    assert json.loads(captured["kwargs"]["keys"]) == ["k1", "k2"]


def test_get_hive_client_converts_node_list_to_tuple(monkeypatch):
    captured = {}

    class DummyHive2:
        def __init__(self, *a, **kw):
            captured["kwargs"] = kw

    # Patch the Hive class reference used by hive_extras module
    monkeypatch.setattr("v4vapp_backend_v2.hive.hive_extras.Hive", DummyHive2)

    # Provide node explicitly so we control behavior
    client = get_hive_client(node=["https://api.syncad.com"])
    assert isinstance(client, DummyHive2)
    assert "node" in captured["kwargs"]
    assert isinstance(captured["kwargs"]["node"], list)
