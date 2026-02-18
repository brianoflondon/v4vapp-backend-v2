from types import SimpleNamespace

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson


class DummyJson:
    def __init__(self, from_account=None, to_account=None):
        self.from_account = from_account
        self.to_account = to_account


def make_conf():
    cfg = SimpleNamespace()
    cfg.hive = SimpleNamespace(
        all_account_names=["server", "treasury", "funding", "exchange"],
        server_account_names=["server"],
    )  # type: ignore
    cfg.hive.custom_json_prefix = "test"
    return cfg


def test_custom_json_uses_transfer_logic_authorized_server_to_treasury(monkeypatch):
    # Patch the singleton instance used by InternalConfig()
    monkeypatch.setattr(
        InternalConfig,
        "_instance",
        SimpleNamespace(
            config=SimpleNamespace(
                hive=SimpleNamespace(
                    all_account_names=["server", "treasury", "funding", "exchange"],
                    server_account_names=["server"],
                    custom_json_prefix="test",
                    custom_json_ids_tracked=["test_transfer", "test_notification"],
                ),
                expense_config=SimpleNamespace(hive_expense_accounts=[]),
            )
        ),
    )
    post = {
        "type": "custom_json",
        "id": "test_transfer",
        "json": {
            "hive_accname_from": "server",
            "hive_accname_to": "treasury",
            "sats": 0,
            "msats": 0,
            "memo": "",
        },
        "required_auths": ["server"],
        "required_posting_auths": [],
        "trx_id": "tx",
        "block_num": 1089989887,
        "trx_num": 3,
        "timestamp": "2025-01-01T00:00:00+00:00",
    }
    cj = CustomJson.model_validate(post)
    assert cj.authorized is True
    assert cj.cust_id == "treasury"


def test_custom_json_uses_transfer_logic_authorized_server_to_customer(monkeypatch):
    # ensure custom_json_prefix is present so the ID is treated as a KeepsatsTransfer
    monkeypatch.setattr(
        InternalConfig,
        "_instance",
        SimpleNamespace(
            config=SimpleNamespace(
                hive=SimpleNamespace(
                    all_account_names=["server", "treasury", "funding", "exchange"],
                    server_account_names=["server"],
                    custom_json_prefix="test",
                    custom_json_ids_tracked=["test_transfer", "test_notification"],
                ),
                expense_config=SimpleNamespace(hive_expense_accounts=[]),
            )
        ),
    )
    post = {
        "type": "custom_json",
        "id": "test_transfer",
        "json": {
            "hive_accname_from": "server",
            "hive_accname_to": "alice",
            "sats": 0,
            "msats": 0,
            "memo": "",
        },
        "required_auths": ["server"],
        "required_posting_auths": [],
        "trx_id": "tx",
        "block_num": 1089989887,
        "trx_num": 3,
        "timestamp": "2025-01-01T00:00:00+00:00",
    }
    cj = CustomJson.model_validate(post)
    assert cj.authorized is True
    assert cj.cust_id == "alice"


def test_custom_json_unauthorized_sets_server_id(monkeypatch):
    monkeypatch.setattr(
        InternalConfig,
        "_instance",
        SimpleNamespace(
            config=SimpleNamespace(
                hive=SimpleNamespace(
                    all_account_names=["server", "treasury", "funding", "exchange"],
                    server_account_names=["server"],
                    custom_json_ids_tracked=[],
                )
            ),
            server_id="server",
        ),
    )

    post = {
        "type": "custom_json",
        "id": "1",
        "json": DummyJson(from_account="bob", to_account="server"),
        "required_auths": ["someone_else"],
        "required_posting_auths": [],
        "trx_id": "tx",
        "block_num": 1,
        "trx_num": 0,
        "timestamp": "2025-01-01T00:00:00+00:00",
    }
    cj = CustomJson.model_validate(post)
    assert cj.authorized is False
    assert cj.cust_id == "server"
