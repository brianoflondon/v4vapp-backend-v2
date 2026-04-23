"""
Tests for magi_json_data models (VSCCall / VSCCallPayload)
and their integration with op_custom_json (id: vsc.call).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from v4vapp_backend_v2.hive_models.account_name_type import AccName
from v4vapp_backend_v2.hive_models.magi_json_data import VSCCall, VSCCallPayload
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

# ---------------------------------------------------------------------------
# Fixtures / shared data
# ---------------------------------------------------------------------------

# The exact JSON string as it arrives from the blockchain
VSC_CALL_JSON_STR = (
    '{"net_id":"vsc-mainnet","caller":"hive:devser.v4vapp",'
    '"contract_id":"vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",'
    '"action":"transfer","payload":{"amount":"2500","to":"hive:v4vapp-test"},'
    '"rc_limit":1000}'
)

# Same data as a Python dict (already deserialized)
VSC_CALL_DICT = json.loads(VSC_CALL_JSON_STR)

# A full custom_json Hive operation envelope for vsc.call
VSC_CALL_OP = {
    "_id": "aabbccdd11223344aabbccdd11223344aabbccdd",
    "block_num": 99000001,
    "id": "vsc.call",
    "json": VSC_CALL_JSON_STR,
    "timestamp": datetime(2026, 4, 23, 10, 0, 0, tzinfo=timezone.utc),
    "required_auths": ["devser.v4vapp"],
    "required_posting_auths": [],
    "trx_id": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    "trx_num": 1,
    "type": "custom_json",
}


@pytest.fixture(autouse=True)
def configure_and_reset_config(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


# ---------------------------------------------------------------------------
# VSCCallPayload unit tests
# ---------------------------------------------------------------------------


def test_vsc_call_payload_with_hive_prefix():
    """payload.to with 'hive:' prefix is stripped and stored as bare account name."""
    payload = VSCCallPayload.model_validate({"amount": "2500", "to": "hive:v4vapp-test"})
    assert payload.to_account == "v4vapp-test"
    assert isinstance(payload.to_account, AccName)


def test_vsc_call_payload_without_hive_prefix():
    """payload.to without 'hive:' prefix is accepted unchanged."""
    payload = VSCCallPayload.model_validate({"amount": "100", "to": "alice"})
    assert payload.to_account == "alice"
    assert isinstance(payload.to_account, AccName)


# ---------------------------------------------------------------------------
# VSCCall unit tests
# ---------------------------------------------------------------------------


def test_vsc_call_from_string():
    """VSCCall can be built directly from the raw JSON string payload."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.net_id == "vsc-mainnet"
    assert call.caller == "devser.v4vapp"
    assert isinstance(call.caller, AccName)
    assert call.contract_id == "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d"
    assert call.action == "transfer"
    assert call.rc_limit == 1000


def test_vsc_call_caller_hive_prefix_stripped():
    """The 'hive:' prefix in caller is stripped during validation."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert not str(call.caller).startswith("hive:")
    assert call.caller == "devser.v4vapp"


def test_vsc_call_from_account_property():
    """from_account property returns caller as AccName."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.from_account == AccName("devser.v4vapp")


def test_vsc_call_to_account_property():
    """to_account property returns payload.to_account as AccName."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.to_account == AccName("v4vapp-test")


def test_vsc_call_amount_property():
    """amount property returns the payload amount string."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.amount == "2500"


def test_vsc_call_log_str():
    """log_str contains key fields."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    log = call.log_str
    assert "devser.v4vapp" in log
    assert "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d" in log
    assert "transfer" in log
    assert "2500" in log
    assert "v4vapp-test" in log


def test_vsc_call_notification_str_equals_log_str():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.notification_str == call.log_str


def test_vsc_call_log_extra():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    extra = call.log_extra
    assert "vsc_call" in extra


def test_vsc_call_missing_caller_raises():
    """Omitting required 'caller' field raises a ValidationError."""
    bad = {k: v for k, v in VSC_CALL_DICT.items() if k != "caller"}
    with pytest.raises(ValidationError):
        VSCCall.model_validate(bad)


def test_vsc_call_missing_payload_raises():
    """Omitting required 'payload' field raises a ValidationError."""
    bad = {k: v for k, v in VSC_CALL_DICT.items() if k != "payload"}
    with pytest.raises(ValidationError):
        VSCCall.model_validate(bad)


# ---------------------------------------------------------------------------
# Integration: vsc.call via CustomJson (full pipeline)
# ---------------------------------------------------------------------------


def test_custom_json_vsc_call_detected():
    """CustomJson with id='vsc.call' parses json_data into a VSCCall instance."""
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    assert isinstance(custom_json.json_data, VSCCall)


def test_custom_json_vsc_call_fields():
    """Fields surfaced through CustomJson properties are correct."""
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    assert custom_json.from_account == "devser.v4vapp"
    assert custom_json.to_account == "v4vapp-test"
    assert custom_json.cj_id == "vsc.call"


def test_custom_json_vsc_call_log_str():
    """log_str delegates to VSCCall.log_str and appends link + short_id."""
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    log = custom_json.log_str
    assert "devser.v4vapp" in log
    assert "transfer" in log


def test_custom_json_vsc_call_notification_str():
    """notification_str delegates to VSCCall.notification_str and appends markdown link."""
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    notif = custom_json.notification_str
    assert "devser.v4vapp" in notif


def test_custom_json_vsc_call_json_str_input():
    """json field as a raw JSON string is deserialised properly."""
    op = VSC_CALL_OP.copy()
    op["json"] = VSC_CALL_JSON_STR  # ensure string form
    custom_json = CustomJson.model_validate(op)
    assert isinstance(custom_json.json_data, VSCCall)
    assert custom_json.json_data.amount == "2500"


def test_custom_json_vsc_call_json_dict_input():
    """json field as a pre-parsed dict is also handled correctly."""
    op = VSC_CALL_OP.copy()
    op["json"] = VSC_CALL_DICT  # dict form
    custom_json = CustomJson.model_validate(op)
    assert isinstance(custom_json.json_data, VSCCall)


def test_custom_json_unknown_id_not_vsc_call():
    """An unknown custom_json id does NOT produce a VSCCall instance."""
    op = VSC_CALL_OP.copy()
    op["id"] = "some.unknown.id"
    custom_json = CustomJson.model_validate(op)
    assert not isinstance(custom_json.json_data, VSCCall)
