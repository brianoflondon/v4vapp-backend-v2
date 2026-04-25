"""
Tests for magi_json_data models (VSCCall / VSCCallPayload / VSCSwapPayload / VSCIntent)
and their integration with op_custom_json (id: vsc.call).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from v4vapp_backend_v2.hive_models.magi_json_data import (
    VSCCall,
    VSCCallPayload,
    VSCIntent,
    VSCSwapPayload,
)
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

# ---------------------------------------------------------------------------
# Fixtures / shared data
# ---------------------------------------------------------------------------

# ── Record 1: transfer with memo (payload is dict with hive: prefixes) ─────
VSC_CALL_JSON_STR = (
    '{   "net_id": "vsc-mainnet",   "caller": "hive:v4vapp-test",'
    '   "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",'
    '   "action": "transfer",'
    '   "payload": {     "amount": "25",     "to": "hive:devser.v4vapp",'
    '     "memo": "brianoflondon@walletofsatoshi.com #v4vapp"   },'
    '   "rc_limit": 1000 }'
)
VSC_CALL_DICT = json.loads(VSC_CALL_JSON_STR)

# ── Record 2: execute/swap – payload is a JSON string, has intents ──────────
_SWAP_PAYLOAD_STR = (
    '{"type":"swap","version":"1.0.0","asset_in":"HIVE","asset_out":"BTC",'
    '"amount_in":"26772","min_amount_out":"2041",'
    '"recipient":"bc1qskmt62sh6ej2tl4ak9wqpr69z7e50yexp2jna9",'
    '"destination_chain":"BTC"}'
)
VSC_EXECUTE_DICT = {
    "net_id": "vsc-mainnet",
    "caller": "hive:zphrs",
    "contract_id": "vsc1Brvi4YZHLkocYNAFd7Gf1JpsPjzNnv4i45",
    "action": "execute",
    "payload": _SWAP_PAYLOAD_STR,
    "rc_limit": 10000,
    "intents": [{"type": "transfer.allow", "args": {"limit": "26.772", "token": "hive"}}],
}

# ── Record 5: already-processed transfer (no hive: prefix, json is a dict) ──
VSC_PROCESSED_DICT = {
    "net_id": "vsc-mainnet",
    "caller": "v4vapp-test",
    "contract_id": "vsc1BdrQ6EtbQ64rq2PkPd21x4MaLnVRcJj85d",
    "action": "transfer",
    "payload": {"amount": "25", "to": "devser.v4vapp"},
    "rc_limit": 1000,
}

# A full custom_json Hive operation envelope for vsc.call (transfer with memo)
VSC_CALL_OP = {
    "_id": "aabbccdd11223344aabbccdd11223344aabbccdd",
    "block_num": 105775911,
    "id": "vsc.call",
    "json": VSC_CALL_JSON_STR,
    "timestamp": datetime(2026, 4, 23, 15, 58, 15, tzinfo=timezone.utc),
    "required_auths": ["v4vapp-test"],
    "required_posting_auths": [],
    "trx_id": "353e79e7f20b99ad0dce288943dd6cbd658424af",
    "trx_num": 17,
    "type": "custom_json",
}

# Envelope for the execute/swap operation
VSC_EXECUTE_OP = {
    "_id": "bbccddee22334455bbccddee22334455bbccddee",
    "block_num": 105777312,
    "id": "vsc.call",
    "json": json.dumps(VSC_EXECUTE_DICT),
    "timestamp": datetime(2026, 4, 23, 17, 8, 27, tzinfo=timezone.utc),
    "required_auths": ["zphrs"],
    "required_posting_auths": [],
    "trx_id": "3790dce308f05023511eb63cde9afb097eb23bde",
    "trx_num": 20,
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
    """payload.to with 'hive:' prefix is stored as-is (prefix preserved)."""
    payload = VSCCallPayload.model_validate({"amount": "2500", "to": "hive:v4vapp-test"})
    assert payload.to == "hive:v4vapp-test"
    assert isinstance(payload.to, str)


def test_vsc_call_payload_without_hive_prefix():
    """payload.to without 'hive:' prefix is accepted unchanged."""
    payload = VSCCallPayload.model_validate({"amount": "100", "to": "alice"})
    assert payload.to == "alice"
    assert isinstance(payload.to, str)


def test_vsc_call_payload_with_memo():
    """payload.memo is captured when present."""
    payload = VSCCallPayload.model_validate({
        "amount": "25",
        "to": "hive:devser.v4vapp",
        "memo": "brianoflondon@walletofsatoshi.com #v4vapp",
    })
    assert payload.memo == "brianoflondon@walletofsatoshi.com #v4vapp"


def test_vsc_call_payload_memo_defaults_empty():
    """memo defaults to empty string when absent."""
    payload = VSCCallPayload.model_validate({"amount": "25", "to": "alice"})
    assert payload.memo == ""


# ---------------------------------------------------------------------------
# VSCSwapPayload unit tests
# ---------------------------------------------------------------------------


def test_vsc_swap_payload_from_dict():
    """VSCSwapPayload parses the decoded swap dict correctly."""
    raw = json.loads(_SWAP_PAYLOAD_STR)
    swap = VSCSwapPayload.model_validate(raw)
    assert swap.type == "swap"
    assert swap.asset_in == "HIVE"
    assert swap.asset_out == "BTC"
    assert swap.amount_in == "26772"
    assert swap.min_amount_out == "2041"
    assert swap.destination_chain == "BTC"


def test_vsc_swap_payload_hive_to_hive():
    """VSCSwapPayload handles HBD→HIVE swaps (recipient is a Hive address)."""
    raw = {
        "type": "swap",
        "version": "1.0.0",
        "asset_in": "HBD",
        "asset_out": "HIVE",
        "amount_in": "625",
        "min_amount_out": "10187",
        "recipient": "hive:zphrs",
    }
    swap = VSCSwapPayload.model_validate(raw)
    assert swap.asset_in == "HBD"
    assert swap.recipient == "hive:zphrs"


# ---------------------------------------------------------------------------
# VSCIntent unit tests
# ---------------------------------------------------------------------------


def test_vsc_intent_parses():
    intent = VSCIntent.model_validate({
        "type": "transfer.allow",
        "args": {"limit": "26.772", "token": "hive"},
    })
    assert intent.type == "transfer.allow"
    assert intent.args.limit == "26.772"
    assert intent.args.token == "hive"


# ---------------------------------------------------------------------------
# VSCCall – transfer (record 1)
# ---------------------------------------------------------------------------


def test_vsc_call_transfer_basic():
    """VSCCall built from a transfer dict with hive: prefix on caller and to."""
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.net_id == "vsc-mainnet"
    assert call.action == "transfer"
    assert call.rc_limit == 1000
    assert isinstance(call.payload, VSCCallPayload)


def test_vsc_call_transfer_caller_stripped():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.caller == "hive:v4vapp-test"
    assert str(call.caller).startswith("hive:")


def test_vsc_call_transfer_from_account():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.from_account == "hive:v4vapp-test"


def test_vsc_call_transfer_to_account():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.to_account == "hive:devser.v4vapp"


def test_vsc_call_transfer_amount():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.amount == "25"


def test_vsc_call_transfer_memo():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert "walletofsatoshi" in call.memo


def test_vsc_call_transfer_log_str():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    log = call.log_str
    assert "hive:v4vapp-test" in log
    assert "hive:devser.v4vapp" in log
    assert "25" in log
    assert "walletofsatoshi" in log


def test_vsc_call_no_intents_on_transfer():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.intents == []


# ---------------------------------------------------------------------------
# VSCCall – execute/swap (records 2–4)
# ---------------------------------------------------------------------------


def test_vsc_call_execute_payload_is_swap():
    """Payload JSON string is decoded into VSCSwapPayload."""
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert isinstance(call.payload, VSCSwapPayload)


def test_vsc_call_execute_swap_fields():
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert call.payload.asset_in == "HIVE"
    assert call.payload.asset_out == "BTC"
    assert call.payload.amount_in == "26772"


def test_vsc_call_execute_amount_uses_amount_in():
    """amount property returns amount_in for swap payloads."""
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert call.amount == "26772"


def test_vsc_call_execute_to_account_empty():
    """to_account returns empty string for execute payloads."""
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert call.to_account == ""


def test_vsc_call_execute_memo_empty():
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert call.memo == ""


def test_vsc_call_execute_intents():
    """intents are parsed correctly."""
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    assert len(call.intents) == 1
    assert call.intents[0].type == "transfer.allow"
    assert call.intents[0].args.limit == "26.772"
    assert call.intents[0].args.token == "hive"


def test_vsc_call_execute_log_str():
    call = VSCCall.model_validate(VSC_EXECUTE_DICT)
    log = call.log_str
    assert "zphrs" in log
    assert "HIVE" in log
    assert "BTC" in log


# ---------------------------------------------------------------------------
# VSCCall – already-processed (record 5, no hive: prefix)
# ---------------------------------------------------------------------------


def test_vsc_call_processed_no_prefix():
    """Accepts caller and to without hive: prefix (plain account names)."""
    call = VSCCall.model_validate(VSC_PROCESSED_DICT)
    assert call.caller == "v4vapp-test"
    assert call.from_account == "v4vapp-test"
    assert call.to_account == "devser.v4vapp"
    assert call.amount == "25"


# ---------------------------------------------------------------------------
# VSCCall – error cases
# ---------------------------------------------------------------------------


def test_vsc_call_missing_caller_raises():
    bad = {k: v for k, v in VSC_CALL_DICT.items() if k != "caller"}
    with pytest.raises(ValidationError):
        VSCCall.model_validate(bad)


def test_vsc_call_missing_payload_raises():
    bad = {k: v for k, v in VSC_CALL_DICT.items() if k != "payload"}
    with pytest.raises(ValidationError):
        VSCCall.model_validate(bad)


def test_vsc_call_notification_str_equals_log_str():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert call.notification_str == call.log_str


def test_vsc_call_log_extra():
    call = VSCCall.model_validate(VSC_CALL_DICT)
    assert "vsc_call" in call.log_extra


# ---------------------------------------------------------------------------
# Integration: vsc.call via CustomJson (full pipeline)
# ---------------------------------------------------------------------------


def test_custom_json_vsc_call_transfer_detected():
    """CustomJson with id='vsc.call' parses json_data into a VSCCall instance."""
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    assert isinstance(custom_json.json_data, VSCCall)
    assert isinstance(custom_json.json_data.payload, VSCCallPayload)


def test_custom_json_vsc_call_transfer_fields():
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    assert custom_json.from_account == "hive:v4vapp-test"
    assert custom_json.to_account == "hive:devser.v4vapp"
    assert custom_json.cj_id == "vsc.call"


def test_custom_json_vsc_call_transfer_memo():
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    assert "walletofsatoshi" in custom_json.memo


def test_custom_json_vsc_call_execute_detected():
    """CustomJson with execute/swap op deserialises to VSCCall with VSCSwapPayload."""
    custom_json = CustomJson.model_validate(VSC_EXECUTE_OP)
    assert isinstance(custom_json.json_data, VSCCall)
    assert isinstance(custom_json.json_data.payload, VSCSwapPayload)


def test_custom_json_vsc_call_execute_intents():
    custom_json = CustomJson.model_validate(VSC_EXECUTE_OP)
    assert len(custom_json.json_data.intents) == 1
    assert custom_json.json_data.intents[0].type == "transfer.allow"


def test_custom_json_vsc_call_log_str_transfer():
    custom_json = CustomJson.model_validate(VSC_CALL_OP)
    log = custom_json.log_str
    assert "v4vapp-test" in log
    assert "devser.v4vapp" in log


def test_custom_json_vsc_call_log_str_execute():
    custom_json = CustomJson.model_validate(VSC_EXECUTE_OP)
    log = custom_json.log_str
    assert "zphrs" in log
    assert "HIVE" in log


def test_custom_json_unknown_id_not_vsc_call():
    """An unknown custom_json id does NOT produce a VSCCall instance."""
    op = VSC_CALL_OP.copy()
    op["id"] = "some.unknown.id"
    custom_json = CustomJson.model_validate(op)
    assert not isinstance(custom_json.json_data, VSCCall)


def test_custom_json_vsc_call_json_dict_input():
    """json field as a pre-parsed dict is also handled correctly."""
    op = VSC_CALL_OP.copy()
    op["json"] = VSC_CALL_DICT  # dict form
    custom_json = CustomJson.model_validate(op)
    assert isinstance(custom_json.json_data, VSCCall)
