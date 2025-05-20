import json
from datetime import datetime, timezone

from pydantic import ValidationError
from pytest import raises

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.hive_models.custom_json_data import KeepsatsTransfer
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_custom_json import CustomJson

post1 = {
    "_id": "6740b6c755e3a8ade6050ca707d6d8b44374c1f8",
    "block_num": 94415566,
    "id": "v4vapp_transfer",
    "json": '{"hive_accname_from":"v4vapp.dev","hive_accname_to":"v4vapp-test","sats":100,"memo":null}',
    "timestamp": datetime(2025, 3, 24, 12, 15, 48, tzinfo=timezone.utc),
    "required_auths": ["v4vapp"],
    "required_posting_auths": [],
    "trx_id": "6dbf6225d6aa5c514c551c80cc08dd8c917e2808",
    "trx_num": 44,
    "type": "custom_json",
}
post2 = {
    "_id": "5d20fda5e7be2e25071d67d4be0fd8d505c92d33",
    "block_num": 94420359,
    "id": "v4vapp_transfer",
    "json": '{"hive_accname_from":"v4vapp.dev","memo":"lnbc22310n1pn7rpgspp5qswzh2j6rtjq5qw77udnm788cn5rw88d0750zaexwslcasum47cqhp59tfdd364ldvnr704x4gl8grr4hmt94g36yhnz6mjhhl4mxqqnx8scqzpgxqyz5vqsp5hmhxls5zk77s40yw2tsy2vgeque9tnqg60kntwh84eu33sxncmkq9qxpqysgq7jf42thn9eud67hz8p6pzxyaky047y6a8xkkk087kg386msq5qw3xrpvcw7ce5gras85a8d64z3l63q7nvjsn2293cxntvjhmh0wmnspd79qc0","sats":2231,"pay_result":{"payment_error":"","payment_preimage":"BKBaf0UTmC+Ag9NL1rr2rI2i0E0TDSKxz3+zrxPPc8M=","payment_hash":"BBwrqloa5AoB3vcbPfjnxOg3HO1/qPF3JnQ/jsObr7A="},"HIVE":8.181522,"HBD":1.976238,"invoice_message":"","hive_accname_to":""}',
    "op_in_trx": 0,
    "required_auths": ["v4vapp"],
    "required_posting_auths": [],
    "timestamp": datetime(2025, 3, 24, 16, 15, 42, tzinfo=timezone.utc),
    "trx_id": "259a72580bc5856cff2e2cfb24d4c5c0326a5859",
    "trx_num": 10,
    "type": "custom_json",
}


def test_custom_json_validate():
    OpBase.watch_users = ["v4vapp.dev"]
    TrackedBaseModel.update_quote_sync()
    for post in [post1, post2]:
        custom_json = CustomJson.model_validate(post)
        json_data = json.loads(post["json"])
        assert custom_json.json_data.from_account == json_data["hive_accname_from"]
        assert custom_json.json_data.to_account == json_data["hive_accname_to"]
        assert custom_json.json_data.sats == json_data["sats"]
        assert custom_json.cj_id == "v4vapp_transfer"
        assert custom_json.conv.sats == json_data["sats"]
        print(custom_json.log_str)
        print(custom_json.log_extra)
        if (
            custom_json.json_data.to_account in OpBase.watch_users
            or custom_json.json_data.from_account in OpBase.watch_users
        ):
            assert custom_json.is_watched
        else:
            assert not custom_json.is_watched


def test_custom_json_not_valid():
    post3 = post2.copy()
    post3["id"] = "podping"
    custom_json = CustomJson.model_validate(post3)
    assert isinstance(custom_json.json_data, dict)
    print(custom_json)


# Baddly formed json in a v4vapp_transfer custom_json
post4 = {
    "realm": "real",
    "trx_id": "3acf3683db021a4ac2e7d300d669908d436a4e58",
    "op_in_trx": 1,
    "type": "custom_json",
    "block_num": 94989334,
    "trx_num": 17,
    "timestamp": "2025-04-13 10:55:27+00:00",
    "id": "ssc-mainnet-hive",
    "json": '[{"contractName": "tokens", "contractAction": "transfer", "contractPayload": {"symbol": "HBR", "to": "gwajnberg", "quantity": "0.125", "memo": "Obrigado por ajudar delegando HIVE para o projeto Hive-BR"}}]',
    "required_auths": ["hive-br.voter"],
    "required_posting_auths": [],
    "link": "https://hivehub.dev/tx/3acf3683db021a4ac2e7d300d669908d436a4e58",
}


def test_custom_json_not_valid_2():
    custom_json = CustomJson.model_validate(post4)
    print(custom_json)
    assert not isinstance(custom_json.json_data, KeepsatsTransfer)
    post5 = post4.copy()
    post5["json"] = "this is bad data"
    with raises(ValidationError) as exc_info:
        custom_json = CustomJson.model_validate(post5)
    assert "Invalid JSON" in str(exc_info.value)
    assert "json_invalid" in str(exc_info.value)
