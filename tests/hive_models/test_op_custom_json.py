from datetime import datetime, timezone
import json

import pytz

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
    for post in [post1, post2]:
        custom_json = CustomJson.model_validate(post)
        json_data = json.loads(post["json"])
        assert custom_json.json_data.from_account == json_data["hive_accname_from"]
        assert custom_json.json_data.to_account == json_data["hive_accname_to"]
        assert custom_json.json_data.sats == json_data["sats"]
        assert custom_json.cj_id == "v4vapp_transfer"
