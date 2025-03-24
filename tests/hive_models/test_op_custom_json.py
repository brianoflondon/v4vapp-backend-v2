from datetime import datetime, timezone

import pytz

from v4vapp_backend_v2.hive_models.op_custom_json import CustomJsonKeepsats

post = {
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


def test_custom_json_validate():
    custom_json = CustomJsonKeepsats.model_validate(post)
    assert custom_json.json_data.from_account == "v4vapp.dev"
    assert custom_json.json_data.to_account == "v4vapp-test"
    assert custom_json.json_data.sats == 100
    assert custom_json.json_data.memo == ""
