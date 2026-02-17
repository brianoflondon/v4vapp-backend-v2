from datetime import datetime, timezone
from pathlib import Path

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals_for_mongodb
from v4vapp_backend_v2.hive_models.op_base import OpBase
from v4vapp_backend_v2.hive_models.op_producer_missed import ProducerMissed
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward


@pytest.fixture(autouse=True)
async def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


@pytest.mark.asyncio
async def test_witness_first_run_ignores_non_producer_reward(set_base_config_path_combined: None):
    """If the DB contains other ops with `producer` (e.g. producer_missed),
    `witness_first_run` must still return the last *producer_reward* document.

    This used to fail because the query only filtered on `producer` and
    the code tried to validate a non-`producer_reward` doc as
    `ProducerReward` (missing `vesting_shares`).
    """
    db = InternalConfig.db

    producer_name = "test-witness-xyz"

    # Insert a ProducerMissed with a higher block_num (should be ignored)
    missed = ProducerMissed.model_validate(
        {
            "trx_id": "trx-missed-1",
            "type": "producer_missed",
            "block_num": 200,
            "producer": producer_name,
            "timestamp": datetime.now(tz=timezone.utc),
        }
    )
    missed_doc = convert_decimals_for_mongodb(missed.model_dump(by_alias=True))
    await db[OpBase.collection().name].insert_one(document=missed_doc)

    # Insert an older ProducerReward (this is the one we expect to be returned)
    reward = ProducerReward.model_validate(
        {
            "trx_id": "trx-reward-1",
            "type": "producer_reward",
            "block_num": 100,
            "producer": producer_name,
            "vesting_shares": {"amount": "123456", "nai": "@@000000037", "precision": 6},
            "timestamp": datetime.now(tz=timezone.utc),
        }
    )
    reward_doc = convert_decimals_for_mongodb(reward.model_dump(by_alias=True))
    await db[OpBase.collection().name].insert_one(document=reward_doc)

    # Now call the function under test
    from src.hive_monitor_v2 import witness_first_run

    result = await witness_first_run(producer_name)
    assert result is not None
    assert isinstance(result, ProducerReward)
    assert result.trx_id == "trx-reward-1"
