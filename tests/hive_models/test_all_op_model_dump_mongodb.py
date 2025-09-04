from datetime import timedelta
from pathlib import Path

import pytest
from pymongo.errors import DuplicateKeyError

from tests.get_last_quote import last_quote
from tests.load_data import load_hive_events
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.helpers.general_purpose_funcs import convert_decimals
from v4vapp_backend_v2.hive_models.op_account_witness_vote import AccountWitnessVote
from v4vapp_backend_v2.hive_models.op_fill_order import FillOrder
from v4vapp_backend_v2.hive_models.op_limit_order_create import LimitOrderCreate
from v4vapp_backend_v2.hive_models.op_producer_reward import ProducerReward
from v4vapp_backend_v2.hive_models.op_transfer import Transfer
from v4vapp_backend_v2.hive_models.op_types_enums import OpTypes


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
    print("InternalConfig initialized:", i_c)
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op_to_test",
    [
        {"op_type": OpTypes.TRANSFER, "collection_name": "transfer", "model": Transfer},
        {
            "op_type": OpTypes.ACCOUNT_WITNESS_VOTE,
            "collection_name": "account_witness_vote",
            "model": AccountWitnessVote,
        },
        {
            "op_type": OpTypes.PRODUCER_REWARD,
            "collection_name": "producer_reward",
            "model": ProducerReward,
        },
        {
            "op_type": OpTypes.LIMIT_ORDER_CREATE,
            "collection_name": "limit_order_create",
            "model": LimitOrderCreate,
        },
        {
            "op_type": OpTypes.FILL_ORDER,
            "collection_name": "fill_order",
            "model": FillOrder,
        },
    ],
    ids=[
        "test_transfer",
        "test_account_witness_vote",
        "test_producer_reward",
        "test_limit_order_create",
        "test_fill_order",
    ],
)
async def test_model_dump_mongodb(op_to_test):
    """
    Test the dumping of the model into MongoDB.
    This test performs the following steps:
    1. Updates the quote for the model.
    2. Connects to a MongoDB test client.
    3. Drops the existing collection if it exists.
    4. Creates a unique index on the collection.
    5. Loads hive events of the specified type and validates them against the model.
    6. Inserts the validated documents into the collection.
    7. Asserts that the document was inserted successfully.
    8. Attempts to insert a duplicate document and expects a DuplicateKeyError.
    9. Asserts that the document can be found in the collection.
    10. Asserts that a non-existent document cannot be found.
    11. Drops the test database.
    Args:
        set_base_config_path (None): Fixture to set the base configuration path.
        op_to_test (dict): Dictionary containing the operation type and collection name.
    """
    TrackedBaseModel.last_quote = last_quote()
    InternalConfig()
    # collection_name = op_to_test["collection_name"]
    collection_name = "all_ops"
    op_type = op_to_test["op_type"]
    db = InternalConfig.db

    await db[collection_name].drop()
    index_key = [["trx_id", 1], ["op_in_trx", 1], ["block_num", 1]]
    await db[collection_name].create_index(keys=index_key, name="timestamp", unique=True)
    for hive_event in load_hive_events(op_type):
        if hive_event["type"] == op_type.value:
            model_class = op_to_test["model"]
            model_instance = model_class.model_validate(hive_event)
            if op_type == OpTypes.PRODUCER_REWARD:
                model_instance.delta = timedelta(seconds=33)
                model_instance.mean = timedelta(seconds=33)
            insert_op = model_instance.model_dump(by_alias=True)
            insert_op = convert_decimals(insert_op)
            insert_ans = await db[collection_name].insert_one(
                document=insert_op,
            )
            assert insert_ans is not None
            find_one_ans = await db[collection_name].find_one(
                filter={"trx_id": model_instance.trx_id},
            )
            assert find_one_ans is not None
            with pytest.raises(DuplicateKeyError) as exc_info:
                await db[collection_name].insert_one(document=insert_op)
                print(exc_info)

    await InternalConfig.db_client.drop_database("test_db")
