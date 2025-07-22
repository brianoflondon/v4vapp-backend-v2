from pathlib import Path
from pprint import pprint
from timeit import default_timer as timer

import pytest

from tests.get_last_quote import last_quote
from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.config.setup import InternalConfig
from v4vapp_backend_v2.database.db_pymongo import DBConn
from v4vapp_backend_v2.hive_models.op_all import op_any_or_base
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment


@pytest.fixture(scope="module")
def module_monkeypatch():
    """MonkeyPatch fixture with module scope."""
    from _pytest.monkeypatch import MonkeyPatch

    monkey_patch = MonkeyPatch()
    yield monkey_patch
    monkey_patch.undo()  # Restore original values after module tests


@pytest.fixture(autouse=True, scope="module")
async def set_base_config_path_combined(module_monkeypatch):
    test_config_path = Path("tests/data/config")
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    module_monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    i_c = InternalConfig()
    print("InternalConfig initialized:", i_c)
    db_conn = DBConn()
    await db_conn.setup_database()
    yield
    module_monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


params = {
    "hive_ops": op_any_or_base,
    "invoices": Invoice.model_validate,
    "payments": Payment.model_validate,
}


@pytest.mark.parametrize("collection_name,validator", params.items())
async def test_db_collections(collection_name, validator):
    """
    Parameterized test for validating documents in different collections.
    """
    # Initialize the database client
    TrackedBaseModel.last_quote = last_quote()
    # Get the collection
    collection = InternalConfig.db[collection_name]
    cursor = collection.find({})
    count = 0
    start = timer()

    # Iterate through the documents in the collection
    async for doc in cursor:
        count += 1
        try:
            # Validate the document using the provided validator
            id = doc.get("_id")
            validated_doc = validator(doc)
            assert validated_doc
        except Exception as e:
            print(f"Error validating document in collection '{collection_name}': {e}")
            print(f"Document: {id}")
            print("--------------------------------------")
            pprint(doc, indent=2)

    print(f"Total documents in {collection_name}: {count} in {timer() - start:.2f} seconds")
