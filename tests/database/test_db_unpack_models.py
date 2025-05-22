from pathlib import Path
from pprint import pprint
from timeit import default_timer as timer

import pytest

from v4vapp_backend_v2.actions.tracked_models import TrackedBaseModel
from v4vapp_backend_v2.database.db import MongoDBClient
from v4vapp_backend_v2.hive_models.op_all import op_any_or_base
from v4vapp_backend_v2.models.invoice_models import Invoice
from v4vapp_backend_v2.models.payment_models import Payment


@pytest.fixture(autouse=True)
def set_base_config_path_combined(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


params = {
    "hive_ops": op_any_or_base,
    "invoices": Invoice.model_validate,
    "payments": Payment.model_validate,
}


@pytest.mark.asyncio
@pytest.mark.parametrize("collection_name,validator", params.items())
async def test_db_collections(collection_name, validator):
    """
    Parameterized test for validating documents in different collections.
    """
    # Initialize the database client
    await TrackedBaseModel.update_quote()
    async with MongoDBClient(
        db_conn="conn_1",
        db_name="lnd_monitor_v2_voltage",
        db_user="lnd_monitor",
    ) as db_client:
        # Get the collection
        collection = await db_client.get_collection(collection_name)
        cursor = collection.find()
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
