import asyncio
from pathlib import Path
from random import random
from timeit import default_timer as timeit

import pytest

from v4vapp_backend_v2.process.cust_id_class import CustID, CustIDLockException
from v4vapp_backend_v2.config.setup import InternalConfig, logger
from v4vapp_backend_v2.hive_models.account_name_type import AccName


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
    i_c = InternalConfig()
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance


TEST_BASE_TIME = 0.5
TEST_RAND_TIME = 1


async def process_customer(customer_id: CustID, comment: str = "") -> bool:
    try:
        logger.info(f"Starting processing for customer {customer_id} {comment}")
        start = timeit()
        async with customer_id.locked(timeout=None, blocking_timeout=None):
            # This is the critical section where the lock is held
            sleep_time = TEST_BASE_TIME + random() * TEST_RAND_TIME
            logger.info(
                f"Lock acquired for {customer_id}. {sleep_time:.1f}s Performing exclusive operations... {comment}"
            )
            # Simulate some work that requires exclusive access
            await asyncio.sleep(sleep_time)
            logger.info(f"{comment} for {customer_id}")
            logger.info(f"Operations completed for {customer_id}. Releasing lock... {comment}")

        logger.info(f"Processing finished for {customer_id} after {timeit() - start:.1f}s")
        return True
    except CustIDLockException as e:
        logger.info(f"Could not acquire lock for {customer_id}: {e}")
        return False


@pytest.mark.asyncio
async def test_cust_id_lock():
    customers = [
        CustID("customer123"),
        CustID("customer456"),
        CustID("customer789"),
    ]
    for customer in customers:
        logger.info(f"Unlocking lock for {customer}")
        await CustID.release_lock(cust_id=str(customer))

    cust = CustID("customer123")
    cust2 = CustID("customer123")  # Same customer to test lock
    tasks = [
        process_customer(cust, "First processing"),
        process_customer(CustID("customer456"), "Only processing"),
        process_customer(cust2, "Second processing"),  # Same customer to test lock
        process_customer(CustID("customer789"), "Another customer processing"),
        process_customer(CustID("customer123"), "Third processing"),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    logger.info(results)
    assert all(isinstance(result, bool) for result in results), (
        "All tasks should return a boolean result"
    )
    await CustID.clear_all_locks()
    logger.info("All locks cleared after test completion.")

def test_cust_id():
    cust_id = CustID("testaccount")
    assert isinstance(cust_id, str), "CustID should be a string"
    assert cust_id.link == "https://hivehub.dev/@testaccount", "Link property is incorrect"
    assert cust_id.markdown_link == "[testaccount](https://hivehub.dev/@testaccount)", (
        "Markdown link property is incorrect"
    )
    assert cust_id.is_hive, "valid_hive_account should return True for valid account"

    cust_id = CustID("0x98689kjhkjhiuh")
    assert not cust_id.is_hive, "valid_hive_account should return False for invalid account"
