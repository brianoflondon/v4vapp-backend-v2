import asyncio
from pathlib import Path
from random import random

import pytest

from v4vapp_backend_v2.actions.cust_id_class import CustID, CustIDLockException


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


async def process_customer(customer_id: CustID, comment: str = "") -> bool:
    try:
        print(f"Starting processing for customer {customer_id} {comment}")

        async with customer_id.locked(timeout=None, blocking_timeout=None, group_id="test_group"):
            # This is the critical section where the lock is held
            print(f"Lock acquired for {customer_id}. Performing exclusive operations... {comment}")
            # Simulate some work that requires exclusive access
            await asyncio.sleep(random() * 0.1)  # Replace with actual async operations
            print(f"{comment} for {customer_id}")
            print(f"Operations completed for {customer_id}. Releasing lock... {comment}")

        print(f"Processing finished for {customer_id}")
        return True
    except CustIDLockException as e:
        print(f"Could not acquire lock for {customer_id}: {e}")
        return False


@pytest.mark.asyncio
async def test_cust_id_lock():
    customers = [
        CustID("customer123"),
        CustID("customer456"),
        CustID("customer789"),
    ]
    for customer in customers:
        print(f"Unlocking lock for {customer}")
        await customer.release_lock(cust_id=customer, group_id="test_group")

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
    print(results)
    assert all(isinstance(result, bool) for result in results), (
        "All tasks should return a boolean result"
    )
