import os
from time import sleep

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig, logger


@pytest.mark.skip("Skipping test for local notification flood")
@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Skipping test on GitHub Actions"
)
# @pytest.mark.asyncio
def test_local_notification_flood():
    """
    Test to ensure that the local notification flood is handled correctly.
    """
    # Simulate a local notification flood
    # This is a placeholder for the actual implementation
    # In a real scenario, you would trigger the flood and check the results
    config_instance = InternalConfig(
        bot_name="@brianoflondon_bot", config_filename="hive.config.yaml"
    )
    assert config_instance is not None
    for n in range(100):
        logger.info(
            f"{n:>4} Local notification flood test completed successfully.",
            extra={"notification": True},
        )
    while config_instance.notification_lock:
        sleep(0.2)
    sleep(1)
    #     await asyncio.sleep(1)
    # await asyncio.sleep(2)


# Simulate some delay for the test
