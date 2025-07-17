from pathlib import Path
from time import sleep

import pytest

from v4vapp_backend_v2.config.setup import InternalConfig


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


def test_redis_client_decode_responses_false():
    internal_config = InternalConfig()
    redis_client = internal_config.redis
    # Test the async Redis client
    assert redis_client is not None
    assert redis_client.ping()
    redis_client.setex("test_key", 1, "test_value")
    assert redis_client.get("test_key") == b"test_value"
    sleep(1.01)
    assert redis_client.get("test_key") is None
    internal_config.shutdown()  # Ensure proper cleanup after tests


def test_redis_client_decode_responses_true():
    internal_config = InternalConfig()
    redis_client = internal_config.redis_decoded
    # Test the async Redis client
    assert redis_client is not None
    assert redis_client.ping()
    redis_client.setex("test_key", 1, "test_value")
    assert redis_client.get("test_key") == "test_value"
    sleep(1.01)
    assert redis_client.get("test_key") is None
    internal_config.shutdown()  # Ensure proper cleanup after tests
