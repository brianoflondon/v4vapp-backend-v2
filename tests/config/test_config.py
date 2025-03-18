from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from yaml import safe_load

from v4vapp_backend_v2.config.setup import (
    Config,
    InternalConfig,
    NotificationBotConfig,
    StartupFailure,
)


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path
    )
    test_config_logging_path = Path(test_config_path, "logging/")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_LOGGING_CONFIG_PATH",
        test_config_logging_path,
    )
    yield
    # No need to restore the original value, monkeypatch will handle it


@pytest.fixture(autouse=True)
def reset_internal_config(monkeypatch: pytest.MonkeyPatch):
    # Reset the singleton instance before each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)
    yield
    # Reset the singleton instance after each test
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.InternalConfig._instance", None)


def test_valid_config_file_and_model_validate(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)
    assert raw_config is not None

    try:
        config = Config.model_validate(raw_config)
        assert config is not None
    except Exception as e:
        print(e)
        assert False


def test_internal_config(set_base_config_path: None):
    config_file = Path("tests/data/config", "config.yaml")
    with open(config_file) as f_in:
        raw_config = safe_load(f_in)

    try:
        internal_config = InternalConfig()
    except StartupFailure as e:
        print(e)
    assert internal_config is not None
    assert internal_config.config is not None
    int_config = internal_config.config
    assert int_config.version == raw_config["version"]
    assert len(int_config.lnd_connections) == len(raw_config["lnd_connections"])
    assert (
        int_config.lnd_connections["example"].address
        == raw_config["lnd_connections"]["example"]["address"]
    )
    with pytest.raises(KeyError):
        int_config.lnd_connections["bad_example"]


def test_singleton_config(set_base_config_path: None):
    internal_config = InternalConfig()
    internal_config2 = InternalConfig()
    assert internal_config is internal_config2


def test_bad_internal_config(monkeypatch: pytest.MonkeyPatch):
    test_config_path_bad = Path("tests/data/config-bad")
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path_bad
    )
    # detect sys.exit(1) call

    with pytest.raises(StartupFailure):
        InternalConfig()


def test_notification_bot_config(set_base_config_path: None):
    internal_config = InternalConfig()
    assert internal_config.config is not None
    assert internal_config.config.notification_bots is not None
    for bot in internal_config.config.notification_bots:
        assert internal_config.config.notification_bots[bot].chat_id is not None
        print(internal_config.config.notification_bots[bot].token)


def test_notification_bot_find_bot_name(set_base_config_path: None):
    internal_config = InternalConfig()
    bot_name = internal_config.config.find_notification_bot_name(
        "1234567890:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"
    )
    assert bot_name == "second-bot"


@pytest.mark.skip(reason="Not implemented yet")
def test_update_config(set_base_config_path: None):
    internal_config = InternalConfig()
    assert internal_config.config is not None
    sample_telegram_bot = NotificationBotConfig(
        name="@update_bot",
        token="555555555:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        chat_id=1234567890,
    )
    insert = {sample_telegram_bot.name: sample_telegram_bot}
    internal_config.update_config(setting="notification_bots", insert=insert)
    assert (
        internal_config.config.notification_bots[sample_telegram_bot.name]
        == sample_telegram_bot
    )


@pytest.mark.skip(reason="Not implemented yet")
def test_update_config_fail(set_base_config_path: None):
    """
    Fails because of a duplication in bot token
    """
    internal_config = InternalConfig()
    assert internal_config.config is not None
    sample_telegram_bot = NotificationBotConfig(
        name="@update_bot",
        token="1234567890:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        chat_id=1234567890,
    )
    insert = {sample_telegram_bot.name: sample_telegram_bot}
    with pytest.raises(ValueError):
        internal_config.update_config(setting="notification_bots", insert=insert)
