from pathlib import Path

import pytest
from colorama import Fore, Style

from v4vapp_backend_v2.config.setup import InternalConfig, logger


@pytest.fixture
def set_base_config_path(monkeypatch: pytest.MonkeyPatch):
    test_config_path = Path("tests/data/config")
    monkeypatch.setattr("v4vapp_backend_v2.config.setup.BASE_CONFIG_PATH", test_config_path)
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


def test_logger_output(set_base_config_path: None):
    INTERNAL_CONFIG = InternalConfig()
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    logger.debug("This is a debug message")
    logger.exception("This is an exception message")

    print("\033[31m" + "some red text")
    print("\033[32m" + "some green text")
    print("\033[33m" + "some yellow text")
    print("\033[34m" + "some blue text")
    print("\033[35m" + "some purple text")
    print("\033[39m")  # and reset to default color

    logger.info(Fore.GREEN + "This is an info message" + Style.RESET_ALL)
    logger.warning(Fore.YELLOW + "This is a warning message" + Style.RESET_ALL)
    logger.error(Fore.RED + "This is an error message" + Style.RESET_ALL)
    logger.critical(f"{Fore.MAGENTA}This is a magenta message{Style.RESET_ALL}")
    logger.critical("another message")
