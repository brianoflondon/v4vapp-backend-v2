import logging
import unittest
from pathlib import Path

import pytest

from v4vapp_backend_v2.config.mylogger import NotificationFilter


@pytest.fixture(autouse=True)
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


class TestNotificationFilter(unittest.TestCase):
    def setUp(self):
        self.filter = NotificationFilter()

    def test_filter_warning_level(self):
        # Create a log record with level WARNING
        record = logging.LogRecord(
            name="test_logger",
            level=logging.WARNING,
            pathname=__file__,
            lineno=10,
            msg="Test warning message",
            args=(),
            exc_info=None,
        )
        self.assertTrue(self.filter.filter(record))

    def test_filter_info_level(self):
        # Create a log record with level INFO
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=20,
            msg="Test info message",
            args=(),
            exc_info=None,
        )
        self.assertFalse(self.filter.filter(record))

    def test_filter_notification_true(self):
        # Create a log record with notification attribute set to True
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=30,
            msg="Test info message with notification",
            args=(),
            exc_info=None,
        )
        record.notification = True
        self.assertTrue(self.filter.filter(record))

    def test_filter_notification_false(self):
        # Create a log record with notification attribute set to False
        record = logging.LogRecord(
            name="test_logger",
            level=logging.WARNING,
            pathname=__file__,
            lineno=40,
            msg="Test warning message with notification false",
            args=(),
            exc_info=None,
        )
        record.notification = False
        self.assertFalse(self.filter.filter(record))

    def test_filter_notification_silent_module(self):
        # Create a log record from a module with notification_silent set to True
        # if logging.log_notification_silent in the config, a log will appear in the logs but no notfication will be sent
        record = logging.LogRecord(
            name="beemapi.node",
            level=logging.WARNING,
            pathname=__file__,
            lineno=40,
            msg="Test warning message with notification false",
            args=(),
            exc_info=None,
        )
        record.notification = False
        self.assertFalse(self.filter.filter(record))
