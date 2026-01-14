import logging
from pathlib import Path
from unittest.mock import Mock

import pytest

from v4vapp_backend_v2.config.mylogger import (
    CustomNotificationHandler,
    ErrorCode,
    ErrorTrackingFilter,
    NotificationProtocol,
)
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
    # Reset ErrorCodeManager singleton for clean test state
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.error_code_manager.ErrorCodeManager._instance", None
    )
    yield
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.setup.InternalConfig._instance", None
    )  # Resetting InternalConfig instance
    # Reset ErrorCodeManager again after test
    monkeypatch.setattr(
        "v4vapp_backend_v2.config.error_code_manager.ErrorCodeManager._instance", None
    )
    InternalConfig.error_code_manager.clear()  # Clear error codes between tests


@pytest.fixture
def mock_sender():
    sender = Mock(spec=NotificationProtocol)
    return sender


@pytest.fixture
def handler(mock_sender):
    handler = CustomNotificationHandler()
    handler.sender = mock_sender()
    return handler


@pytest.fixture
def error_filter():
    return ErrorTrackingFilter()


def test_error_filter_with_error_code_clear(error_filter, caplog):
    """Test that ErrorTrackingFilter clears error codes properly."""
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=1,
        msg="Test msg",
        args=(),
        exc_info=None,
    )
    record.error_code_clear = "E123"

    # Setting up a previous error code for clearing
    InternalConfig().error_codes["E123"] = ErrorCode(code="E123")

    with caplog.at_level(logging.DEBUG):
        result = error_filter.filter(record)
        # Should allow the record through
        assert result is True
        # Error code should be cleared
        assert "E123" not in InternalConfig().error_codes


def test_error_filter_new_error_code(error_filter, caplog):
    """Test that ErrorTrackingFilter adds new error codes."""
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=1,
        msg="Error message",
        args=(),
        exc_info=None,
    )
    record.error_code = "E456"

    with caplog.at_level(logging.DEBUG):
        result = error_filter.filter(record)

    # Assertions
    assert result is True  # Should allow the record through
    assert "E456" in InternalConfig().error_codes


def test_emit_no_error_code(handler, mock_sender, caplog):
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=1,
        msg="Normal message",
        args=(),
        exc_info=None,
    )

    with caplog.at_level(logging.DEBUG):
        handler.emit(record)

    # Assertions
    mock_sender.assert_called_once()
    assert "Normal message" in mock_sender.mock_calls[1][1][0]


def test_error_filter_suppresses_duplicate_error_code(error_filter, caplog):
    """Test that ErrorTrackingFilter suppresses duplicate error codes within re_alert_time."""
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=1,
        msg="Error message",
        args=(),
        exc_info=None,
    )
    record.error_code = "E789"
    # Add existing error code
    InternalConfig().error_codes["E789"] = ErrorCode(code="E789")

    with caplog.at_level(logging.DEBUG):
        result = error_filter.filter(record)

    # Should suppress the record (return False)
    assert result is False
