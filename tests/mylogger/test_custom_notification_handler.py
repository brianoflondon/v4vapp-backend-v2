import logging
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from v4vapp_backend_v2.config.mylogger import (
    CustomNotificationHandler,
    ErrorCode,
    NotificationProtocol,
)


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


@pytest.fixture
def mock_sender():
    sender = Mock(spec=NotificationProtocol)
    return sender


@pytest.fixture
def handler(mock_sender):
    handler = CustomNotificationHandler()
    handler.error_codes = {}
    handler.sender = mock_sender()
    return handler


def test_emit_with_error_code_clear(handler, mock_sender, caplog):
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test_path.py",
        lineno=1,
        msg="Test msg",
        args=(),
        exc_info=None,
    )
    record.error_code = "E123"
    record.error_code_clear = "E123"

    # Setting up a previous error code for clearing
    handler.error_codes[record.error_code] = ErrorCode(code=record.error_code_clear)

    with patch("v4vapp_backend_v2.config.mylogger.logger.info") as mock_logger_info:
        with caplog.at_level(logging.DEBUG):
            handler.emit(record)
            # first call is to report config filename, then log error and clear
            assert "Error code E123 cleared" in mock_logger_info.call_args[0][0]


def test_emit_new_error_code(handler, mock_sender, caplog):
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
        handler.emit(record)

    # Assertions
    assert "E456" in handler.error_codes
    mock_sender.assert_called_once()
    assert "E456" in handler.error_codes


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


def test_emit_error_code_already_exists(handler, mock_sender, caplog):
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
    handler.error_codes[record.error_code] = ErrorCode(code=record.error_code)

    with caplog.at_level(logging.DEBUG):
        handler.emit(record)

    # Assertions
    mock_sender.send_notification.assert_not_called()
