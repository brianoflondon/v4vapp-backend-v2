import logging
from unittest.mock import Mock, patch

import pytest

from v4vapp_backend_v2.config.mylogger import (
    CustomNotificationHandler,
    ErrorCode,
    NotificationProtocol,
)


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
            assert mock_logger_info.call_count == 2
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
