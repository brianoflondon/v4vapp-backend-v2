from datetime import datetime, timezone
import json
import logging

from v4vapp_backend_v2.config.mylogger import MyJSONFormatter


def test_format_basic_log_record():
    # Create a log record
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.created = datetime.now(tz=timezone.utc).timestamp()

    # Create an instance of MyJSONFormatter
    formatter = MyJSONFormatter()

    # Format the log record
    formatted_message = formatter.format(record)

    # Parse the JSON output
    log_dict = json.loads(formatted_message)

    # Check the output
    assert log_dict["message"] == "Test message"
    assert "timestamp" in log_dict


def test_format_log_record_with_exception():
    # Create a log record with exception info
    try:
        raise ValueError("Test exception")
    except ValueError as ex:
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname=__file__,
            lineno=20,
            msg="Test message with exception",
            args=(),
            exc_info=(type(ex), ex, ex.__traceback__),
        )
        record.created = datetime.now(tz=timezone.utc).timestamp()

    # Create an instance of MyJSONFormatter
    formatter = MyJSONFormatter()

    # Format the log record
    formatted_message = formatter.format(record)

    # Parse the JSON output
    log_dict = json.loads(formatted_message)

    # Check the output
    assert log_dict["message"] == "Test message with exception"
    assert "timestamp" in log_dict
    assert "exc_info" in log_dict


def test_format_log_record_with_custom_keys():
    # Create a log record
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=30,
        msg="Test message with custom keys",
        args=(),
        exc_info=None,
    )
    record.created = datetime.now(tz=timezone.utc).timestamp()

    # Create an instance of MyJSONFormatter with custom keys
    custom_keys = {"log_message": "message", "log_timestamp": "timestamp"}
    formatter = MyJSONFormatter(fmt_keys=custom_keys)

    # Format the log record
    formatted_message = formatter.format(record)

    # Parse the JSON output
    log_dict = json.loads(formatted_message)

    # Check the output
    assert log_dict["log_message"] == "Test message with custom keys"
    assert "log_timestamp" in log_dict
