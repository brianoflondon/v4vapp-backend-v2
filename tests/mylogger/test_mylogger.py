import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from v4vapp_backend_v2.config.mylogger import MyJSONFormatter, human_readable_datetime_str, timedelta_display
from v4vapp_backend_v2.config.setup import InternalConfig, logger


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
            unusual_attr="Unusual attribute",
        )
        record.created = datetime.now(tz=timezone.utc).timestamp()
        # generate a stack_info field
        record.stack_info = "Stack info"

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


def test_timedelta_display():

    # Test with a timedelta of 1 hour, 2 minutes, and 3 seconds
    td = timedelta(hours=1, minutes=2, seconds=3)
    assert timedelta_display(td) == "01h 02m 03s"

    # Test with a timedelta of 0 hours, 0 minutes, and 0 seconds
    td = timedelta(seconds=0)
    assert timedelta_display(td) == "00h 00m 00s"

    # Test with a timedelta of 23 hours, 59 minutes, and 59 seconds
    td = timedelta(hours=23, minutes=59, seconds=59)
    assert timedelta_display(td) == "23h 59m 59s"


def test_human_readable_datetime_str():

    # Test with a datetime object
    dt_obj = datetime(2022, 1, 1, 12, 30, 45, 123456)
    assert human_readable_datetime_str(dt_obj) == "12:30:45.123 Sat 01 Jan"


