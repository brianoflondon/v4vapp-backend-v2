import logging
import unittest

from v4vapp_backend_v2.config.mylogger import NonErrorFilter


class TestNonErrorFilter(unittest.TestCase):

    def setUp(self):
        self.filter = NonErrorFilter()

    def test_filter_debug_level(self):
        # Create a log record with level DEBUG
        record = logging.LogRecord(
            name="test_logger",
            level=logging.DEBUG,
            pathname=__file__,
            lineno=10,
            msg="Test debug message",
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
        self.assertTrue(self.filter.filter(record))

    def test_filter_warning_level(self):
        # Create a log record with level WARNING
        record = logging.LogRecord(
            name="test_logger",
            level=logging.WARNING,
            pathname=__file__,
            lineno=30,
            msg="Test warning message",
            args=(),
            exc_info=None,
        )
        self.assertFalse(self.filter.filter(record))

    def test_filter_error_level(self):
        # Create a log record with level ERROR
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname=__file__,
            lineno=40,
            msg="Test error message",
            args=(),
            exc_info=None,
        )
        self.assertFalse(self.filter.filter(record))

    def test_filter_critical_level(self):
        # Create a log record with level CRITICAL
        record = logging.LogRecord(
            name="test_logger",
            level=logging.CRITICAL,
            pathname=__file__,
            lineno=50,
            msg="Test critical message",
            args=(),
            exc_info=None,
        )
        self.assertFalse(self.filter.filter(record))

if __name__ == "__main__":
    unittest.main()