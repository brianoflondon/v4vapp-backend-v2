import logging
import unittest
from v4vapp_backend_v2.config.mylogger import NotificationFilter

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